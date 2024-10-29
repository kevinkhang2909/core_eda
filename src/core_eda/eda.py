import duckdb
from pathlib import Path
from loguru import logger
import sys
from colorama import Fore
import polars as pl
from tqdm import tqdm
from .functions import jsd

logger.remove()
fmt = '<green>{time:HH:mm:ss}</green> | <level>{message}</level>'
logger.add(sys.stdout, colorize=True, format=fmt)


class EDA:
    def __init__(
            self,
            file_path: Path = None,
            percentile: list = [0.25, 0.5, 0.75],
            prime_key: list = None,
    ):
        self.file_path = file_path
        self.file_type = file_path.suffix[1:]

        self.percentile = percentile
        self.prime_key = prime_key
        self.prime_key_query = ', '.join(self.prime_key)
        self.funcs = ['mean', 'stddev_pop', 'min', 'max']

        self.query_read = f"read_{self.file_type}('{self.file_path}')"
        self.query_select_all = f"select * from {self.query_read}"

        self.df_sample = None
        self.df_numeric = None
        self.df_varchar = None
        self.df_duplicate = None

    def sample(self, limit: int = 10):
        query = f"{self.query_select_all} limit {limit}"
        self.df_sample = duckdb.query(query).pl()

    def count_rows(self) -> int:
        query = f"SELECT count(*) total_rows FROM {self.query_read}"
        return duckdb.query(query).fetchnumpy()['total_rows'][0]

    def check_duplicate(self) -> int:
        query = f"""
        with base as (
            SELECT distinct {self.prime_key_query}
            FROM {self.query_read}
        )
        select count(*) total_prime_key from base
        """
        return duckdb.query(query).fetchnumpy()['total_prime_key'][0]

    def _summary_data_type_(self):
        # set type
        query = f"""
        SET VARIABLE VARCHAR_NAMES = (
            SELECT LIST(column_name)
            FROM (DESCRIBE {self.query_select_all})
            WHERE column_type in ('VARCHAR', 'BOOLEAN')
        )
        """
        duckdb.sql(query)

        # varchar
        query = f"""
        with aggregate as (
            from (select COLUMNS(x -> x in GETVARIABLE('VARCHAR_NAMES')) from {self.query_read}) select
                {{
                    name_: first(alias(columns(*))),
                    type_: first(typeof(columns(*))),
                    sample_: max(columns(*))::varchar,
                    approx_unique_: approx_count_distinct(columns(*)),
                    nulls_count_: count(*) - count(columns(*)),
                }}
        ),
        columns as (unpivot aggregate on columns(*))
        select value.* 
        from columns
        """
        self.df_varchar = duckdb.sql(query).pl()

        # numeric
        query = f"""
        with aggregate as (
            from (select COLUMNS(x -> x not in GETVARIABLE('VARCHAR_NAMES')) from {self.query_read}) select
                {{
                    name_: first(alias(columns(*))),
                    type_: first(typeof(columns(*))),
                    {', \n'.join([f"{i}_: {i}(columns(*))::varchar" for i in self.funcs])},
                    {', \n'.join([f"q_{int(i*100)}th: quantile_cont(columns(*), {i})" for i in self.percentile])},
                    sum_zero_: sum(columns(*))::varchar,
                    nulls_count: count(*) - count(columns(*)),
                }}
        ),
        columns as (unpivot aggregate on columns(*))
        select value.* 
        from columns
        """
        self.df_numeric = duckdb.sql(query).pl()

    def analyze(self, export: bool = False) -> dict:
        # run
        self.sample()
        total_rows = self.count_rows()
        self._summary_data_type_()

        # check duplicate
        message_dup = ''
        if self.prime_key:
            total_dup_key = self.check_duplicate()
            if total_dup_key != total_rows:
                message_dup += f'-> {Fore.RED}Duplicate prime key:{Fore.RESET} Found {total_dup_key:,.0f} {self.prime_key_query}'
            else:
                message_dup += f'-> {Fore.GREEN}Duplicate prime key:{Fore.RESET} Not Found {total_dup_key:,.0f} {self.prime_key_query}'

        # print log
        logger.info("[ANALYZE]:")
        print(
            f"-> Data Shape: ({total_rows:,.0f}, {self.df_sample.shape[1]}) \n"
            f"{message_dup}"
        )

        # export
        dict_ = {
            'sample': self.df_sample,
            'numeric': self.df_numeric,
            'varchar': self.df_varchar,
        }
        if export:
            return dict_

    def value_count(self, col: str):
        total_rows = self.count_rows()
        query = f"""
        with base as (
            select {col}
            , count(*) count_value
            from {self.query_read}
            group by 1
        )
        select *
        , round(count_value / {total_rows}, 2) count_pct
        from base
        order by 1
        """
        return duckdb.sql(query)

    def _query_describe_group(self, col_group_by: list, col_describe: str, percentile: list = [.25, .5, .75]):
        len_col_group_by = len(col_group_by)
        range_ = {', '.join(range(1, len_col_group_by + 1))}
        query = f"""
        SELECT {', '.join(col_group_by)}
        , '{col_describe}' feature_name
        , {'\n, '.join([f"{i}({col_describe}) {i}_" for i in self.funcs])}
        , {'\n, '.join([f"percentile_cont({i}) WITHIN GROUP (ORDER BY {col_describe}) q{int(i * 100)}th" for i in percentile])}
        FROM {self.query_read}
        GROUP BY {range_}, {len_col_group_by + 1}
        ORDER BY {range_}
        """
        return query

    def describe_group(self, col_group_by: list | str, col_describe: list | str, percentile: list = [.25, .5, .75]):
        # handle string
        if isinstance(col_group_by, str):
            col_group_by = [col_group_by]

        if isinstance(col_describe, str):
            col_describe = [col_describe]

        # run
        lst = []
        for feature in tqdm(col_describe, desc=f'Run on stats {len(col_describe)} features'):
            lst.append(f'({self._query_describe_group(col_group_by, feature, percentile)})')
        query = '\nUNION ALL\n'.join(lst)
        return duckdb.sql(query).pl()


class DistributionCheck:
    def __init__(self, data: pl.DataFrame, col_key: str, col_treatment: str, col_features: list):
        self.col_features = col_features
        self.col_key = col_key
        self.col_treatment = col_treatment
        self.data = data

        self.data_group = {}
        self.binary_value = None
        self._check_binary_value()
        logger.info('[DISTRIBUTION CHECK]:')

    def _check_binary_value(self):
        self.binary_value = self.data[self.col_treatment].unique().to_list()
        if len(self.binary_value) != 2:
            raise ValueError(f'-> {self.col_treatment} value must have two values. Current: {self.binary_value}')

    def split(self, frac_samples: float = .5) -> dict:
        # split to 2 comparable parts
        for i, v in enumerate(self.binary_value):
            filter_ = pl.col(self.col_treatment) == v
            self.data_group[f'{i}'] = self.data.filter(filter_).sample(fraction=frac_samples, seed=42)
        self.data_group['all'] = pl.concat([i for i in self.data_group.values()])
        # verbose
        for i, v in self.data_group.items():
            logger.info(f'-> {i}: {v.shape}')
        return self.data_group

    def jsd_score_multi_features(self):
        df_jsd_full = pl.DataFrame()
        for feature in tqdm(self.col_features, desc=f'Run JSD on {len(self.col_treatment)} features'):
            score = jsd(self.data_group['0'][feature], self.data_group['1'][feature])
            df_jsd = (
                pl.DataFrame(score)
                .with_columns(feature_name=pl.lit(feature))
            )
            df_jsd_full = pl.concat([df_jsd_full, df_jsd])
        return df_jsd_full

    def run(self, file_path: Path):
        e = EDA(file_path=file_path)
        df_stats = e.describe_group(col_group_by=self.col_treatment, col_describe=self.col_features)
        df_jsd = self.jsd_score_multi_features()
        df_stats = df_stats.join(df_jsd, how='left', on='feature_name')
        return df_stats
