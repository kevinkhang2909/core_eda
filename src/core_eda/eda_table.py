import duckdb
from pathlib import Path
from loguru import logger
import sys
from colorama import Fore
import polars as pl
import polars.selectors as cs
from tqdm import tqdm

logger.remove()
fmt = '<green>{time:HH:mm:ss}</green> | <level>{message}</level>'
logger.add(sys.stdout, colorize=True, format=fmt)


class EDA_File:
    def __init__(
            self,
            file_path: Path = None,
            percentile: list = None,
            prime_key: str | list = None,
    ):
        self.file_path = file_path
        self.file_type = file_path.suffix[1:]

        if prime_key:
            if isinstance(prime_key, str):
                self.prime_key = [prime_key]
            self.prime_key = prime_key
            self.prime_key_query = ', '.join(self.prime_key)

        self.percentile = [0.25, 0.5, 0.75] if not percentile else percentile
        self.funcs = ['mean', 'stddev_pop', 'min', 'max']

        self.query_read = f"read_{self.file_type}('{self.file_path}')"
        self.query_select_all = f"select * from {self.query_read}"

        self.df_sample = None
        self.df_numeric = None
        self.df_varchar = None
        self.df_duplicate = None

    def sample(self, limit: int = 1000):
        query = f"{self.query_select_all} limit {limit}"
        self.df_sample = duckdb.query(query).pl()

    def count_rows(self) -> int:
        query = f"SELECT count(*) total_rows FROM {self.query_read}"
        return duckdb.query(query).fetchnumpy()['total_rows'][0]

    def count_nulls(self) -> str:
        # count null data
        df = duckdb.query(self.query_select_all).pl()
        null = df.null_count().to_dict(as_series=False)
        null = {i: v[0] for i, v in null.items() if v[0] != 0}

        # message
        null_message = f"""-> Null counts: 
        {null}
        """
        return null_message

    def check_duplicate(self, index_slice: int = 0):
        # check duplicates data
        df = duckdb.query(self.query_select_all).pl()
        total_prime_key = df[self.prime_key].n_unique()

        # message
        sample_dup_df = None
        if df.shape[0] != total_prime_key:
            dup_message = f'-> {Fore.RED}Duplicate prime key:{Fore.RESET} Found {total_prime_key:,.0f} {self.prime_key_query}'
            # sample
            filter_ = (pl.col(i).is_duplicated() for i in self.prime_key)
            sample_dup_dict = df.filter(filter_)[index_slice][self.prime_key].to_dict(as_series=False)
            filter_ = (pl.col(i) == v[0] for i, v in sample_dup_dict.items())
            sample_dup_df = df.filter(filter_)
        else:
            dup_message = f'-> {Fore.GREEN}Duplicate prime key:{Fore.RESET} Not Found {total_prime_key:,.0f} {self.prime_key_query}'
        return dup_message, f"Sample duplicates: \n{sample_dup_df}"

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
        message_null = self.count_nulls()
        message_dup, sample_dup = self.check_duplicate()

        # print log
        logger.info(f"""[ANALYZE]:
        -> Data Shape: ({total_rows:,.0f}, {self.df_sample.shape[1]})
        {message_dup}
        {sample_dup}
        {message_null}
        {self.df_sample.head()}
        === DONE ===
        """)

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
        return duckdb.sql(query).pl()

    def _query_describe_group(self, col_group_by: list, col_describe: str):
        len_col_group_by = len(col_group_by)
        range_ = ', '.join([str(i) for i in range(1, len_col_group_by + 1)])
        query = f"""
        SELECT {', '.join(col_group_by)}
        , '{col_describe}' feature_name
        , {'\n, '.join([f"{i}({col_describe}) {i}_" for i in self.funcs])}
        , {'\n, '.join([f"percentile_cont({i}) WITHIN GROUP (ORDER BY {col_describe}) q{int(i * 100)}th" for i in self.percentile])}
        FROM {self.query_read}
        GROUP BY {range_}, {len_col_group_by + 1}
        ORDER BY {range_}
        """
        return query

    def describe_group(self, col_group_by: list | str, col_describe: list | str):
        # handle string
        if isinstance(col_group_by, str):
            col_group_by = [col_group_by]

        if isinstance(col_describe, str):
            col_describe = [col_describe]

        # run
        lst = []
        for feature in tqdm(col_describe, desc=f'Run Stats on {len(col_describe)} features'):
            lst.append(f'({self._query_describe_group(col_group_by, feature)})')
        query = '\nUNION ALL\n'.join(lst)
        return duckdb.sql(query).pl()


class EDA_Dataframe:
    def __init__(self, data: pl.DataFrame, prime_key: str):
        self.data = data
        self.prime_key = prime_key
        logger.info('[EDA Dataframe]:')

        self._convert_decimal()
        self.data_shape = self.data.shape

    def _convert_decimal(self):
        col_decimal = [i for i, v in dict(self.data.schema).items() if v == pl.Decimal]
        if col_decimal:
            self.data = self.data.with_columns(pl.col(i).cast(pl.Float64) for i in col_decimal)
            logger.info(f'-> Decimal columns found')

    def count_nulls(self):
        null = self.data.null_count().to_dict(as_series=False)
        null = {i: (v[0], round(v[0] / self.data_shape[0], 2)) for i, v in null.items() if v[0] != 0}
        logger.info(f'-> Null count: {null}')
        return null

    def check_schema(self):
        logger.info(f'-> Schema: {self.data.schema}')

    def check_sum_zero(self):
        sum_zero = self.data.select(~cs.by_dtype([pl.String, pl.Date])).fill_null(0).sum().to_dict(as_series=False)
        sum_zero = [i for i, v in sum_zero.items() if v[0] == 0]
        logger.info(f'-> Sum zero count: {sum_zero}')
        return sum_zero

    def check_duplicate(self):
        # check
        num_com = self.data[self.prime_key].n_unique()
        dup_check = f'{Fore.RED}Duplicates{Fore.RESET}' if num_com != self.data_shape[0] else f'{Fore.GREEN}No duplicates{Fore.RESET}'
        logger.info(
            f'-> Data Shape: {self.data_shape} \n'
            f'-> Numbers of prime key: {num_com:,.0f} \n'
            f'-> Check duplicates prime key: {dup_check}'
        )
        # sample
        value_dup = self.data.filter(pl.col(self.prime_key).is_duplicated())[self.prime_key][0]
        sample = self.data.filter(pl.col(self.prime_key) == value_dup)
        if num_com != self.data_shape[0]:
            logger.info(f'-> Duplicated sample: {sample}')
        return sample

    def analyze(self):
        self.check_schema()
        self.count_nulls()
        self.check_sum_zero()
        self.check_duplicate()

    def value_count(self, col: str, sort_col: str | int = 1):
        query = f"""
        with base as (
            select {col}
            , count(*) count_value
            from self
            group by 1
        )
        select *
        , round(count_value / {self.data_shape[0]}, 2) count_pct
        from base
        order by {sort_col}
        """
        logger.info(self.data.sql(query))
