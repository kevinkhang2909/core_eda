import duckdb
from pathlib import Path
from loguru import logger
import sys
from colorama import Fore

logger.remove()
fmt = '<green>{time:HH:mm:ss}</green> | <level>{message}</level>'
logger.add(sys.stdout, colorize=True, format=fmt)


class EDA:
    def __init__(
            self,
            file_path: Path,
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
        self.df_sample = duckdb.query(query)

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
        self.df_varchar = duckdb.sql(query)

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
        self.df_numeric = duckdb.sql(query)

    def analyze(self) -> dict:
        # run
        self.sample()
        total_rows = self.count_rows()
        self._summary_data_type_()

        # check duplicate
        message = ''
        if self.prime_key:
            total_dup_key = self.check_duplicate()
            if total_dup_key != total_rows:
                message += f'{Fore.RED}Duplicate prime key:{Fore.RESET} {total_dup_key:,.0f} {self.prime_key_query}'

        # print log
        logger.info(f"[ANALYZE]:")
        print(
            f"-> Data Shape: ({total_rows:,.0f}, {self.df_sample.shape[1]}) \n"
            f"-> {message}"
        )

        # export
        dict_ = {
            'sample': self.df_sample,
            'numeric': self.df_numeric,
            'varchar': self.df_varchar,
        }
        for i, v in dict_.items():
            print(i, v)

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
        , count_value / {total_rows} count_pct
        from base
        """
        return duckdb.sql(query)

    def describe_group(self, prime_key: list, col_describe: str, percentile: list = [.25, .5, .75]):
        prime_key_query = ', '.join(prime_key)
        funcs = ['min', 'max', 'avg', 'stddev']
        query = f"""
        select {prime_key_query}
        , {', \n'.join([f"{i}({col_describe}) {i}_" for i in funcs])}
        , {', \n'.join([f"percentile_cont({i}) WITHIN GROUP (ORDER BY {col_describe}) q{int(i * 100)}th" for i in percentile])}
        from {self.query_read}
        GROUP BY 1
        ORDER BY 1
        """
        return duckdb.sql(query).pl()
