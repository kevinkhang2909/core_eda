import duckdb
from loguru import logger
import sys
from colorama import Fore
import polars as pl
import polars.selectors as cs
from tqdm import tqdm
from pprint import pprint

logger.remove()
fmt = '<green>{time:HH:mm:ss}</green> | <level>{message}</level>'
logger.add(sys.stdout, colorize=True, format=fmt)


class Func:
    def __init__(self, data: pl.DataFrame, percentile: list = None):
        self.data = data
        self.percentile = [0.25, 0.5, 0.75] if not percentile else percentile
        self.funcs = ['mean', 'stddev_pop', 'min', 'max']

    def _query_describe_group(self, col_group_by: list, col_describe: str):
        len_col_group_by = len(col_group_by)
        range_ = ', '.join([str(i) for i in range(1, len_col_group_by + 1)])
        query = f"""
        SELECT {', '.join(col_group_by)}
        , '{col_describe}' feature_name
        , {'\n, '.join([f"{i}({col_describe}) {i}_" for i in self.funcs])}
        , {'\n, '.join([f"percentile_cont({i}) WITHIN GROUP (ORDER BY {col_describe}) q{int(i * 100)}th" for i in self.percentile])}
        FROM {self.data}
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
    def __init__(self, data: pl.DataFrame, prime_key: str | list):
        self.data = data
        self.prime_key = prime_key
        if isinstance(prime_key, str):
            self.prime_key = [prime_key]
        logger.info('[EDA Dataframe]:')

        self._convert_decimal()
        self.row_count = self.data.shape[0]

    def _convert_decimal(self):
        col_decimal = [i for i, v in dict(self.data.schema).items() if v == pl.Decimal]
        if col_decimal:
            self.data = self.data.with_columns(pl.col(i).cast(pl.Float64) for i in col_decimal)
            logger.info(f'-> Decimal columns found: {len(col_decimal)} columns')

    def count_nulls(self):
        null = self.data.null_count().to_dict(as_series=False)
        null = {i: (v[0], round(v[0] / self.row_count, 2)) for i, v in null.items() if v[0] != 0}
        logger.info(f'-> Null count: {len(null)} columns')
        pprint(null)

    def check_sum_zero(self):
        sum_zero = self.data.select(~cs.by_dtype([pl.String, pl.Date])).fill_null(0).sum().to_dict(as_series=False)
        sum_zero = [i for i, v in sum_zero.items() if v[0] == 0]
        logger.info(f'-> Sum zero count: {len(sum_zero)} columns')
        pprint(sum_zero)

    def check_duplicate(self):
        # check
        num_prime_key = self.data.select(self.prime_key).n_unique()
        dup_dict = {True: f'{Fore.RED}Duplicates{Fore.RESET}', False: f'{Fore.GREEN}No duplicates{Fore.RESET}'}
        check = num_prime_key != self.row_count
        logger.info(
            f'-> Data Shape: {self.row_count} \n'
            f'-> Numbers of prime key: {num_prime_key:,.0f} \n'
            f'-> Check duplicates prime key: {dup_dict[check]}'
        )
        # sample
        sample = self.data.filter(self.data.select(self.prime_key).is_duplicated())[:5]
        if check:
            logger.info('-> Duplicated sample:')
            with pl.Config(
                tbl_hide_column_data_types=True,
                tbl_hide_dataframe_shape=True,
            ):
                print(sample)
        return sample

    def analyze(self):
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
        , round(count_value / {self.row_count}, 2) count_pct
        from base
        order by {sort_col}
        """
        logger.info(self.data.sql(query))
