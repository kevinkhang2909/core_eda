import duckdb
import polars as pl
import polars.selectors as cs
from rich import print
import holidays
from sklearn.preprocessing import FunctionTransformer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import LabelEncoder
import numpy as np
vn_holiday = holidays.country_holidays('VN')


class Func:
    def __init__(self, percentile: list = None):
        self.percentile = [0.25, 0.5, 0.75] if not percentile else percentile
        self.funcs = ['mean', 'stddev_pop', 'min', 'max']

    def _query_describe_group(self, data, col_group_by: list, col_describe: str):
        len_col_group_by = len(col_group_by)
        range_ = ', '.join([str(i) for i in range(1, len_col_group_by + 1)])
        query = f"""
        SELECT {', '.join(col_group_by)}
        , '{col_describe}' feature_name
        , {'\n, '.join([f"{i}({col_describe}) {i}_" for i in self.funcs])}
        , {'\n, '.join([f"percentile_cont({i}) WITHIN GROUP (ORDER BY {col_describe}) q{int(i * 100)}th" for i in self.percentile])}
        FROM data
        GROUP BY {range_}, {len_col_group_by + 1}
        ORDER BY {range_}
        """
        return query

    def describe_group(self, data, col_group_by: list | str, col_describe: list | str):
        # handle string
        if isinstance(col_group_by, str):
            col_group_by = [col_group_by]

        if isinstance(col_describe, str):
            col_describe = [col_describe]

        # run
        lst = [f'{self._query_describe_group(data, col_group_by, f)}' for f in col_describe]
        query = '\nUNION ALL\n'.join(lst)
        return duckdb.sql(query).pl()


class EDA_Dataframe:
    def __init__(self, data: pl.DataFrame, prime_key: str | list):
        self.data = data
        self.prime_key = prime_key
        if isinstance(prime_key, str):
            self.prime_key = [prime_key]
        print('[EDA Dataframe]:')

        self.data = EDA_Dataframe.convert_decimal(self.data)
        self.row_count = self.data.shape[0]

    @staticmethod
    def convert_decimal(data):
        col_decimal = [i for i, v in dict(data.schema).items() if v == pl.Decimal]
        if col_decimal:
            data = data.with_columns(pl.col(i).cast(pl.Float64) for i in col_decimal)
            print(f'-> Decimal columns found: {len(col_decimal)} columns')
        return data

    def count_nulls(self):
        null = (
            self.data
            .null_count()
            .to_dict(as_series=False)
        )
        null = {i: (v[0], round(v[0] / self.row_count, 2)) for i, v in null.items() if v[0] != 0}
        print(f'-> Null count: {len(null)} columns')
        print(null)

    def check_sum_zero(self):
        sum_zero = (
            self.data
            .select(~cs.by_dtype([pl.String, pl.Date]))
            .fill_null(0)
            .sum()
            .to_dict(as_series=False)
        )
        sum_zero = [i for i, v in sum_zero.items() if v[0] == 0]
        print(f'-> Sum zero count: {len(sum_zero)} columns')
        print(sum_zero)

    def check_infinity(self):
        infinity = (
            self.data
            .select(~cs.by_dtype([pl.String, pl.Date]))
            .select(pl.all().is_infinite())
            .sum()
            .to_dict(as_series=False)
        )
        infinity = [i for i, v in infinity.items() if v[0] != 0]
        print(f'-> Infinity count: {len(infinity)} columns')
        print(infinity)

    def check_duplicate(self):
        # check
        num_prime_key = self.data.select(self.prime_key).n_unique()
        dup_dict = {True: f'[red]Duplicates[/]', False: f'[green]No duplicates[/]'}
        check = num_prime_key != self.row_count
        print(
            f'-> Data Shape: {self.data.shape} \n'
            f'-> Numbers of prime key: {num_prime_key:,.0f} \n'
            f'-> Check duplicates prime key: {dup_dict[check]}'
        )
        # sample
        sample = self.data.filter(self.data.select(self.prime_key).is_duplicated())[:5]
        if check:
            print('-> Duplicated sample:')
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
        self.check_infinity()

    @staticmethod
    def value_count(data, col: str, sort_col: str = 'count'):
        value_count = (
            data[col].value_counts()
            .with_columns((pl.col('count') / data.shape[0]).round(3).alias('count_pct'))
            .sort(sort_col, descending=True)
        )
        print(value_count)
        return value_count

    @staticmethod
    def cut(data, col: str, conditions: dict):
        """
        conditions = {
            '1 - 4': pl.col(col) < 5,
            '5 - 9': pl.col(col).is_between(5, 9),
            '10 - 15': pl.col(col).is_between(10, 15),
            '15++': pl.col(col) > 15,
        }
        """
        return data.with_columns(
            pl.coalesce(
                pl.when(v).then(pl.lit(i))
                for i, v in conditions.items()
            ).alias(f'cut_{col}')
        )


class ExtractTime:
    @staticmethod
    def sin_transformer(period):
        return FunctionTransformer(lambda x: np.sin(x / period * 2 * np.pi))

    @staticmethod
    def cos_transformer(period):
        return FunctionTransformer(lambda x: np.cos(x / period * 2 * np.pi))

    @staticmethod
    def trigonometric_features(data, dict_column: dict = None, merge_with_data: bool = True):
        # sin/cos transformation
        if dict_column is None:
            dict_column = {'month': 12, 'day': 30}
        sin_f = [(f'{i}_sin', ExtractTime.sin_transformer(v), [i]) for i, v in dict_column.items()]
        cos_f = [(f'{i}_cos', ExtractTime.cos_transformer(v), [i]) for i, v in dict_column.items()]
        ct = ColumnTransformer(transformers=sin_f + cos_f)
        col = [i[0] for i in sin_f + cos_f]
        df_trigonometric = pl.DataFrame(ct.fit_transform(data), schema=col)
        # export
        if merge_with_data:
            return pl.concat([data, df_trigonometric], how='horizontal')
        else:
            return df_trigonometric

    @staticmethod
    def date_time_features(df: pl.DataFrame, col: str = 'grass_date') -> pl.DataFrame:
        return (
            df
            .with_columns(
                pl.col(col).dt.year().alias('year').cast(pl.Int16),
                pl.col(col).dt.month().alias('month').cast(pl.Int8),
                pl.col(col).dt.day().alias('day').cast(pl.Int8),
                pl.col(col).dt.weekday().alias('weekday').cast(pl.Int8),
                pl.col(col).map_elements(lambda x: 1 if vn_holiday.get(x) else 0, return_dtype=pl.Int64).alias('holiday')
            )
            .with_columns(
                (pl.col('month') - pl.col('day')).alias('days_dif_spike')
            )
        )

    @staticmethod
    def trend(df: pl.DataFrame, col: list, index_column: str = 'grass_date', period: str = '3d') -> pl.DataFrame:
        return df.with_columns(
            pl.mean(i).rolling(index_column=index_column, period=period, closed='left').alias(f'trend_{period}_{i}')
            for i in col
        )

    @staticmethod
    def trend_duckdb(
            data: pl.DataFrame,
            col: str,
            col_partition: str = None,
            col_index: str = 'grass_date',
            period: int | str = 7,
            function: str = 'sum'
    ) -> pl.DataFrame:
        """
        Args:
            data: pl.DataFrame
            col: total_order
            col_partition: item_id
            col_index: grass_date
            period: str if add column window
            function: sum | avg | max | min | stddev_pop
        Returns:
            pl.DataFrame
        """
        # config
        add_partition, add_order = '', f'ORDER BY {col_index}'
        if col_partition:
            add_partition = f'PARTITION BY {col_partition}'
            add_order = f'ORDER BY {col_partition}, {col_index}'

        column_name = f'{function}_{period}d_{col}' if isinstance(period, int) else f'{function}_dynamic_{col}'

        # query
        query = f"""
        with base as (
            SELECT {col_index}
            , {col_partition}
            , {col}
            FROM data
            {add_order}
        )
        
        select * 
        , {function}({col}) OVER range_time AS {column_name}
        from base
        
        WINDOW range_time AS (
            {add_partition}
            ORDER BY {col_index} ASC
            RANGE BETWEEN {period} PRECEDING AND 0 FOLLOWING
            EXCLUDE CURRENT ROW
        )
        """
        return duckdb.sql(query).pl()

    @staticmethod
    def season(df: pl.DataFrame, col: list, period: str = '3d') -> pl.DataFrame:
        return df.with_columns(
            (pl.col(i) - pl.col(f'trend_{period}_{i}')).alias(f'season_{period}_{i}') for i in col
        )

    @staticmethod
    def shift(df: pl.DataFrame, col: list, window: int = 7) -> pl.DataFrame:
        name = 'next' if window < 0 else 'prev'
        return df.with_columns(
            pl.col(i).shift(window).alias(f'{name}_{abs(window)}d_{i}') for i in col
        )


class Encode:
    @staticmethod
    def label(data: pl.DataFrame, col: list):
        le = LabelEncoder()
        dict_ = {}
        for i in col:
            dict_[i] = le.fit_transform(data[i].to_numpy())

        return data.with_columns(pl.Series(i, v) for i, v in dict_.items())
