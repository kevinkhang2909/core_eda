import duckdb
from pathlib import Path
from loguru import logger
import sys

logger.remove()
fmt = '<green>{time:HH:mm:ss}</green> | <level>{message}</level>'
logger.add(sys.stdout, colorize=True, format=fmt)


class EDA:
    def __init__(self, file_path: Path, percentile: list = [0.25, 0.5, 0.75]):
        self.file_path = file_path
        self.file_type = file_path.suffix[1:]

        self.funcs = ['mean', 'stddev_pop', 'min', 'max']
        self.percentile = percentile

        self.query_read_file = f"read_{self.file_type}('{self.file_path}')"

        self.df_sample = None
        self.total_rows = None
        self.df_numeric = None
        self.df_varchar = None
        self.df_overview = None

    def sample(self, limit: int = 10):
        query = f"SELECT * FROM {self.query_read_file} limit {limit}"
        self.df_sample = duckdb.query(query).pl()

    def count_rows(self):
        query = f"SELECT count(*) total_rows FROM {self.query_read_file}"
        self.total_rows = duckdb.query(query).pl()['total_rows'].to_list()[0]

    def summary_data_type(self):
        # overview
        query = f"""
        with aggregate as (
            from {self.query_read_file} select
            {{
                name: first(alias(columns(*))),
                type: first(typeof(columns(*))),
                max: max(columns(*))::varchar,
                min: min(columns(*))::varchar,
                nulls: count(*) - count(columns(*)),
            }}
        ),
        columns as (unpivot aggregate on columns(*))
        select value.* 
        from columns
        """
        df_overview = duckdb.query(query).pl()
        self.df_overview = df_overview

        # varchar
        query = f"""select name from df_overview where type in ('VARCHAR', 'BOOLEAN')"""
        list_varchar = duckdb.sql(query).pl()['name'].to_list()
        if list_varchar:
            list_varchar = ', '.join(list_varchar)
            query = f"""
            with aggregate as (
                from (select {list_varchar} from {self.query_read_file}) select
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
        query = f"""select name from df_overview where type not in ('VARCHAR', 'BOOLEAN')"""
        list_numeric = duckdb.sql(query).pl()['name'].to_list()
        if list_numeric:
            list_numeric = ', '.join(list_numeric)
            query = f"""
            with aggregate as (
                from (select {list_numeric} from {self.query_read_file}) select
                    {{
                        name_: first(alias(columns(*))),
                        type_: first(typeof(columns(*))),
                        {', \n'.join([f"{i}_: {i}(columns(*))::varchar" for i in self.funcs])},
                        {', \n'.join([f"q_{int(i*100)}th: quantile_cont(columns(*), {i})" for i in self.percentile])},
                        nulls_count: count(*) - count(columns(*)),
                    }}
            ),
            columns as (unpivot aggregate on columns(*))
            select value.* 
            from columns
            """
            self.df_numeric = duckdb.sql(query).pl()

    def analyze(self) -> dict:
        self.sample()
        self.count_rows()
        self.summary_data_type()

        logger.info(f"[ANALYZE]:")
        print(
            f"-> Data Shape: ({self.total_rows:,.0f}, {self.df_overview.shape[0]}) \n"
        )

        dict_ = {
            'overview': self.df_overview,
            'sample': self.df_sample,
            'numeric': self.df_numeric,
            'varchar': self.df_varchar,
        }
        return dict_
