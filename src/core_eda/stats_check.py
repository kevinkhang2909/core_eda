import polars as pl
from pprint import pprint
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb
from .functions import jsd


def stats_check(data: pl.DataFrame, col_treatment: str, feature: str):
    # jsd
    treatment = data.filter(pl.col(col_treatment) == 1)
    control = data.filter(pl.col(col_treatment) == 0)
    score = jsd(treatment[feature], control[feature])

    # statistic
    funcs = ['']
    query = f"""
    select {col_treatment}
    , 
    from data
    """

    # stats
    lst = [
        pl.col(feature).mean().cast(pl.Float64).alias('mean'),
        pl.col(feature).std().cast(pl.Float64).alias('std'),
        pl.col(feature).min().cast(pl.Float64).alias('min'),
        pl.col(feature).max().cast(pl.Float64).alias('max'),
    ]

    result = (
        data.group_by(col_treatment, maintain_order=True).agg(lst)
        .with_columns(
            pl.lit(feature).alias('feature'),
            pl.col(col_treatment).replace_strict({1: 'treatment', 0: 'control'})
        )
        .pivot(index='feature', on=col_treatment)
        .with_columns(
            jsd_score=pl.lit(score['score']),
            jsd_meaning=pl.lit(score['meaning']),
        )
    )
    return result


class PipelineStats:
    def __init__(self, col_key: str, col_treatment: str, col_features: list):
        self.col_features = col_features
        self.col_key = col_key
        self.col_treatment = col_treatment

    def split(self, data: pl.DataFrame, col: str, samples: int = 100):
        """
        param: col must be 0 and 1
        """
        treatment = data.filter(pl.col(col) == 1).sample(samples, seed=42)
        control = data.filter(pl.col(col) == 0).sample(samples, seed=42)
        return treatment, control

    def run(self, treatment: pl.DataFrame, control: pl.DataFrame):
        df_sample = pl.concat([treatment, control])

        df_stats_full = pl.DataFrame()
        for feature in self.col_features:
            print(feature)
            df_stats = stats_check(df_sample, self.col_treatment, feature)
            df_stats_full = pl.concat([df_stats_full, df_stats])
        return df_stats_full


class Plot:
    @staticmethod
    def hist(data: pl.DataFrame, data_stats: pl.DataFrame, col_feature: str, col_treatment: str):
        figure = data_stats.filter(feature=col_feature).to_dicts()[0]
        pprint(figure)

        fig, ax = plt.subplots(1, 1, figsize=(6, 4))
        sns.histplot(data=data, x=col_feature, kde=True, hue=col_treatment, ax=ax)
        fig.tight_layout()
