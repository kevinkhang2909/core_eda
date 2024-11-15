from pathlib import Path
from loguru import logger
import sys
import polars as pl
from tqdm import tqdm
from .functions import jsd

logger.remove()
fmt = '<green>{time:HH:mm:ss}</green> | <level>{message}</level>'
logger.add(sys.stdout, colorize=True, format=fmt)


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
        for feature in tqdm(self.col_features, desc=f'Run JSD on {len(self.col_features)} features'):
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
