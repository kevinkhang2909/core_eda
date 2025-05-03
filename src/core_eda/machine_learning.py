import polars as pl
import pandas as pd
from pathlib import Path
import numpy as np
import joblib
from rich import print
from tqdm.auto import tqdm
import matplotlib.pyplot as plt
import seaborn as sns
from dataclasses import dataclass

from sklearn.metrics import (
    classification_report,
    r2_score,
    mean_squared_error,
    roc_auc_score,
    f1_score,
)
from sklearn.model_selection import train_test_split, KFold, TimeSeriesSplit

from xgboost import XGBClassifier, XGBRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LinearRegression, LogisticRegression

plt.style.use('fivethirtyeight')


def feature_importance(model_input, all_features: list) -> pl.DataFrame:
    """
    Calculate feature importance scores.
    :param model_input: Trained model with feature_importances_ attribute
    :param all_features: List of feature names
    :return: DataFrame with feature importance scores
    """
    return (
        pl.DataFrame({
            'feature': all_features,
            'contribution': model_input.feature_importances_
        })
        .sort('contribution', descending=True)
    )


@dataclass
class DataInput:
    x: np.ndarray | pd.DataFrame | pl.DataFrame
    y: np.ndarray | pd.DataFrame | pl.DataFrame
    test_size: float = 0.2
    target_names: list = None
    save_model: Path = None

    def __post_init__(self):
        if not 0 < self.test_size < 1:
            raise ValueError("test_size must be between 0 and 1")

        self.x_train, self.x_test, self.y_train, self.y_test = train_test_split(
            self.x, self.y, test_size=self.test_size
        )


class CrossValidation(DataInput):
    def __init__(
            self,
            x,
            y,
            fold: str = 'kf',
            metric : str = 'mse',
            n_splits: int = 5,
            test_size: float = .2,
            target_names: list = None,
            save_model: Path | str = None,
    ):
        super().__init__(x, y, test_size, target_names, save_model)
        fold_lst = {
            'kf': KFold(n_splits=n_splits),
            'tf': TimeSeriesSplit(n_splits=n_splits),
        }
        metric_lst = {
            'mse': mean_squared_error,
            'roc': roc_auc_score,
            'f1': f1_score,
        }
        self.metric = metric
        self.kf = fold_lst[fold]
        self.metric_func = metric_lst[self.metric]
        self.n_splits = n_splits

    def cross_valid(self, model) -> dict:
        # validation
        lst_score = []
        for train_index, test_index in tqdm(self.kf.split(self.x_train), total=self.n_splits, desc='KFold'):
            # select
            x_train = self.x_train.to_numpy()[train_index]
            y_train = self.y_train.to_numpy()[train_index].ravel()
            x_test = self.x_train.to_numpy()[test_index]
            y_test = self.y_train.to_numpy()[test_index].ravel()
            # fit
            model.fit(x_train, y_train)
            # predict
            y_pred = model.predict(x_test)
            score = self.metric_func(y_test, y_pred)
            lst_score.append(score)
        return {'mean_score': np.mean(lst_score).item(), 'std_score': np.std(lst_score).item()}


    def run_model(self, select_models: list, problem: str = 'classification') -> dict:
        """
        Cross validation
        :param select_models: [RF, XGB, LN, TabPFN]
        """
        select_models = [i.upper() for i in select_models]
        if problem == 'classification':
            models = [
                ('LG', LogisticRegression(random_state=42)),
                ('RF', RandomForestClassifier(random_state=42)),
                ('XGB', XGBClassifier(random_state=42)),
                ('TabPFN', TabPFNClassifier(random_state=42)),
            ]
        else:
            models = [
                ('LN', LinearRegression()),
                ('LG', LogisticRegression(random_state=42)),
                ('RF', RandomForestRegressor(random_state=42)),
                ('XGB', XGBRegressor(random_state=42)),
                ('TabPFN', TabPFNRegressor(random_state=42)),
            ]
        models = [i for i in models if i[0] in select_models]

        return {
            name: self.cross_valid(model)
            for name, model in models
        }

    def plot(self, scores: dict, figsize: tuple = (8, 4)):
        df_result = (
            pl.DataFrame(list(scores.items()), orient='row', schema=['model_name', 'val'])
            .unnest('val')
        )
        fig, ax = plt.subplots(1, 1, figsize=figsize)
        sns.barplot(data=df_result, x='model_name', y='mean_score', ax=ax)
        ax.set_title(f'Score: {self.metric}')
        fig.tight_layout()


class PipelineClassification(DataInput):
    def run_xgboost(
            self,
            params: dict = None,
    ):
        # params
        if not params:
            params = {
                'metric': 'auc',
                'random_state': 42,
                'device': 'cuda',
                'enable_categorical': True,
            }

        # train
        print(params)
        xgb_model = XGBClassifier(**params)
        xgb_model.fit(
            self.x_train, self.y_train,
            eval_set=[(self.x_test, self.y_test)],
            verbose=10,
        )
        # predict
        y_pred = xgb_model.predict(self.x_test)

        # save model
        if self.save_model:
            model_path = joblib.dump(xgb_model, self.save_model)
            print(f'Save model to {model_path}')

        # report
        print(classification_report(self.y_test, y_pred, target_names=self.target_names, zero_division=0))
        return xgb_model


class PipelineRegression(DataInput):
    def run_xgboost(
            self,
            params: dict = None,
    ):
        # params
        if not params:
            params = {
                'metric': 'mse',
                'random_state': 42,
                'device': 'cuda',
            }

        # train
        print(params)
        xgb_model = XGBRegressor(**params)
        xgb_model.fit(
            self.x_train, self.y_train,
            eval_set=[(self.x_test, self.y_test)],
            verbose=10,
        )
        # save model
        if self.save_model:
            model_path = joblib.dump(xgb_model, self.save_model)
            print(f'Save model to {model_path}')

        # predict
        y_pred = xgb_model.predict(self.x_test)

        # report
        print(f'R2 Score: {r2_score(self.y_test, y_pred)}')
        return xgb_model
