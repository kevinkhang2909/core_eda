import polars as pl
from pathlib import Path
import joblib
from sklearn.metrics import classification_report
from xgboost import XGBClassifier, XGBRegressor
from rich import print


def feature_importance(model_input, all_features: list) -> pl.DataFrame:
    return (
        pl.DataFrame({
            'feature': all_features,
            'contribution': model_input.feature_importances_
        })
        .sort('contribution', descending=True)
    )


class DataInput:
    def __init__(self, x_train, y_train, x_test, y_test, target_names: list = None, save_model: Path | str = None):
        self.x_train = x_train
        self.y_train = y_train
        self.x_test = x_test
        self.y_test = y_test
        self.target_names = target_names
        self.save_model = save_model
        self.rf_params = {
            'colsample_bynode': 0.8,
            'learning_rate': 1,
            'max_depth': 5,
            'num_parallel_tree': 100,
            'objective': 'binary:logistic',
            'subsample': 0.8,
            'tree_method': 'hist',
            'device': 'cuda',
        }


class PipelineClassification(DataInput):
    def run_xgboost(
            self,
            params: dict = None,
            use_rf: bool = None,
    ):
        # params
        if not params:
            params = {
                'metric': 'auc',
                'random_state': 42,
                'device': 'cuda',
                'enable_categorical': True,
            }
        if use_rf:
            params = self.rf_params

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
    def xgb(
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
        xgb_model = XGBRegressor(**params)
        xgb_model.fit(
            self.x_train, self.y_train,
            eval_set=[(self.x_test, self.y_test)],
        )
        # predict
        pred = xgb_model.predict(self.x_test)
        return xgb_model
