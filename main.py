from src.pipelines.data_pipeline import data_pipeline
from src.config.settings import (
DATA_TRAIN_PATH_WITH_FEATURES,
DATA_TEST_PATH_WITH_FEATURES,
DATA_VALIDATION_PATH_WITH_FEATURES)
import pandas as pd
from src.pipelines.train_pipeline import pre_processing
from src.pipelines.feature_selection_pipeline import backward_feature_selection
from sklearn.cluster import KMeans


if __name__ == "__main__":

    # data_pipeline(rebuild_interim=False)

    df_train = pd.read_parquet(DATA_TRAIN_PATH_WITH_FEATURES).dropna()
    df_test = pd.read_parquet(DATA_TEST_PATH_WITH_FEATURES).dropna()
    df_val = pd.read_parquet(DATA_VALIDATION_PATH_WITH_FEATURES).dropna()

    # print(df_train.columns)
    # quit()

    n_components = int(df_train.shape[1])

    print(n_components)

    train_scaled, test_scaled, val_scaled = pre_processing(n_components=4)



    model = KMeans(n_clusters=2, random_state=0, n_init="auto")

    model.fit(train_scaled)

    train_pred = model.predict(train_scaled)
    test_pred = model.predict(test_scaled)
    val_pred = model.predict(val_scaled)


    df_train['pred'] = train_pred
    df_test['pred'] = test_pred
    df_val['pred'] = val_pred

    look_features = ['mean_return', 'median_return', 'std_return', 'avg_time_drawdown', 'sharpe', 'max_drawdown']
    group_train = df_train.groupby('pred')[look_features].mean()
    group_test = df_test.groupby('pred')[look_features].mean()
    group_val = df_val.groupby('pred')[look_features].mean()

    print("train")
    print(group_train)
    print(df_train['pred'].value_counts())

    print("\ntest")
    print(group_test)
    print(df_test['pred'].value_counts())
    print("n\val")
    print(group_val)
    print(df_val['pred'].value_counts())