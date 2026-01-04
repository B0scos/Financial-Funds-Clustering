import pandas as pd
from src.pipelines.train_pipeline import run_training, evaluate


def run_all_experiments(df_train, df_test, df_val, models, clusters, pre_processing_flags, look_features):
    """
    Runs a grid of clustering experiments and returns the results.

    This function iterates through all combinations of models, cluster counts,
    and preprocessing steps, evaluates the results, and aggregates them
    into a single DataFrame.

    Parameters
    ----------
    df_train : pd.DataFrame
        The training dataset.
    df_test : pd.DataFrame
        The testing dataset.
    df_val : pd.DataFrame
        The validation dataset.
    models : list
        A list of model trainer classes to use.
    clusters : list
        A list of integers representing the number of clusters to try.
    pre_processing_flags : list
        A list of preprocessing functions to apply.
    look_features : list
        A list of feature names to use for evaluation.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the aggregated results from all experiments.
    """
    all_results = []

    for model_cls in models:
        for cluster in clusters:
            for type_process in pre_processing_flags:
                model_name = model_cls.__name__
                preprocessing_name = str(type_process.__name__) if type_process else "None"
                print(f"Running experiment: Model={model_name}, Clusters={cluster}, Preprocessing={preprocessing_name}")

                # Make copies to ensure experiment isolation, as run_training adds a 'pred' column
                df_train_copy, df_test_copy, df_val_copy = df_train.copy(), df_test.copy(), df_val.copy()

                (df_train_res, df_test_res, df_val_res), (train_features, test_features, val_features) = run_training(
                    model_cls, df_train_copy, df_test_copy, df_val_copy, pre_processing=type_process, n_clusters=cluster
                )

                # Evaluate all datasets
                for name, df_res, features in [('train', df_train_res, train_features), ('test', df_test_res, test_features), ('validation', df_val_res, val_features)]:
                    results_df = evaluate(df_res, features, look_features=look_features)
                    results_df['dataset'] = name
                    results_df['model'] = model_name
                    results_df['n_clusters'] = cluster
                    results_df['preprocessing'] = preprocessing_name
                    all_results.append(results_df)
                    print(f"\n{name.capitalize()} Results:\n{results_df}")

                print('#' * 50 + '\n')

    return pd.concat(all_results, ignore_index=True)