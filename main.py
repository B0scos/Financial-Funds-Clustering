import pandas as pd
from src.utils.load_data import load_data_with_features
from src.models.kmeans import KMeansTrainer
from src.pipelines.experiment_pipeline import run_all_experiments
from src.process.pre_processing import PCA_scalling, scalling, PCA, just_filter
from src.models.gmm import GMMTrainer


def main():
    """
    Defines and runs the full clustering experiment pipeline.
    """
    # 1. Define Experiment Configuration
    look_features = ['mean_return', 'median_return', 'std_return', 'avg_time_drawdown', 'sharpe', 'max_drawdown']
    models = [KMeansTrainer, GMMTrainer]
    clusters = [2, 3, 4, 5]
    pre_processing_flags = [just_filter, scalling, PCA, PCA_scalling]

    # 2. Load Data
    df_train, df_test, df_val = load_data_with_features()

    # 3. Run Experiments
    results_df = run_all_experiments(df_train, df_test, df_val, models, clusters, pre_processing_flags, look_features)

    # 4. Save Results
    results_df.to_csv('experiment_results.csv', index=False)
    print("\nAll experiments complete. Results saved to experiment_results.csv")


if __name__ == "__main__":
    main()
