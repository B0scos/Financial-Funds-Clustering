from sklearn.metrics import silhouette_score, calinski_harabasz_score
import numpy as np

def evaluate_clusters(X, labels):
    if len(np.unique(labels)) <= 1:
        return -np.inf  # invalid clustering

    return {
        "silhouette": silhouette_score(X, labels),
        "calinski": calinski_harabasz_score(X, labels)
    }

def param_search(model_cls, param_grid, X):
    best_score = -np.inf
    best_params = None
    best_labels = None

    for params in param_grid:
        model = model_cls()
        model.set_params(**params)
        model.build()
        model.fit(X)

        labels = model.predict(X)
        scores = evaluate_clusters(X, labels)

        score = scores["silhouette"]  # choose your primary metric

        if score > best_score:
            best_score = score
            best_params = params
            best_labels = labels

    return best_params, best_score, best_labels
