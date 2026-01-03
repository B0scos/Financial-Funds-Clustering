from sklearn.mixture import GaussianMixture
from .base import BaseTrainer

class GMMTrainer(BaseTrainer):
    def __init__(self, n_components=2, random_state=0, n_init=1, **kwargs):
        self.params = dict(
            n_components=n_components,
            random_state=random_state,
            n_init=n_init
        )
        self.params.update(kwargs)
        self.model = None

    def set_params(self, **kwargs):
        self.params.update(kwargs)

    def build(self):
        # The main script uses 'n_clusters', but GMM expects 'n_components'.
        if 'n_clusters' in self.params:
            self.params['n_components'] = self.params.pop('n_clusters')
        self.model = GaussianMixture(**self.params)

    def fit(self, X):
        self.model.fit(X)

    def predict(self, X):
        return self.model.predict(X)
