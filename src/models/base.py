from abc import ABC, abstractmethod

class BaseTrainer(ABC):
    @abstractmethod
    def build(self): ...
    @abstractmethod
    def fit(self, X): ...
    @abstractmethod
    def predict(self, X): ...

    def set_params(self, **kwargs):
        # every model will implement
        raise NotImplementedError