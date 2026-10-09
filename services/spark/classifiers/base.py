from abc import ABC, abstractmethod


class BaseClassifier(ABC):
    """Abstract base class for sentiment classification models."""

    @abstractmethod
    def classify(self, text: str) -> str:
        """Classify input text into one of: 'positive', 'neutral', or 'negative'."""
        pass
