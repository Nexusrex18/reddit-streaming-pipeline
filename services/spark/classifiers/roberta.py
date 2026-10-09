from .base import BaseClassifier


class RobertaClassifier(BaseClassifier):
    """Placeholder interface for transformer-based (e.g. RoBERTa) sentiment classifier."""

    def __init__(self, model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest"):
        self.model_name = model_name

    def classify(self, text: str) -> str:
        # Fallback stub for resource-constrained environments
        return "neutral"
