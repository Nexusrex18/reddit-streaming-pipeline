from nltk.sentiment.vader import SentimentIntensityAnalyzer
from .base import BaseClassifier


class VaderClassifier(BaseClassifier):
    """Rule-based sentiment classifier using NLTK VADER compound polarity scores."""

    def __init__(self, pos_threshold: float = 0.05, neg_threshold: float = -0.05):
        self.sia = SentimentIntensityAnalyzer()
        self.pos_threshold = pos_threshold
        self.neg_threshold = neg_threshold

    def classify(self, text: str) -> str:
        if not text:
            return "neutral"
        compound = self.sia.polarity_scores(text)["compound"]
        if compound >= self.pos_threshold:
            return "positive"
        elif compound <= self.neg_threshold:
            return "negative"
        else:
            return "neutral"
