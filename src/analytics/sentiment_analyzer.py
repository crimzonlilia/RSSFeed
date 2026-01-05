# src/analytics/sentiment_analyzer.py
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """Analyze sentiment of text using VADER"""
    
    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer()
        logger.info(" Sentiment Analyzer initialized")
    
    def analyze(self, text: str) -> dict:
        """
        Analyze sentiment of text
        Returns: {'compound': float, 'sentiment': str, 'pos': float, 'neg': float, 'neu': float}
        """
        if not text or len(text.strip()) == 0:
            return {
                'compound': 0.0,
                'sentiment': 'neutral',
                'pos': 0.0,
                'neg': 0.0,
                'neu': 1.0
            }
        
        # Get VADER scores
        scores = self.analyzer.polarity_scores(text)
        
        # Classify sentiment based on compound score
        compound = scores['compound']
        if compound >= 0.05:
            sentiment = 'positive'
        elif compound <= -0.05:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        return {
            'compound': compound,
            'sentiment': sentiment,
            'pos': scores['pos'],
            'neg': scores['neg'],
            'neu': scores['neu']
        }


# Test analyzer
if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    
    test_texts = [
        "This is an amazing breakthrough in AI technology!",
        "The company faced severe criticism for data breach.",
        "The weather today is partly cloudy."
    ]
    
    for text in test_texts:
        result = analyzer.analyze(text)
        print(f"\nText: {text}")
        print(f"Sentiment: {result['sentiment']} (compound: {result['compound']:.2f})")