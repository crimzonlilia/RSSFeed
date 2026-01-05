import re
import html
from typing import Optional


class TextCleaner:
    """Clean and preprocess text data"""
    
    @staticmethod
    def clean_html(text: str) -> str:
        """Remove HTML tags and entities"""
        if not text:
            return ""
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        return text
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean text: remove URLs, extra spaces, etc."""
        if not text:
            return ""
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove special characters (keep basic punctuation)
        text = re.sub(r'[^\w\s.,!?-]', '', text)
        
        return text
    
    @staticmethod
    def extract_keywords(text: str, top_n: int = 10) -> str:
        """Extract top keywords (simple word frequency)"""
        if not text:
            return ""
        
        # Simple tokenization
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
        
        # Remove common stop words
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 
                     'to', 'for', 'of', 'with', 'by', 'from', 'as', 'is', 'was',
                     'are', 'been', 'be', 'have', 'has', 'had', 'do', 'does',
                     'did', 'will', 'would', 'should', 'could', 'may', 'might',
                     'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he',
                     'she', 'it', 'we', 'they', 'what', 'which', 'who', 'when',
                     'where', 'why', 'how', 'all', 'each', 'every', 'both',
                     'few', 'more', 'most', 'other', 'some', 'such'}
        
        words = [w for w in words if w not in stop_words]
        
        # Count frequency
        word_freq = {}
        for word in words:
            word_freq[word] = word_freq.get(word, 0) + 1
        
        # Get top N
        top_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:top_n]
        
        return ','.join([word for word, _ in top_words])
    
    @classmethod
    def process_article(cls, title: str, summary: str, content: str) -> dict:
        """Process article text fields"""
        # Clean each field
        title_clean = cls.clean_text(cls.clean_html(title))
        summary_clean = cls.clean_text(cls.clean_html(summary))
        content_clean = cls.clean_text(cls.clean_html(content))
        
        # Combine all text for analysis
        full_text = f"{title_clean} {summary_clean} {content_clean}"
        
        # Extract keywords
        keywords = cls.extract_keywords(full_text)
        
        return {
            'title_clean': title_clean,
            'summary_clean': summary_clean,
            'content_clean': content_clean,
            'full_text': full_text,
            'keywords': keywords,
            'text_length': len(full_text)
        }


# Test cleaner
if __name__ == "__main__":
    cleaner = TextCleaner()
    
    test_text = """
    <p>This is a <strong>test article</strong> about AI and machine learning.</p>
    Check out this link: https://example.com
    """
    
    result = cleaner.process_article(test_text, "", "")
    print(result)