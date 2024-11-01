import polars as pl
import matplotlib.pyplot as plt
import re
import emoji
from tqdm import tqdm

plt.style.use('seaborn-v0_8')

class TextEDA:
    EMOJI_PATTERN = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE
    )

    BRACKETS_PATTERN = re.compile(r"[\(\[\<\"\|].*?[\)\]\>\"\|]")
    SPECIAL_CHARS_PATTERN = re.compile(r'\-|\_|\*')
    WHITESPACE_PATTERN = re.compile(r'\s+')
    PHONE_PATTERN = re.compile(r'(\+84|0)[0-9]{9,10}')
    URL_PATTERN = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    )

    @staticmethod
    def remove_text_between_emojis(text):
        """Remove text between emojis for repeated texts."""
        emojis = TextEDA.EMOJI_PATTERN.findall(text)
        if len(emojis) < 2:
            return text
        regex = f"[{emojis[0]}].*?[{emojis[1]}]"
        return re.sub(regex, "", text)

    @staticmethod
    def clean_text_pipeline(text: str) -> str:
        """Clean text for repeated texts."""
        text = str(text).lower().strip()
        text = TextEDA.remove_text_between_emojis(text)
        text = emoji.replace_emoji(text, ' ')
        text = TextEDA.BRACKETS_PATTERN.sub(' ', text)
        text = TextEDA.SPECIAL_CHARS_PATTERN.sub(' ', text)
        text = TextEDA.WHITESPACE_PATTERN.sub(' ', text)
        return text.rstrip('.').strip()

    @staticmethod
    def len_text(data: pl.DataFrame, col: str, seperator: str = ' ') -> pl.DataFrame:
        """Calculate word count using Polars' native operations."""
        return data.with_columns(pl.col(col).str.split(seperator).list.len().alias(f'{col}_word_count'))

    @staticmethod
    def clean_text(data: pl.DataFrame, col: str = 'item_name') -> pl.DataFrame:
        """Clean text and add to df."""
        lst = [TextEDA.clean_text_pipeline(str(x)) for x in tqdm(data[col].to_list(), desc='[TextEDA] Clean Text')]
        return data.with_columns(pl.Series(name=f'{col}_clean', values=lst))

    @staticmethod
    def _detect_pattern(text: str, pattern: re.Pattern) -> bool:
        """Helper method for pattern detection."""
        return bool(pattern.search(text))

    @staticmethod
    def detect_phone(data: pl.DataFrame, col: str = 'item_name') -> pl.DataFrame:
        """Detect phone numbers."""
        return data.with_columns(
            pl.col(col)
            .map_elements(lambda x: TextEDA._detect_pattern(x, TextEDA.PHONE_PATTERN), return_dtype=pl.Boolean)
            .alias('phone_detect')
        )

    @staticmethod
    def detect_url(data: pl.DataFrame, col: str = 'item_name') -> pl.DataFrame:
        """Detect URLs."""
        return data.with_columns(
            pl.col(col)
            .map_elements(lambda x: TextEDA._detect_pattern(x, TextEDA.URL_PATTERN), return_dtype=pl.Boolean)
            .alias('url_detect')
        )

    @staticmethod
    def detect_words(data: pl.DataFrame, patterns: list, col: str = 'item_name') -> pl.DataFrame:
        """Detect words."""
        patterns_set = set(patterns)
        return data.with_columns(
            pl.col(col)
            .map_elements(lambda x: bool(patterns_set.intersection(x.split())), return_dtype=pl.Boolean)
            .alias('word_detect')
        )
