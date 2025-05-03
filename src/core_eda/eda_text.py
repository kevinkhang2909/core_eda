import polars as pl
import matplotlib.pyplot as plt
import re
import emoji
from tqdm import tqdm

plt.style.use("seaborn-v0_8")


class TextEDA:
    EMOJI_PATTERN = re.compile(
        "["
        "\U0001f600-\U0001f64f"  # emoticons
        "\U0001f300-\U0001f5ff"  # symbols & pictographs
        "\U0001f680-\U0001f6ff"  # transport & map symbols
        "\U0001f1e0-\U0001f1ff"  # flags (iOS)
        "\U00002702-\U000027b0"
        "\U000024c2-\U0001f251"
        "]+",
        flags=re.UNICODE,
    )

    BRACKETS_PATTERN = re.compile(r"[\(\[\<\"\|].*?[\)\]\>\"\|]")
    SPECIAL_CHARS_PATTERN = re.compile(r"\-|\_|\*")
    WHITESPACE_PATTERN = re.compile(r"\s+")
    LINE_BREAK_PATTERN = re.compile(r"\n+")
    WINDOW_STYLE_LINE_BREAK_PATTERN = re.compile(r"\r\n")

    PHONE_PATTERN = re.compile(r"(\+84|0)[0-9]{9,10}")
    URL_PATTERN = re.compile(
        r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
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
    def clean_text_multi_method(text: str) -> str:
        """Clean text for repeated texts."""
        text = str(text).lower().strip()
        text = TextEDA.remove_text_between_emojis(text)
        text = emoji.replace_emoji(text, " ")
        text = TextEDA.BRACKETS_PATTERN.sub(" ", text)
        text = TextEDA.SPECIAL_CHARS_PATTERN.sub(" ", text)
        text = TextEDA.WHITESPACE_PATTERN.sub(" ", text)
        text = TextEDA.LINE_BREAK_PATTERN.sub(" ", text)
        return text.rstrip(".").strip()

    @staticmethod
    def len_text(data: pl.DataFrame, col: str, seperator: str = " ") -> pl.DataFrame:
        """Calculate word count using Polars' native operations."""
        return data.with_columns(
            pl.col(col).str.split(seperator).list.len().alias(f"{col}_word_count")
        )

    @staticmethod
    def clean_text_pipeline_polars(
        data: pl.DataFrame, col: str = "item_name"
    ) -> pl.DataFrame:
        """Clean text and add to df."""
        lst = [
            TextEDA.clean_text_multi_method(str(x))
            for x in tqdm(data[col].to_list(), desc="[TextEDA] Clean Text")
        ]
        return data.with_columns(pl.Series(name=f"{col}_clean", values=lst))

    @staticmethod
    def _detect_pattern(text: str, pattern: re.Pattern) -> bool:
        """Helper method for pattern detection."""
        return bool(pattern.search(text))

    @staticmethod
    def detect_phone(data: pl.DataFrame, col: str = "item_name") -> pl.DataFrame:
        """Detect phone numbers."""
        return data.with_columns(
            pl.col(col)
            .map_elements(
                lambda x: TextEDA._detect_pattern(x, TextEDA.PHONE_PATTERN),
                return_dtype=pl.Boolean,
            )
            .alias("phone_detect")
        )

    @staticmethod
    def detect_url(data: pl.DataFrame, col: str = "item_name") -> pl.DataFrame:
        """Detect URLs."""
        return data.with_columns(
            pl.col(col)
            .map_elements(
                lambda x: TextEDA._detect_pattern(x, TextEDA.URL_PATTERN),
                return_dtype=pl.Boolean,
            )
            .alias("url_detect")
        )

    @staticmethod
    def detect_words(
        data: pl.DataFrame, patterns: list, col: str = "item_name"
    ) -> pl.DataFrame:
        """Detect words."""
        patterns_set = set(patterns)
        return data.with_columns(
            pl.col(col)
            .map_elements(
                lambda x: bool(patterns_set.intersection(x.split())),
                return_dtype=pl.Boolean,
            )
            .alias("word_detect")
        )
