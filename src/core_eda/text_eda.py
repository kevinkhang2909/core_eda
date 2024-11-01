import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
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
            .map_elements(lambda x: TextEDA._detect_pattern(x, TextEDA.PHONE_PATTERN))
            .alias('phone_detect')
        )

    @staticmethod
    def detect_url(data: pl.DataFrame, col: str = 'item_name') -> pl.DataFrame:
        """Detect URLs."""
        return data.with_columns(
            pl.col(col)
            .map_elements(lambda x: TextEDA._detect_pattern(x, TextEDA.URL_PATTERN))
            .alias('url_detect')
        )

    @staticmethod
    def detect_words(data: pl.DataFrame, patterns: list, col: str = 'item_name') -> pl.DataFrame:
        """Detect words."""
        patterns_set = set(patterns)
        return data.with_columns(
            pl.col(col)
            .map_elements(lambda x: bool(patterns_set.intersection(x.split())))
            .alias('word_detect')
        )


class TextPLOT:
    @staticmethod
    def len_word(data: pl.DataFrame, col_target: str, col_agg: str, **kwargs):
        # kwargs
        name = kwargs.get('name', '')
        xtick: int = kwargs.get('xtick', 0)
        xlim: list = kwargs.get('xlim', [])
        fig_size: tuple = kwargs.get('fig_size', (8, 8))

        data_group_by = data.group_by(col_target).agg(pl.col(col_agg).count())
        # fig
        fig, axes = plt.subplots(1, 2, figsize=fig_size)
        axes = axes.flatten()

        # bar plot
        sns.barplot(data=data_group_by, x=col_target, y=col_agg, ax=axes[0])
        axes[0].set_title(f'Numbers of {name} by {name} length')
        if xtick > 0:
            new_ticks = [i.get_text() for i in axes[0].get_xticklabels()]
            axes[0].set_xticks(range(0, len(new_ticks), xtick))

        # box plot
        sns.boxplot(data, x=col_target, ax=axes[1])
        axes[1].set_title(f'Distribution')
        if xlim:
            axes[1].set_xlim(xlim)

        fig.tight_layout()
