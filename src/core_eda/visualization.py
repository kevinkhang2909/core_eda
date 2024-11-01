import seaborn as sns
import polars as pl
from textwrap import TextWrapper
from PIL import Image
import numpy as np
import re
from pathlib import Path
from matplotlib import font_manager
import matplotlib.pyplot as plt
from core_pro.ultilities import make_dir


class Functions:
    def __init__(self, save_path: Path = None, **kwargs):
        self.fig_size = kwargs.get("fig_size", (8, 6))
        self.save_path = save_path
        self.default_cmap = sns.diverging_palette(230, 20, as_cmap=True)

    def _create_figure(self):
        return plt.subplots(figsize=self.fig_size)

    def _save_figure(self, fig: plt.Figure, save_path: Path = None) -> None:
        if save_path:
            make_dir(save_path)
            fig.savefig(save_path, bbox_inches='tight', dpi=300)


class DataPLOT(Functions):
    def __init__(self, save_path: Path = None, **kwargs):
        super().__init__(save_path=save_path, **kwargs)

    def bar(self, data, x: str,  y: str):
        fig, ax = self._create_figure()
        sns.barplot(data=data, x=x, y=y, ax=ax)
        for container in ax.containers:
            ax.bar_label(container, fmt='%.2f')
        self._save_figure(fig, self.save_path)

    def correlation_heatmap(self, data):
        if isinstance(data, pl.DataFrame):
            data = data.to_pandas()
        fig, ax = self._create_figure()
        sns.heatmap(data.corr(), cmap=self.default_cmap, annot=True, linewidths=.5, fmt=",.2f")
        self._save_figure(fig, self.save_path)


class ImageTextPLOT:
    def __init__(self, truncated: int = 2500, line_wrap: int = 75, fig_size: tuple = (12, 7), **kwargs):
        self.font = self._get_font()
        self.truncated = truncated
        self.line_wrap = line_wrap
        self.fig_size = fig_size
        self.suptitle = kwargs.get('suptitle', 'Chart')
        self.suptitle_size = kwargs.get('suptitle_size', 16)
        self.text_size = kwargs.get('text_size', 10)

    @staticmethod
    def _get_font():
        name = 'OpenSans-VariableFont_wdth,wght.ttf'
        path = Path(f'/media/kevin/data_4t/Test/font/{name}')
        if str(Path.home()) == '/Users/kevinkhang':
            path = Path.home() / f'Downloads/Data/Font/{name}'

        if not path.exists():
            print('Please Download Font')

        font = font_manager.FontEntry(fname=str(path))
        font_manager.fontManager.ttflist.append(font)
        return font.name

    def clean_string(self, obj, indent: int = 0):
        pad_long_text = '...\n\n[long text]'
        indent_str = ' ' * indent

        if isinstance(obj, dict):
            lines = ['{']
            for key, value in obj.items():
                lines.append(f'{indent_str}  "{key}": {self.clean_string(value, indent + 2)}')
            lines.append(f'{indent_str}}}')
            return '\n'.join(lines)

        if isinstance(obj, list):
            lines = ['[']
            lines.extend(f'{indent_str}  {self.clean_string(item, indent + 2)},' for item in obj)
            if len(lines) > 1:
                lines[-1] = lines[-1].rstrip(',')
            lines.append(f'{indent_str}]')
            return '\n'.join(lines)

        if isinstance(obj, str):
            truncated_text = re.sub(r'\s+', ' ', obj.strip())
            if len(truncated_text) > self.truncated:
                truncated_text = truncated_text[:self.truncated] + pad_long_text
            wrapped_text = TextWrapper(width=self.line_wrap).wrap(truncated_text)
            return '\n'.join(wrapped_text)

        if obj is None:
            return 'null'

        return str(obj).lower()

    def plot_compare_texts(
            self,
            texts: list,
            titles: list = None,
            save_path: Path = None,
    ):
        fig, axes = plt.subplots(1, 2, figsize=self.fig_size )
        fig.suptitle(self.suptitle, fontsize=self.suptitle_size, fontdict={'family': self.font})

        if not titles:
            titles = ['Raw Data', 'Edited Data']

        for ax, t, ti in zip(axes, texts, titles):
            # clean text
            clean_text = self.clean_string(t)
            # plot
            ax.text(0.05, 1, clean_text, fontsize=self.text_size, va='top', ha='left', wrap=True, fontdict={'family': self.font})
            ax.set_title(ti)
            ax.axis('off')

        # line
        line = plt.Line2D([0.5, 0.5], [0.05, 0.92], transform=fig.transFigure, ls='--', color='grey', linewidth=1)
        fig.add_artist(line)

        # clean
        fig.tight_layout()
        # save
        if save_path:
            fig.savefig(save_path, bbox_inches='tight', dpi=300)

    def plot_one_image_long_caption(self, file_path: Path, caption: str, save_path: Path = None):
        nrows, ncols = 1, 2
        fig, axes = plt.subplots(nrows, ncols, figsize=self.fig_size)
        # img
        axes[0].imshow(Image.open(file_path))
        # text
        clean_text = self.clean_string(caption)
        axes[1].text(0.05, 1, clean_text, fontsize=self.text_size, va='top', ha='left', wrap=True, fontdict={'family': self.font})
        # clean
        for i in range(nrows + 1):
            axes[i].axis('off')
        fig.tight_layout()
        # save
        if save_path:
            fig.savefig(save_path, bbox_inches='tight', dpi=300)

    def plot_multiple_images(self, file_paths: list, num_cols: int = 3):
        # layout
        num_images, ncol = len(file_paths), min(len(file_paths), num_cols)
        nrow = int(np.ceil(num_images / ncol))
        fig, axes = plt.subplots(nrow, ncol, figsize=self.fig_size)
        axes = axes.flatten()

        # plot
        for i, image in enumerate(file_paths):
            axes[i].imshow(Image.open(image))
            axes[i].axis('off')
        # remove axes
        for _ in range(num_images, ncol * nrow, 1):
            axes[_].remove()
        fig.tight_layout()


class TextPLOT(Functions):
    def __init__(self, save_path: Path = None, **kwargs):
        super().__init__(save_path=save_path, **kwargs)

    def len_word(self, data: pl.DataFrame, col_target: str, col_agg: str, **kwargs):
        # kwargs
        name = kwargs.get('name', '')
        xtick: int = kwargs.get('xtick', 20)
        xlim: list = kwargs.get('xlim', [0, 60])

        data_group_by = data.group_by(col_target).agg(pl.col(col_agg).count())
        # fig
        fig, axes = plt.subplots(1, 2, figsize=self.fig_size)
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
        self._save_figure(fig, self.save_path)
