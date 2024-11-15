from pathlib import Path
from src.core_eda.eda_table import EDA_File


file = Path().home() / 'Downloads/Data/fss_item_sample/fss_items_clean.parquet'

e = EDA_File(file, prime_key=['same_item_id'])
z = e.describe_group('same_level1_global_be_category', col_describe='same_item_price')
dict_ = e.analyze()
print(e.value_count('same_level1_global_be_category'))
