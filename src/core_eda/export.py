class Export:
    def __init__(self):
        pass

    def csv_to_parquet(self):
        """
        COPY (SELECT * FROM read_csv_auto(
        'http://raw.githubusercontent.com/fivethirtyeight/data/master/bechdel/movies.csv'))
        TO 'movies.parquet' (FORMAT 'parquet');
        """