"""Exploratory Data Analysis (EDA): first look at the cleaned data."""
from pyspark.sql import functions as F


def show_value_counts(df, column, n=10):
    """Show how often each value appears (like pandas value_counts)."""
    print("\nValue counts for", column)
    df.groupBy(column).count().orderBy(F.desc("count")).show(n, truncate=False)


def count_missing(df):
    """Show how many null values each column has."""
    print("\nMissing values per column")
    missing = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
    missing.show(vertical=True)


def run_eda(df):
    """Print the main EDA results."""
    print("\nNumber of movies:", df.count())
    print("Number of columns:", len(df.columns))
    df.printSchema()

    count_missing(df)

    print("\nSummary statistics")
    df.select("budget_musd", "revenue_musd", "vote_average", "vote_count",
              "popularity", "runtime").describe().show()

    show_value_counts(df, "original_language")
    show_value_counts(df, "belongs_to_collection")
    show_value_counts(df, "director")

    # Genres hold several values ("Action|Adventure"), so split them first
    genres = df.select(F.explode(F.split(F.col("genres"), "\\|")).alias("genre"))
    show_value_counts(genres, "genre")
