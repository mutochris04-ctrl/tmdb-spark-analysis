"""Step 2: Clean and prepare the movie data with PySpark."""
from pyspark.sql import functions as F

DROP_COLUMNS = ["adult", "imdb_id", "original_title", "video", "homepage"]

FINAL_COLUMNS = ["id", "title", "tagline", "release_date", "genres", "belongs_to_collection",
                 "original_language", "budget_musd", "revenue_musd", "production_companies",
                 "production_countries", "vote_count", "vote_average", "popularity", "runtime",
                 "overview", "spoken_languages", "poster_path", "cast", "cast_size", "director",
                 "crew_size"]


def load_raw_data(spark, path):
    """Read the raw JSON file (one movie per line) into a Spark DataFrame."""
    return spark.read.json(path)


def extract_json_columns(df):
    """Turn the JSON-like columns into simple text, separated by '|'."""
    df = df.withColumn("belongs_to_collection", F.col("belongs_to_collection.name"))
    df = df.withColumn("genres", F.concat_ws("|", F.col("genres.name")))
    df = df.withColumn("spoken_languages", F.concat_ws("|", F.col("spoken_languages.english_name")))
    df = df.withColumn("production_countries", F.concat_ws("|", F.col("production_countries.name")))
    df = df.withColumn("production_companies", F.concat_ws("|", F.col("production_companies.name")))

    # Cast and crew come from the "credits" part of the API answer
    df = df.withColumn("cast", F.concat_ws("|", F.col("credits.cast.name")))
    df = df.withColumn("cast_size", F.size(F.col("credits.cast")))
    df = df.withColumn("crew_size", F.size(F.col("credits.crew")))

    # Director: keep only crew members whose job is "Director"
    directors = F.filter(F.col("credits.crew"), lambda person: person["job"] == "Director")
    # sort the names so "Joe Russo|Anthony Russo" and "Anthony Russo|Joe Russo" become the same
    df = df.withColumn("director", F.concat_ws("|", F.array_sort(directors["name"])))

    df = df.drop("credits")
    return df


def fix_types_and_values(df):
    """Fix data types, replace unrealistic values, and convert money to millions."""
    df = df.withColumn("id", F.col("id").cast("int"))
    df = df.withColumn("budget", F.col("budget").cast("double"))
    df = df.withColumn("revenue", F.col("revenue").cast("double"))
    df = df.withColumn("popularity", F.col("popularity").cast("double"))
    df = df.withColumn("runtime", F.col("runtime").cast("double"))
    df = df.withColumn("release_date", F.to_date(F.col("release_date")))

    # 0 is not a real budget, revenue or runtime -> make it empty (null)
    for column in ["budget", "revenue", "runtime"]:
        df = df.withColumn(column, F.when(F.col(column) == 0, None).otherwise(F.col(column)))

    # Money in million USD
    df = df.withColumn("budget_musd", F.col("budget") / 1000000)
    df = df.withColumn("revenue_musd", F.col("revenue") / 1000000)

    # No votes means the rating is not meaningful
    df = df.withColumn("vote_average",
                       F.when(F.col("vote_count") == 0, None).otherwise(F.col("vote_average")))

    # Placeholder text and empty text -> null
    for column in ["overview", "tagline"]:
        df = df.withColumn(column,
                           F.when(F.col(column).isin("No Data", ""), None).otherwise(F.col(column)))

    # Empty text after joining lists -> null
    for column in ["genres", "spoken_languages", "production_countries",
                   "production_companies", "cast", "director"]:
        df = df.withColumn(column, F.when(F.col(column) == "", None).otherwise(F.col(column)))

    return df


def remove_bad_rows(df):
    """Remove duplicates, rows without id/title, rows with too little data, and unreleased movies."""
    df = df.dropDuplicates(["id"])
    df = df.dropna(subset=["id", "title"])
    # Lab rule: keep rows with at least 10 non-null values.
    # A movie with fewer filled columns has too little information to analyse fairly.
    df = df.dropna(thresh=10)
    df = df.filter(F.col("status") == "Released")
    df = df.drop("status")
    return df


def clean_movies(df):
    """Run all cleaning steps and return the final DataFrame with the required column order."""
    df = df.drop(*DROP_COLUMNS)
    df = extract_json_columns(df)
    df = fix_types_and_values(df)
    df = remove_bad_rows(df)
    # Spark DataFrames have no index, so instead of "reset index" we sort by id
    df = df.select(FINAL_COLUMNS).orderBy("id")
    return df
