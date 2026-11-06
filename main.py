"""Run the whole TMDB pipeline: fetch -> clean -> EDA -> KPIs -> charts."""
import os
import sys

sys.path.append("src")

from pyspark.sql import SparkSession

from fetch_data import fetch_all_movies, RAW_FILE
from clean_data import load_raw_data, clean_movies
from eda import run_eda
from kpis import (add_profit_and_roi, show_rankings, search_queries,
                  franchise_vs_standalone, top_franchises, top_directors,
                  success_vs_appreciation)
from visualize import make_all_charts


def main():
    # 1. Fetch (skip if we already have the raw file)
    if not os.path.exists(RAW_FILE):
        fetch_all_movies()

    # 2. Start Spark
    spark = SparkSession.builder.appName("TMDBMovieAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 3. Clean
    raw = load_raw_data(spark, RAW_FILE)
    movies = clean_movies(raw)
    movies = add_profit_and_roi(movies)
    movies.cache()  # we use this DataFrame many times, so keep it in memory

    # 4. EDA
    run_eda(movies)

    # 5. KPIs
    show_rankings(movies)
    search_queries(movies)
    franchise_result = franchise_vs_standalone(movies)
    top_franchises(movies)
    top_directors(movies)
    success_vs_appreciation(movies)

    # 6. Charts
    make_all_charts(movies, franchise_result)

    # 7. Save the clean data for the report (via pandas, which works well on Windows)
    os.makedirs("output", exist_ok=True)
    movies.toPandas().to_csv("output/clean_movies.csv", index=False)
    print("\nSaved output/clean_movies.csv")

    spark.stop()


if __name__ == "__main__":
    main()
