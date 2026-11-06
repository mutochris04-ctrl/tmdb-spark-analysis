# TMDB Movie Data Analysis with Spark

A movie data analysis pipeline built with PySpark.
It fetches movie data from the TMDB API, cleans and transforms it,
calculates KPIs and presents the results with charts.

## Project structure

```
main.py            runs the whole pipeline in order
src/fetch_data.py  Step 1: fetch the 19 movies from the TMDB API (with retry and backoff)
src/clean_data.py  Step 2: clean and transform the data with PySpark
src/eda.py         Exploratory data analysis
src/kpis.py        Step 3: rankings, searches, franchises and directors
src/visualize.py   Step 4: the five Matplotlib charts
output/charts/     the saved charts
REPORT.md          final report: methodology, key insights and conclusions
```

## Setup

1. Install the packages: `pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and add your TMDB API key.

## How to run

```
python main.py
```

The first run fetches the movies and saves them in `data/raw_movies.json`.
Later runs reuse that file instead of calling the API again.
The results are printed in the terminal, the charts are saved in `output/charts/`
and the clean data in `output/clean_movies.csv`.

## Results

See [REPORT.md](REPORT.md) for the methodology, key findings, charts and limitations.

## Status

Complete.
