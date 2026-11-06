# Final Report: TMDB Movie Data Analysis with Spark

**Author:** Christine Umutoni
**Module:** DEM05 – Big Data, Data Processing, and Pipelines (Lab 1)

## 1. Goal

Build a movie data pipeline with PySpark: fetch movies from the TMDB API, clean them,
calculate KPIs, and present the main findings with charts.

## 2. Methodology

The pipeline is split into small modules in `src/`, and `main.py` runs them in order:

| Step | File | What it does |
|---|---|---|
| 1. Fetch | `fetch_data.py` | Calls the TMDB API for the 19 given movie IDs (with credits). Retries up to 3 times with a growing wait (2 s, then 4 s). Saves one movie per line in `data/raw_movies.json`. |
| 2. Clean | `clean_data.py` | Drops the 5 irrelevant columns, turns the JSON-like columns into text separated by `\|`, extracts cast, director and crew size, fixes data types, replaces 0 budget/revenue/runtime with null, converts money to million USD, removes duplicates, keeps rows with at least 10 non-null values and only "Released" movies, and puts the columns in the required order. |
| 3. EDA | `eda.py` | Counts rows, columns and missing values, shows summary statistics and value counts for the extracted columns. |
| 4. KPIs | `kpis.py` | Adds profit (revenue − budget) and ROI (revenue ÷ budget), ranks movies, runs the two searches, and compares franchises, standalone movies and directors. |
| 5. Charts | `visualize.py` | Draws the 5 required Matplotlib charts into `output/charts/`. |

**Design choices**

- **Ranking function:** the lab asks for a user-defined function to streamline rankings.
  I wrote one reusable Python function, `rank_movies()`, that filters, sorts and keeps the top 5 for every KPI.
  I did not use a Spark UDF (`F.udf`), because Spark's built-in functions are faster and a UDF is not needed for sorting.
- **ROI** follows the lab definition: Revenue ÷ Budget (3.0 means the movie earned 3 times its budget).
- **"Reset index":** Spark DataFrames have no index, so the final data is sorted by `id` instead.
- **Performance:** the clean DataFrame is cached because it is used many times.
  Data is only converted to pandas (`toPandas()`) after Spark has reduced it, for charts and the CSV.

## 3. The data

- 19 IDs were requested. ID `0` does not exist on TMDB (404), so **18 movies** were analysed.
- After cleaning there were no missing values except `belongs_to_collection`
  (2 standalone movies: *Titanic* and *Beauty and the Beast*).
- All 18 movies are English-language, US-produced blockbusters. Budgets range from 125 to 356 million USD,
  revenues from 1,243 to 2,942 million USD, and ratings from 6.5 to 8.2.

## 4. Key findings

**Best and worst performers**

| KPI | Top movie |
|---|---|
| Highest revenue | Avengers: Endgame (2,942 M USD) |
| Highest budget | Avengers: Endgame (356 M USD) |
| Highest profit | Avatar (2,687 M USD) |
| Lowest profit | Star Wars: The Last Jedi (1,034 M USD) |
| Highest ROI (budget ≥ 10 M) | Avatar (12.3×) |
| Lowest ROI (budget ≥ 10 M) | Star Wars: The Last Jedi (4.4×) |
| Most voted | The Avengers (40,259 votes) |
| Highest rated (≥ 10 votes) | Avengers: Endgame (8.25) |
| Lowest rated (≥ 10 votes) | Jurassic World: Fallen Kingdom (6.52) |
| Most popular | Avengers: Endgame (64.9) |

Every movie in this dataset made a profit; even the "lowest profit" movie earned over 1 billion USD.

**Searches**

Both searches return **no movies**. This is correct, not an error: none of the 18 movies stars
Bruce Willis or Uma Thurman, and none is directed by Quentin Tarantino.
The search code would find them if they were in the data.

**Franchise vs standalone**

| | Mean revenue (M USD) | Median ROI | Mean budget (M USD) | Mean popularity | Mean rating |
|---|---|---|---|---|---|
| Standalone (2 movies) | 1,765 | 9.6× | 180 | 26.5 | 7.44 |
| Franchise (16 movies) | 1,692 | 7.8× | 218 | 27.1 | 7.40 |

Franchise movies cost more on average but did not earn more, so their ROI is lower.
Ratings and popularity are almost the same. With only 2 standalone movies, this comparison is not strong evidence.

**Most successful franchises and directors**

- The Avengers Collection leads clearly: 4 movies, 7,919 M USD total revenue, mean rating 7.96.
- James Cameron has the highest total revenue (5,188 M USD from *Avatar* and *Titanic*).
  Anthony and Joe Russo are close behind (4,995 M USD) with the highest mean rating (8.25).

**What the charts show**

1. **Revenue vs budget:** a bigger budget does not guarantee more revenue. *Avatar* (237 M budget)
   earned almost as much as *Avengers: Endgame* (356 M budget).
2. **ROI by genre:** Comedy and Romance have the highest median ROI (about 9.7×);
   Action and Science Fiction the lowest (about 7×). Expensive genres return less per dollar.
3. **Popularity vs rating:** higher-rated movies tend to be more popular; the most popular movies
   (*Endgame*, *Infinity War*) are also the highest rated.
4. **Yearly box office:** the highest totals are in 2015 and 2018–2019, the years with several big franchise releases.
   The years depend on which movies were in the list, so this is not a general market trend.
5. **Franchise vs standalone:** shows the five comparison metrics side by side (see the table above).

**Extra analysis: money vs audience**

Splitting movies at the median profit (1,301 M USD) and median rating (7.26):
4 movies were hits with both money and audience (*Avatar*, *Endgame*, *Titanic*, *Infinity War*),
while 5 earned a lot but were rated lower (e.g. *Star Wars: The Force Awakens*, *Jurassic World*).
Commercial success and audience appreciation do not always go together.

## 5. Limitations

- Only 18 movies, all very successful blockbusters, so the results describe top movies, not the whole film market.
- Revenue is not adjusted for inflation, which favours recent movies.
- The searches return nothing because of the movie list, not because of the code.

## 6. Conclusion

The pipeline meets the lab's steps: it fetches, cleans, explores and analyses the data with PySpark and
presents the results with five charts. The main insights are that big budgets do not guarantee the best return,
the Avengers franchise dominates this dataset, and high ratings and high popularity usually go together.
