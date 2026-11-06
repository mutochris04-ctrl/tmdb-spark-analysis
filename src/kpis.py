"""Step 3: KPIs - rankings, searches, franchises and directors."""
from pyspark.sql import functions as F


def add_profit_and_roi(df):
    """Add profit (revenue - budget) and ROI (revenue / budget) columns."""
    df = df.withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
    # ROI as defined in the lab instructions: Revenue / Budget
    # (e.g. 3.0 means the movie earned 3 times its budget)
    df = df.withColumn("roi",
                       F.when(F.col("budget_musd") > 0,
                              F.col("revenue_musd") / F.col("budget_musd")))
    return df


def rank_movies(df, column, highest=True, min_budget=None, min_votes=None, n=5):
    """Ranking helper used for every KPI: filter if needed, then sort and keep the top n."""
    result = df.filter(F.col(column).isNotNull())

    if min_budget is not None:
        result = result.filter(F.col("budget_musd") >= min_budget)
    if min_votes is not None:
        result = result.filter(F.col("vote_count") >= min_votes)

    if highest:
        result = result.orderBy(F.desc(column))
    else:
        result = result.orderBy(F.asc(column))

    return result.select("title", column).limit(n)


def show_rankings(df):
    """Show the best and worst movies for each KPI."""
    rankings = [
        ("Highest Revenue", "revenue_musd", True, None, None),
        ("Highest Budget", "budget_musd", True, None, None),
        ("Highest Profit", "profit_musd", True, None, None),
        ("Lowest Profit", "profit_musd", False, None, None),
        ("Highest ROI (budget >= 10M)", "roi", True, 10, None),
        ("Lowest ROI (budget >= 10M)", "roi", False, 10, None),
        ("Most Voted", "vote_count", True, None, None),
        ("Highest Rated (>= 10 votes)", "vote_average", True, None, 10),
        ("Lowest Rated (>= 10 votes)", "vote_average", False, None, 10),
        ("Most Popular", "popularity", True, None, None),
    ]
    for name, column, highest, min_budget, min_votes in rankings:
        print("\n" + name)
        rank_movies(df, column, highest, min_budget, min_votes).show(truncate=False)


def search_queries(df):
    """Search 1: Sci-Fi Action with Bruce Willis. Search 2: Uma Thurman + Tarantino."""
    print("\nSearch 1: Science Fiction Action movies starring Bruce Willis")
    search1 = (df.filter(F.col("genres").contains("Science Fiction"))
                 .filter(F.col("genres").contains("Action"))
                 .filter(F.col("cast").contains("Bruce Willis"))
                 .orderBy(F.desc("vote_average")))
    search1.select("title", "genres", "vote_average").show(truncate=False)

    print("\nSearch 2: Uma Thurman movies directed by Quentin Tarantino")
    search2 = (df.filter(F.col("cast").contains("Uma Thurman"))
                 .filter(F.col("director") == "Quentin Tarantino")
                 .orderBy(F.asc("runtime")))
    search2.select("title", "director", "runtime").show(truncate=False)


def franchise_vs_standalone(df):
    """Compare movies in a collection (franchise) with standalone movies."""
    df = df.withColumn("movie_type",
                       F.when(F.col("belongs_to_collection").isNotNull(), "Franchise")
                        .otherwise("Standalone"))
    result = df.groupBy("movie_type").agg(
        F.round(F.mean("revenue_musd"), 2).alias("mean_revenue_musd"),
        F.round(F.median("roi"), 2).alias("median_roi"),
        F.round(F.mean("budget_musd"), 2).alias("mean_budget_musd"),
        F.round(F.mean("popularity"), 2).alias("mean_popularity"),
        F.round(F.mean("vote_average"), 2).alias("mean_rating"),
    )
    print("\nFranchise vs Standalone")
    result.show()
    return result


def top_franchises(df):
    """Most successful franchises."""
    result = (df.filter(F.col("belongs_to_collection").isNotNull())
                .groupBy("belongs_to_collection").agg(
                    F.count("id").alias("movies"),
                    F.round(F.sum("budget_musd"), 2).alias("total_budget_musd"),
                    F.round(F.mean("budget_musd"), 2).alias("mean_budget_musd"),
                    F.round(F.sum("revenue_musd"), 2).alias("total_revenue_musd"),
                    F.round(F.mean("revenue_musd"), 2).alias("mean_revenue_musd"),
                    F.round(F.mean("vote_average"), 2).alias("mean_rating"))
                .orderBy(F.desc("total_revenue_musd")))
    print("\nMost successful franchises")
    result.show(truncate=False)
    return result


def top_directors(df):
    """Most successful directors."""
    result = (df.filter(F.col("director").isNotNull())
                .groupBy("director").agg(
                    F.count("id").alias("movies"),
                    F.round(F.sum("revenue_musd"), 2).alias("total_revenue_musd"),
                    F.round(F.mean("vote_average"), 2).alias("mean_rating"))
                .orderBy(F.desc("total_revenue_musd")))
    print("\nMost successful directors")
    result.show(truncate=False)
    return result


def success_vs_appreciation(df):
    """Extra analysis: does money (profit) agree with audience appreciation (rating)?"""
    data = df.filter(F.col("profit_musd").isNotNull() & F.col("vote_average").isNotNull())

    # The middle values (medians) split movies into "high" and "low"
    middle_profit = data.select(F.median("profit_musd")).first()[0]
    middle_rating = data.select(F.median("vote_average")).first()[0]

    high_profit = F.col("profit_musd") >= middle_profit
    high_rating = F.col("vote_average") >= middle_rating

    data = data.withColumn(
        "success_group",
        F.when(high_profit & high_rating, "Hit with money and audience")
         .when(high_profit, "Money hit, lower rating")
         .when(high_rating, "Loved, lower profit")
         .otherwise("Lower profit and rating"))

    print("\nCommercial success vs audience appreciation")
    print("Median profit (M USD):", round(middle_profit, 2), "| Median rating:", round(middle_rating, 2))
    data.select("title", "profit_musd", "vote_average", "success_group") \
        .orderBy("success_group", F.desc("profit_musd")).show(truncate=False)
    data.groupBy("success_group").count().show(truncate=False)
    return data
