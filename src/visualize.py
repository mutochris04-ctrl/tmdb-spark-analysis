"""Step 4: Charts with Matplotlib (Spark results -> pandas -> chart)."""
import os

import matplotlib
matplotlib.use("Agg")  # save charts as files without opening windows
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

CHART_FOLDER = "output/charts"


def save_chart(file_name):
    """Save the current chart into the charts folder and close it."""
    os.makedirs(CHART_FOLDER, exist_ok=True)
    plt.tight_layout()
    plt.savefig(CHART_FOLDER + "/" + file_name)
    plt.close()
    print("Saved chart:", file_name)


def revenue_vs_budget(df):
    """Scatter plot: does a bigger budget bring more revenue?"""
    data = df.select("budget_musd", "revenue_musd").dropna().toPandas()
    plt.figure(figsize=(8, 5))
    plt.scatter(data["budget_musd"], data["revenue_musd"])
    plt.xlabel("Budget (million USD)")
    plt.ylabel("Revenue (million USD)")
    plt.title("Revenue vs Budget")
    save_chart("1_revenue_vs_budget.png")


def roi_by_genre(df):
    """Bar chart of the median ROI for each genre."""
    genres = df.select(F.explode(F.split(F.col("genres"), "\\|")).alias("genre"), "roi")
    data = (genres.dropna().groupBy("genre").agg(F.median("roi").alias("median_roi"))
                  .orderBy(F.desc("median_roi")).toPandas())
    plt.figure(figsize=(9, 5))
    plt.bar(data["genre"], data["median_roi"])
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Median ROI (revenue / budget, times)")
    plt.title("ROI by Genre")
    save_chart("2_roi_by_genre.png")


def popularity_vs_rating(df):
    """Scatter plot: are higher-rated movies also more popular?"""
    data = df.select("popularity", "vote_average").dropna().toPandas()
    plt.figure(figsize=(8, 5))
    plt.scatter(data["vote_average"], data["popularity"])
    plt.xlabel("Rating (vote average)")
    plt.ylabel("Popularity")
    plt.title("Popularity vs Rating")
    save_chart("3_popularity_vs_rating.png")


def yearly_box_office(df):
    """Line chart of total revenue per release year."""
    data = (df.withColumn("year", F.year("release_date"))
              .groupBy("year").agg(F.sum("revenue_musd").alias("total_revenue_musd"))
              .dropna().orderBy("year").toPandas())
    plt.figure(figsize=(8, 5))
    plt.plot(data["year"], data["total_revenue_musd"], marker="o")
    plt.xlabel("Year")
    plt.ylabel("Total revenue (million USD)")
    plt.title("Yearly Box Office Revenue")
    save_chart("4_yearly_box_office.png")


def franchise_chart(franchise_result):
    """Compare franchise and standalone movies on the five required metrics."""
    data = franchise_result.toPandas()
    metrics = [("mean_revenue_musd", "Mean Revenue (M USD)"),
               ("median_roi", "Median ROI (times)"),
               ("mean_budget_musd", "Mean Budget (M USD)"),
               ("mean_popularity", "Mean Popularity"),
               ("mean_rating", "Mean Rating")]
    fig, axes = plt.subplots(1, 5, figsize=(16, 4))
    for i in range(5):
        column, title = metrics[i]
        axes[i].bar(data["movie_type"], data[column])
        axes[i].set_title(title)
    fig.suptitle("Franchise vs Standalone")
    save_chart("5_franchise_vs_standalone.png")


def make_all_charts(df, franchise_result):
    """Create and save all five required charts."""
    revenue_vs_budget(df)
    roi_by_genre(df)
    popularity_vs_rating(df)
    yearly_box_office(df)
    franchise_chart(franchise_result)
