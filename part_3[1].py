from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, lit, year, to_date, to_timestamp, coalesce, explode, regexp_replace, split 
import os
import glob

def compute_tags(df):
    cleaned = (
        df.withColumn(
            "clean_tags",
            regexp_replace(
                regexp_replace(
                    regexp_replace(col("tags"), r'"', ''),
                    r'[\[\]]', ''
                ),
                r"[\r\n]+", " "
            )
        )
        .withColumn("tag", explode(split(col("clean_tags"), r"\|")))
        .filter(
            (col("tag").isNotNull()) &
            (col("tag") != "") &
            (~col("tag").isin("[none]", "none"))
        )
        .select("country", "upload_year", "tag")
    )

    return (
        cleaned.groupBy("country", "upload_year", "tag")
        .agg(count("*").alias("num_tags"))
    )

def main():
    # start Spark session
    spark = (
        SparkSession.builder
        .appName("YouTube Category Metrics by Country")
        .master("local[4]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config(
            "spark.jars",
            os.path.expanduser("~/mysql-connector/mysql-connector-j-8.3.0/mysql-connector-j-8.3.0.jar")
        )
        .getOrCreate()
    )

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "../dataset")

    country_files = [
        os.path.basename(f)
        for f in glob.glob(os.path.join(data_dir, "*videos.csv"))
    ]

    engagement_frames = []
    tags_frames = []

    for csv_file in country_files:
        country = csv_file[:2]
        csv_path = os.path.join(data_dir, csv_file)
        json_path = os.path.join(data_dir, f"{country}_category_id.json")

        # read .csv files
        df = (
            spark.read
            .option("header", True)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .csv(csv_path)
        ).withColumn("country", lit(country))

        # trending year
        if "trending_date" in df.columns:
            df = df.withColumn("trending_year", year(to_date(col("trending_date"), "yy.dd.MM")))
        else:
            df = df.withColumn("trending_year", lit(None).cast("int"))

        # upload year
        if "publish_time" in df.columns:
            df = df.withColumn(
                "upload_year",
                year(
                    coalesce(
                        to_timestamp(col("publish_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                        to_timestamp(col("publish_time"))
                    )
                )
            )
        else:
            df = df.withColumn("upload_year", lit(None).cast("int"))

        df = df.withColumn("title", col("title"))

        # read .json files
        cat = spark.read.option("multiline", "true").json(json_path)
        cat = cat.withColumn("items", explode(col("items")))
        cat = cat.select(
            col("items.id").alias("category_id"),
            col("items.snippet.title").alias("category")
        )

        df = df.join(cat, on="category_id", how="left")

        engagement = df.groupBy(
            "country", "trending_year", "upload_year", "category", "title"
        ).agg(
            count("*").alias("video_count"),
            avg(col("views").cast("float")).alias("avg_views"),
            avg(col("likes").cast("float")).alias("avg_likes"),
            avg(col("comment_count").cast("float")).alias("avg_comments"),
            avg(
                col("likes").cast("float") + col("comment_count").cast("float")
            ).alias("avg_likes_plus_comments")
        )

        engagement_frames.append(engagement)
        tags_frames.append(compute_tags(df))

    # engagement
    engagement_rate = engagement_frames[0]
    for e in engagement_frames[1:]:
        engagement_rate = engagement_rate.unionByName(e, allowMissingColumns=True)
        
    # categories
    category_count = (
        engagement_rate.groupBy("country", "category")
        .agg(count("*").alias("num_categories"))
    )

    # tags
    tag_count = tags_frames[0]
    for t in tags_frames[1:]:
        tag_count = tag_count.unionByName(t, allowMissingColumns=True)

    # store outputs in MySQL
    (
        engagement_rate.write
        .format("jdbc")
        .option("url", "jdbc:mysql://127.0.0.1:3306/yt169")
        .option("dbtable", "engagement_rate")
        .option("user", "root")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("overwrite")
        .save()
    )

    (
        category_count.write
        .format("jdbc")
        .option("url", "jdbc:mysql://127.0.0.1:3306/yt169")
        .option("dbtable", "category_count")
        .option("user", "root")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("overwrite")
        .save()
    )

    (
        tag_count.write
        .format("jdbc")
        .option("url", "jdbc:mysql://127.0.0.1:3306/yt169")
        .option("dbtable", "tag_count")
        .option("user", "root")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("overwrite")
        .save()
    )

    spark.stop()

if __name__ == "__main__":
    main()