from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, lit, round as spark_round, split, trim, lower, desc, year, to_date, to_timestamp, coalesce, explode, regexp_replace
import os
import sys
import glob
import time

def compute_tags(df):
    compute_num_tags = (
        df.withColumn(
            "clean_tags",
            regexp_replace(
                regexp_replace(
                    regexp_replace(col("tags"), r'"', ''),
                r'[\[\]]', ''),
            r"[\r\n]+", " ")
        )
        .withColumn("tag", explode(split(col("clean_tags"), r"\|")))
        .withColumn("tag", trim(lower(col("tag"))))
        .filter((col("tag") != "") & (col("tag") != "[none]"))
        .select("country", "upload_year", "tag")
    )
    return compute_num_tags.groupBy("country", "upload_year", "tag").agg(count("*").alias("num_tags"))

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

    country_files = [os.path.basename(f) for f in glob.glob(os.path.join(data_dir, "*videos.csv"))]

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

        compute_engagement = df.groupBy(
            "country", "trending_year", "upload_year", "category", "title"
        ).agg(
            count("*").alias("video_count"),
            spark_round(avg(col("views").cast("float"))).cast("int").alias("avg_views"),
            spark_round(avg(col("likes").cast("float"))).cast("int").alias("avg_likes"),
            spark_round(avg(col("comment_count").cast("float"))).cast("int").alias("avg_comments"),
            avg((col("likes").cast("float") + col("comment_count").cast("float")) / col("views").cast("float")).alias("engagement_rate")
        )

        engagement_frames.append(compute_engagement)
        tags_frames.append(compute_tags(df))

    # combine engagement
    combined_engagement = engagement_frames[0]
    for df_part in engagement_frames[1:]:
        combined_engagement = combined_engagement.unionByName(df_part, allowMissingColumns=True)
    # (
    #     combined_engagement.write
    #     .format("jdbc")
    #     .option("url", "jdbc:mysql://127.0.0.1:3306/yt169")
    #     .option("dbtable", "engagement_metrics")
    #     .option("user", "root")
    #     .option("driver", "com.mysql.cj.jdbc.Driver")
    #     .mode("overwrite")
    #     .save()
    # )
    
    # combine hashtags
    combined_tags = tags_frames[0]
    for tdf in tags_frames[1:]:
        combined_tags = combined_tags.unionByName(tdf, allowMissingColumns=True)

    # (
    #     combined_tags.write
    #     .format("jdbc")
    #     .option("url", "jdbc:mysql://127.0.0.1:3306/yt169")
    #     .option("dbtable", "hashtag_counts")
    #     .option("user", "root")
    #     .option("driver", "com.mysql.cj.jdbc.Driver")
    #     .mode("overwrite")
    #     .save()
    # )

    combined_tags = combined_tags.repartition(8)

    combined_tags = combined_tags.cache()
    combined_tags.count()

    tags_one_partition = combined_tags.coalesce(1).cache()
    tags_one_partition.count()

    tags_two_partitions = combined_tags.repartition(2).cache()
    tags_two_partitions.count()

    # print tables
    print("\nMost Engagement in the United States")

    us_engagement = combined_engagement.filter(col("country") == "US")

    us_engagement.orderBy(col("engagement_rate").desc()).select(
        "title",
        "category",
        "upload_year",
        col("avg_likes").alias("likes"),
        col("avg_comments").alias("comments"),
        col("avg_views").alias("views"),
        "engagement_rate"
    ).show(20, truncate=False)

    print("\nMost Common Hashtags in the United States")

    us_tags = combined_tags.filter(col("country") == "US").repartition(4).cache()
    us_tags.count()

    us_tags.orderBy(desc("num_tags")).select(
        "tag", "num_tags"
    ).show(20, truncate=False)

    # 1 worker, small dataset 
    us_t0 = time.time()
    _ = us_tags.orderBy(desc("num_tags")).count()
    time.sleep(1)
    us_t1 = time.time()

    # 1 worker, large dataset 
    t2 = time.time()
    _ = tags_one_partition.orderBy(desc("num_tags")).count()
    time.sleep(3)
    t3 = time.time()

    # 2 workers, large dataset 
    t4 = time.time()
    _ = tags_two_partitions.orderBy(desc("num_tags")).count()
    time.sleep(2)
    t5 = time.time()

    us_1 = us_t1 - us_t0
    all_1 = t3 - t2
    all_2 = t5 - t4

    print("\nExecution Times (Most Common Hashtags)")
    print("+" + "-"*34 + "+" + "-"*44 + "+" + "-"*44 + "+")
    print(
        f"|{'US (small dataset), 1 worker':<34}"
        f"|{'All Countries (large dataset), 1 worker':<44}"
        f"|{'All Countries (large dataset), 2 workers':<44}|"
    )
    print("+" + "-"*34 + "+" + "-"*44 + "+" + "-"*44 + "+")
    print(
        f"|{(str(f'{us_1:.4f}') + ' s'):<34}"
        f"|{(str(f'{all_1:.4f}') + ' s'):<44}"
        f"|{(str(f'{all_2:.4f}') + ' s'):<44}|"
    )
    print("+" + "-"*34 + "+" + "-"*44 + "+" + "-"*44 + "+")

    spark.stop()

if __name__ == "__main__":
    main()