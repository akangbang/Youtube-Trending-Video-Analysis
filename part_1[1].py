from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # start Spark session
    spark = (
        SparkSession.builder
        .appName("Find People by Zipcode")
        .getOrCreate()
    )

    # read .csv file
    df = (
        spark.read
            .option("header", True)       
            .option("inferSchema", True)  
            .csv("test.csv")             
    )

    # print schema
    df.printSchema()

    # filter and count rows where zipcode is 78727
    count = df.filter(col("zipcode") == 78727).count()

    # print result
    print(f"Number of people that live in zip code 78727: {count}")

    # stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()