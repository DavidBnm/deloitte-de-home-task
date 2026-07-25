import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    logging.info("Initializing SparkSession for Dataproc execution...")

    spark = SparkSession.builder \
        .appName("DeloitteDataSkewAnalysis") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    try:
        logging.info("Reading dataset directly from BigQuery table: raw_data.addresses...")

        df = spark.read.format("bigquery") \
            .option("table", "deloitte-de-home-ty.raw_data.addresses") \
            .option("temporaryGcsBucket", "deloitte-code-bucket") \
            .load()

        logging.info("Executing GroupBy aggregation on the skewed 'city' column...")

        aggregated_df = df.groupBy("city") \
            .agg(
                F.count("address_id").alias("total_addresses"),
                F.concat_ws(", ", F.collect_list("street")).alias("compiled_streets")
            )

        logging.info("Evaluating Spark Action via DataFrame collection...")
        result = aggregated_df.collect()

        logging.info("Aggregation completed successfully. Sample Output:")
        for row in result[:5]:
            logging.info(f"City: {row['city']} | Total Count: {row['total_addresses']}")

        logging.info("Pipeline executed successfully.")

    except Exception as e:
        logging.critical(f"Spark Job failed during execution lifecycle: {e}")
        raise
    finally:
        logging.info("Shutting down active SparkSession context...")
        spark.stop()

if __name__ == "__main__":
    main()