"""AWS Glue (PySpark) job: raw sales CSV -> validated, de-duplicated Parquet.

Reads   s3://<bucket>/raw/sales/ingestion_date=<date>/
Writes  s3://<bucket>/processed/sales/year=YYYY/month=MM/   (valid rows, Parquet)
        s3://<bucket>/rejected/sales/ingestion_date=<date>/ (invalid rows + reason, CSV)
        s3://<bucket>/reports/sales/ingestion_date=<date>/  (run metrics, JSON)

Rules (kept in sync with src/validation.py):
  * required fields present: order_id, order_date, product, city, quantity, unit_price
  * order_id matches ORD + digits
  * order_date is a valid YYYY-MM-DD date, not in the future
  * quantity is a whole number in 1..100
  * unit_price is a positive number
  * duplicates: same order_id -> keep the row with the latest event_timestamp

Idempotency: processed partitions are overwritten dynamically, so re-running the same
ingestion date replaces its partitions instead of double-counting sales.

Job parameters:
  --JOB_NAME        set by Glue
  --BUCKET          data lake bucket name
  --INGESTION_DATE  YYYY-MM-DD folder under raw/sales/ to process
"""

import json
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

REQUIRED = ["order_id", "order_date", "product", "city", "quantity", "unit_price"]
PAYMENT_METHODS = ["UPI", "Card", "NetBanking", "COD"]
ORDER_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]


def add_rejection_reason(df: DataFrame) -> DataFrame:
    """Add a `rejection_reason` column; null means the row is valid."""
    checks = [
        (F.col(c).isNull() | (F.trim(F.col(c)) == ""), f"missing {c}") for c in REQUIRED
    ] + [
        (~F.col("order_id").rlike(r"^ORD\d{4,}$"), "bad order_id"),
        (F.col("order_date_parsed").isNull(), "bad order_date"),
        (F.col("order_date_parsed") > F.current_date(), "future order_date"),
        (
            F.col("quantity_parsed").isNull()
            | (F.col("quantity_parsed") < 1)
            | (F.col("quantity_parsed") > 100),
            "bad quantity",
        ),
        (F.col("unit_price_parsed").isNull() | (F.col("unit_price_parsed") <= 0), "bad unit_price"),
        (
            F.col("payment_method").isNotNull() & ~F.col("payment_method").isin(PAYMENT_METHODS),
            "bad payment_method",
        ),
        (
            F.col("order_status").isNotNull() & ~F.col("order_status").isin(ORDER_STATUSES),
            "bad order_status",
        ),
    ]
    reasons = F.concat_ws("; ", *[F.when(cond, F.lit(msg)) for cond, msg in checks])
    return df.withColumn("rejection_reason", F.when(reasons != "", reasons))


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "INGESTION_DATE"])
    bucket, ingestion_date = args["BUCKET"], args["INGESTION_DATE"]

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    source = f"s3://{bucket}/raw/sales/ingestion_date={ingestion_date}/"
    raw = spark.read.option("header", True).csv(source)  # all columns as strings
    input_count = raw.count()

    typed = (
        raw.withColumn("order_date_parsed", F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn(
            "quantity_parsed",
            F.when(F.col("quantity").rlike(r"^-?\d+$"), F.col("quantity").cast("int")),
        )
        .withColumn("unit_price_parsed", F.col("unit_price").cast("decimal(12,2)"))
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
    )
    checked = add_rejection_reason(typed)

    rejected = checked.filter(F.col("rejection_reason").isNotNull())
    valid = checked.filter(F.col("rejection_reason").isNull())

    latest_first = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc_nulls_last())
    deduped = (
        valid.withColumn("_rn", F.row_number().over(latest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    processed = deduped.select(
        "order_id",
        "customer_id",
        F.col("order_date_parsed").alias("order_date"),
        F.col("event_ts").alias("event_timestamp"),
        F.trim("product").alias("product"),
        F.initcap(F.trim("city")).alias("city"),
        F.col("quantity_parsed").alias("quantity"),
        F.col("unit_price_parsed").alias("unit_price"),
        (F.col("quantity_parsed") * F.col("unit_price_parsed"))
        .cast("decimal(14,2)")
        .alias("revenue"),
        "payment_method",
        "order_status",
        F.lit(ingestion_date).cast("date").alias("ingestion_date"),
        F.year("order_date_parsed").alias("year"),
        F.format_string("%02d", F.month("order_date_parsed")).alias("month"),
    )

    processed.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{bucket}/processed/sales/"
    )
    rejected.select(*raw.columns, "rejection_reason").coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(f"s3://{bucket}/rejected/sales/ingestion_date={ingestion_date}/")

    valid_count = valid.count()
    output_count = processed.count()
    report = {
        "ingestion_date": ingestion_date,
        "input_rows": input_count,
        "rejected_rows": rejected.count(),
        "valid_rows": valid_count,
        "duplicates_removed": valid_count - output_count,
        "output_rows": output_count,
    }
    # Reconciliation: input = rejected + duplicates_removed + output
    report["reconciled"] = input_count == (
        report["rejected_rows"] + report["duplicates_removed"] + output_count
    )
    print(json.dumps(report))
    spark.createDataFrame([report]).coalesce(1).write.mode("overwrite").json(
        f"s3://{bucket}/reports/sales/ingestion_date={ingestion_date}/"
    )

    if not report["reconciled"]:
        raise RuntimeError(f"Row counts do not reconcile: {report}")

    job.commit()


if __name__ == "__main__":
    main()
