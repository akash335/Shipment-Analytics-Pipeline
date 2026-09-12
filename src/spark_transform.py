from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    datediff,
    lag,
    lit,
    month,
    row_number,
    sum,
    to_date,
    year,
    when,
)


DB_URL = "jdbc:postgresql://localhost:5432/shipment_db"

DB_PROPERTIES = {
    "user": "akash",
    "password": "",
    "driver": "org.postgresql.Driver",
}


OUTPUT_PATH = "data/processed/shipments"


spark = (
    SparkSession.builder
    .appName("ShipmentAnalyticsPipeline")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3",
    )
    .config(
        "spark.sql.adaptive.enabled",
        "true",
    )
    .config(
        "spark.sql.shuffle.partitions",
        "8",
    )
    .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")


print("\n========== SHIPMENT ETL START ==========\n")


# ==========================================================
# 1. READ SHIPMENTS FROM POSTGRESQL
# ==========================================================

shipments = (
    spark.read
    .format("jdbc")
    .option("url", DB_URL)
    .option(
        "dbtable",
        "shipments",
    )
    .option("user", DB_PROPERTIES["user"])
    .option("password", DB_PROPERTIES["password"])
    .option("driver", DB_PROPERTIES["driver"])
    .option("partitionColumn", "shipment_date")
    .option("lowerBound", "2026-01-01")
    .option("upperBound", "2026-07-01")
    .option("numPartitions", "8")
    .load()
)


print(
    f"Shipments loaded: {shipments.count():,}"
)


# ==========================================================
# 2. READ DIMENSION TABLES
# ==========================================================

customers = (
    spark.read
    .format("jdbc")
    .option("url", DB_URL)
    .option("dbtable", "customers")
    .option("user", DB_PROPERTIES["user"])
    .option("password", DB_PROPERTIES["password"])
    .option("driver", DB_PROPERTIES["driver"])
    .load()
)


warehouses = (
    spark.read
    .format("jdbc")
    .option("url", DB_URL)
    .option("dbtable", "warehouses")
    .option("user", DB_PROPERTIES["user"])
    .option("password", DB_PROPERTIES["password"])
    .option("driver", DB_PROPERTIES["driver"])
    .load()
)


carriers = (
    spark.read
    .format("jdbc")
    .option("url", DB_URL)
    .option("dbtable", "carriers")
    .option("user", DB_PROPERTIES["user"])
    .option("password", DB_PROPERTIES["password"])
    .option("driver", DB_PROPERTIES["driver"])
    .load()
)


print(
    f"Customers loaded: {customers.count():,}"
)

print(
    f"Warehouses loaded: {warehouses.count():,}"
)

print(
    f"Carriers loaded: {carriers.count():,}"
)


# ==========================================================
# 3. DATA CLEANING
# ==========================================================

clean = (
    shipments

    .dropDuplicates(
        ["shipment_id"]
    )

    .withColumn(
        "shipment_date",
        to_date("shipment_date"),
    )

    .withColumn(
        "expected_delivery_date",
        to_date("expected_delivery_date"),
    )

    .withColumn(
        "actual_delivery_date",
        to_date("actual_delivery_date"),
    )

    .filter(
        col("shipment_id").isNotNull()
    )

    .filter(
        col("customer_id").isNotNull()
    )

    .filter(
        col("warehouse_id").isNotNull()
    )

    .filter(
        col("package_weight_kg") > 0
    )

    .filter(
        col("shipping_cost") >= 0
    )
)


# ==========================================================
# 4. BROADCAST DIMENSION JOINS
# ==========================================================

enriched = (
    clean

    .join(
        broadcast(
            customers.select(
                "customer_id",
                col("customer_name").alias(
                    "customer_name"
                ),
                col("region").alias(
                    "customer_region"
                ),
            )
        ),
        "customer_id",
        "left",
    )

    .join(
        broadcast(
            warehouses.select(
                "warehouse_id",
                col("warehouse_name").alias(
                    "warehouse_name"
                ),
                col("region").alias(
                    "warehouse_region"
                ),
                "capacity",
            )
        ),
        "warehouse_id",
        "left",
    )

    .join(
        broadcast(
            carriers.select(
                "carrier_id",
                "carrier_name",
                "service_level",
            )
        ),
        "carrier_id",
        "left",
    )
)


# ==========================================================
# 5. DERIVED METRICS
# ==========================================================

transformed = (
    enriched

    .withColumn(
        "delivery_days",
        when(
            col("actual_delivery_date").isNotNull(),
            datediff(
                "actual_delivery_date",
                "shipment_date",
            ),
        ),
    )

    .withColumn(
        "delay_days",
        when(
            col("actual_delivery_date").isNotNull(),
            datediff(
                "actual_delivery_date",
                "expected_delivery_date",
            ),
        ),
    )

    .withColumn(
        "on_time_flag",
        when(
            col("actual_delivery_date").isNull(),
            lit(None),
        )
        .when(
            col("actual_delivery_date")
            <= col("expected_delivery_date"),
            1,
        )
        .otherwise(0),
    )

    .withColumn(
        "shipping_cost_per_kg",
        when(
            col("package_weight_kg") > 0,
            col("shipping_cost")
            / col("package_weight_kg"),
        ),
    )

    .withColumn(
        "shipment_year",
        year("shipment_date"),
    )

    .withColumn(
        "shipment_month",
        month("shipment_date"),
    )
)


# ==========================================================
# 6. WINDOW FUNCTIONS
# ==========================================================

customer_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(
        col("shipment_date"),
        col("shipment_id"),
    )
)


with_windows = (
    transformed

    .withColumn(
        "customer_shipment_number",
        row_number().over(
            customer_window
        ),
    )

    .withColumn(
        "previous_shipment_date",
        lag("shipment_date").over(
            customer_window
        ),
    )
)


# ==========================================================
# 7. DATA QUALITY CHECKS
# ==========================================================

source_count = shipments.count()

processed_count = with_windows.count()

invalid_customer_count = (
    with_windows
    .filter(
        col("customer_name").isNull()
    )
    .count()
)

invalid_warehouse_count = (
    with_windows
    .filter(
        col("warehouse_name").isNull()
    )
    .count()
)

invalid_carrier_count = (
    with_windows
    .filter(
        col("carrier_name").isNull()
    )
    .count()
)


print("\n========== DATA QUALITY ==========")

print(
    f"Source records:      {source_count:,}"
)

print(
    f"Processed records:   {processed_count:,}"
)

print(
    f"Missing customers:   {invalid_customer_count:,}"
)

print(
    f"Missing warehouses:  {invalid_warehouse_count:,}"
)

print(
    f"Missing carriers:    {invalid_carrier_count:,}"
)


if processed_count != source_count:
    raise RuntimeError(
        "Record reconciliation failed."
    )


if (
    invalid_customer_count > 0
    or invalid_warehouse_count > 0
    or invalid_carrier_count > 0
):
    raise RuntimeError(
        "Referential integrity validation failed."
    )


print(
    "✅ Data quality checks passed."
)


# ==========================================================
# 8. WRITE PARTITIONED PARQUET
# ==========================================================

(
    with_windows

    .repartition(
        "destination_region",
        "shipment_year",
        "shipment_month",
    )

    .write
    .mode("overwrite")
    .partitionBy(
        "destination_region",
        "shipment_year",
        "shipment_month",
    )
    .parquet(
        OUTPUT_PATH
    )
)


print(
    f"\n✅ Parquet written to: {OUTPUT_PATH}"
)


# ==========================================================
# 9. ANALYTICS PREVIEW
# ==========================================================

print("\n========== TOP CARRIERS ==========")

(
    with_windows
    .groupBy("carrier_name")
    .agg(
        sum("shipping_cost").alias(
            "total_shipping_cost"
        ),
        avg("delay_days").alias(
            "avg_delay_days"
        ),
    )
    .orderBy(
        col("total_shipping_cost").desc()
    )
    .show(
        10,
        truncate=False,
    )
)


print("\n========== REGIONAL PERFORMANCE ==========")

(
    with_windows
    .groupBy(
        "destination_region"
    )
    .agg(
        sum("shipping_cost").alias(
            "shipping_cost"
        ),
        avg("delay_days").alias(
            "avg_delay_days"
        ),
        avg("on_time_flag").alias(
            "on_time_rate"
        ),
    )
    .orderBy(
        "destination_region"
    )
    .show(
        20,
        truncate=False,
    )
)


print(
    "\n========== ETL COMPLETE ==========\n"
)


spark.stop()
