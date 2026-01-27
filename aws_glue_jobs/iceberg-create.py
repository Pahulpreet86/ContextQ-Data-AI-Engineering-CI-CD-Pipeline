import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


REQUIRED_ARGS = [
    "JOB_NAME",
    "CATALOG_NAME",
    "DATABASE",
    "WAREHOUSE_PATH",
    "TABLE",
    "ICEBERG_LOCATION",
]


def init_glue():
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    return spark, job, args


def configure_iceberg(spark, args):

    catalog = args["CATALOG_NAME"]
    warehouse = args["WAREHOUSE_PATH"]

    spark.conf.set(
        f"spark.sql.catalog.{catalog}",
        "org.apache.iceberg.spark.SparkCatalog",
    )

    spark.conf.set(
        f"spark.sql.catalog.{catalog}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )

    spark.conf.set(
        f"spark.sql.catalog.{catalog}.warehouse",
        warehouse,
    )

    spark.conf.set(
        f"spark.sql.catalog.{catalog}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )

    spark.conf.set(
        "spark.sql.defaultCatalog",
        catalog,
    )


def show_environment(spark):
    print(spark.sparkContext.getConf().getAll())

    spark.sql("SHOW CATALOGS").show()
    spark.sql("SHOW DATABASES").show()

    print(spark.conf.get("spark.sql.extensions", "NOT_SET"))


def create_corporate_registry_table(spark, args):

    table=args["TABLE"]
    database = args["DATABASE"]
    location = args["ICEBERG_LOCATION"]

    create_sql = f"""
    CREATE TABLE {database}.{table} (
        corporate_id STRING,
        tech_record_id STRING,
        tech_source_system STRING,
        tech_company STRING,
        tech_company_name_normalised STRING,
        tech_industry_std STRING,
        tech_country_std STRING,
        tech_market_cap_usd DOUBLE,
        tech_estimated_revenue DOUBLE,
        tech_processing_ts TIMESTAMP,

        reg_record_id STRING,
        reg_source_system STRING,
        reg_company STRING,
        reg_company_name_normalised STRING,
        reg_domain STRING,
        reg_industry_std STRING,
        reg_country_std STRING,
        reg_year_founded DOUBLE,
        reg_current_employee_estimate BIGINT,
        reg_total_employee_estimate BIGINT,
        reg_processing_ts TIMESTAMP,

        processing_ts TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (
        bucket(32, corporate_id)
    )
    LOCATION '{location}'
    TBLPROPERTIES (
      'format-version'='2',
      'write.format.default'='parquet',
      'write.parquet.compression-codec'='zstd'
    )
    """

    spark.sql(create_sql)


def show_tables(spark, args):

    spark.sql(f"USE {args['CATALOG_NAME']}.{args['DATABASE']}")
    print(spark.catalog.listTables())


def main():

    spark, job, args = init_glue()

    configure_iceberg(spark, args)

    show_environment(spark)

    create_corporate_registry_table(spark, args)

    show_tables(spark, args)

    job.commit()


if __name__ == "__main__":
    main()
