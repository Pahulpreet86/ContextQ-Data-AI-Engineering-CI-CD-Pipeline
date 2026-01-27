import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

REQUIRED_ARGS = [
    "JOB_NAME",
    "CATALOG_NAME",
    "DATA_LAKE_BUCKET",
    "DATABASE",
    "WAREHOUSE_PATH",
    "STAGING_PREFIX",
    "TABLE"
]


def get_job_args():

    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

    return {
        "JOB_NAME": args["JOB_NAME"],
        "CATALOG_NAME": args["CATALOG_NAME"],
        "DATA_LAKE_BUCKET": args["DATA_LAKE_BUCKET"],
        "DATABASE": args["DATABASE"],
        "WAREHOUSE_PATH": args["WAREHOUSE_PATH"],
        "STAGING_PREFIX": args["STAGING_PREFIX"],
        "TABLE":args["TABLE"],
    }


def init_glue(job_name, args):

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(job_name, args)

    return spark, job


def configure_iceberg(spark, catalog_name, warehouse_path):

    spark.conf.set(
        f"spark.sql.catalog.{catalog_name}",
        "org.apache.iceberg.spark.SparkCatalog",
    )

    spark.conf.set(
        f"spark.sql.catalog.{catalog_name}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )

    spark.conf.set(
        f"spark.sql.catalog.{catalog_name}.warehouse",
        warehouse_path,
    )

    spark.conf.set(
        f"spark.sql.catalog.{catalog_name}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )

    spark.conf.set(
        "spark.sql.defaultCatalog",
        catalog_name,
    )


def read_staging_data(spark, bucket, prefix):

    path = f"s3://{bucket}/{prefix}/"

    return spark.read.parquet(path)


def validate_schema(source_df, target_df, strict=True):

    source_cols = {f.name: f.dataType for f in source_df.schema}
    target_cols = {f.name: f.dataType for f in target_df.schema}

    missing = set(target_cols) - set(source_cols)
    extra = set(source_cols) - set(target_cols)

    errors = []

    if missing:
        errors.append(f"Missing: {missing}")

    if extra:
        errors.append(f"Extra: {extra}")

    if strict:
        mismatches = []

        for col in source_cols.keys() & target_cols.keys():
            if source_cols[col] != target_cols[col]:
                mismatches.append(col)

        if mismatches:
            errors.append(f"Type mismatch: {mismatches}")

    if errors:
        raise ValueError("Schema check failed: " + " | ".join(errors))

    return True


def prepare_views(spark, catalog, database, df):

    spark.sql(f"USE {catalog}.{database}")

    df.createOrReplaceTempView("staging_corporate_registry")


def run_upsert(spark, catalog, database, table):
    
    _table = f"{catalog}.{database}.{table}"

    merge_sql = f"""
    MERGE INTO {_table} AS target
    USING staging_corporate_registry AS source
    ON target.corporate_id = source.corporate_id

    WHEN MATCHED THEN UPDATE SET

        target.tech_record_id               = source.tech_record_id,
        target.tech_source_system           = source.tech_source_system,
        target.tech_company                 = source.tech_company,
        target.tech_company_name_normalised = source.tech_company_name_normalised,
        target.tech_industry_std            = source.tech_industry_std,
        target.tech_country_std             = source.tech_country_std,
        target.tech_market_cap_usd          = source.tech_market_cap_usd,
        target.tech_estimated_revenue       = source.tech_estimated_revenue,
        target.tech_processing_ts           = source.tech_processing_ts,

        target.reg_record_id                = source.reg_record_id,
        target.reg_source_system            = source.reg_source_system,
        target.reg_company                  = source.reg_company,
        target.reg_company_name_normalised  = source.reg_company_name_normalised,
        target.reg_domain                   = source.reg_domain,
        target.reg_industry_std             = source.reg_industry_std,
        target.reg_country_std              = source.reg_country_std,
        target.reg_year_founded             = source.reg_year_founded,
        target.reg_current_employee_estimate= source.reg_current_employee_estimate,
        target.reg_total_employee_estimate  = source.reg_total_employee_estimate,
        target.reg_processing_ts            = source.reg_processing_ts,

        target.processing_ts                = source.processing_ts

    WHEN NOT MATCHED THEN INSERT (

        corporate_id,

        tech_record_id,
        tech_source_system,
        tech_company,
        tech_company_name_normalised,
        tech_industry_std,
        tech_country_std,
        tech_market_cap_usd,
        tech_estimated_revenue,
        tech_processing_ts,

        reg_record_id,
        reg_source_system,
        reg_company,
        reg_company_name_normalised,
        reg_domain,
        reg_industry_std,
        reg_country_std,
        reg_year_founded,
        reg_current_employee_estimate,
        reg_total_employee_estimate,
        reg_processing_ts,

        processing_ts
    )

    VALUES (

        source.corporate_id,

        source.tech_record_id,
        source.tech_source_system,
        source.tech_company,
        source.tech_company_name_normalised,
        source.tech_industry_std,
        source.tech_country_std,
        source.tech_market_cap_usd,
        source.tech_estimated_revenue,
        source.tech_processing_ts,

        source.reg_record_id,
        source.reg_source_system,
        source.reg_company,
        source.reg_company_name_normalised,
        source.reg_domain,
        source.reg_industry_std,
        source.reg_country_std,
        source.reg_year_founded,
        source.reg_current_employee_estimate,
        source.reg_total_employee_estimate,
        source.reg_processing_ts,

        source.processing_ts
    )
    """

    spark.sql(merge_sql)



def main():


    args = get_job_args()

  
    spark, job = init_glue(
        args["JOB_NAME"],
        args,
    )


    configure_iceberg(
        spark,
        args["CATALOG_NAME"],
        args["WAREHOUSE_PATH"],
    )


    staging_df = read_staging_data(
        spark,
        args["DATA_LAKE_BUCKET"],
        args["STAGING_PREFIX"],
    )


    target_df = spark.table(
    f"{args['CATALOG_NAME']}.{args['DATABASE']}.{args['TABLE']}"
)

    #Data Contract Check
    validate_schema(staging_df, target_df)


    prepare_views(
        spark,
        args["CATALOG_NAME"],
        args["DATABASE"],
        staging_df,
    )


    run_upsert(
        spark,
        args["CATALOG_NAME"],
        args["DATABASE"],
        args["TABLE"],
    )

    job.commit()


if __name__ == "__main__":
    main()
