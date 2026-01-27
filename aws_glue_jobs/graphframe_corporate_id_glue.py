import sys
from typing import Tuple

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min as spark_min

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from graphframes import GraphFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,

)

def init_glue():

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "SILVER_COMPANIES_PATH",
            "SILVER_TECH_PATH",
            "FUZZY_MATCHES_PATH",
            "GOLDEN_CORPORATE_REGISTRY",
            "CHECKPOINT_PATH",
        ],
    )

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    return spark, job, args


def read_inputs(
    spark,
    companies_path: str,
    tech_path: str,
    fuzzy_path: str,
) -> Tuple[DataFrame, DataFrame, DataFrame]:

    companies_df = spark.read.parquet(companies_path)
    tech_df = spark.read.parquet(tech_path)
    fuzzy_df = spark.read.parquet(fuzzy_path)

    return companies_df, tech_df, fuzzy_df


def build_vertices(
    companies_df: DataFrame,
    tech_df: DataFrame,
) -> DataFrame:

    return (
        companies_df
        .select(col("record_id").alias("id"))
        .union(
            tech_df.select(col("record_id").alias("id"))
        )
        .distinct()
    )


def build_edges(fuzzy_df: DataFrame) -> DataFrame:

    return fuzzy_df.select(
        col("left_id").alias("src"),
        col("right_id").alias("dst"),
    )


def build_graph(
    spark,
    vertices: DataFrame,
    edges: DataFrame,
    checkpoint_path: str,
) -> GraphFrame:

    spark.sparkContext.setCheckpointDir(checkpoint_path)

    return GraphFrame(vertices, edges)



def generate_corporate_ids(graph: GraphFrame) -> DataFrame:

    components = graph.connectedComponents()

    corporate_ids = (
        components
        .groupBy("component")
        .agg(spark_min("id").alias("corporate_id"))
    )

    record_map = (
        components
        .join(corporate_ids, on="component")
        .select(
            col("id").alias("record_id"),
            col("corporate_id"),
        )
    )

    return record_map


def attach_and_write(
    base_df: DataFrame,
    mapping_df: DataFrame,
    output_path: str,
):

    result = (
        base_df
        .join(mapping_df, on="record_id", how="left")
    )

    (
        result.write
        .mode("overwrite")
        .parquet(output_path)
    )

def join_sources(
    silver_tech_with_corp_id_df,
    silver_companies_with_corp_id_df
):

    joined_df = (
        silver_tech_with_corp_id_df.alias("tech")
        .join(
            silver_companies_with_corp_id_df.alias("reg"),
            col("tech.corporate_id") == col("reg.corporate_id"),
            "left"
        )
        .select(
            col("tech.corporate_id").alias("corporate_id"),

            col("tech.record_id").alias("tech_record_id"),
            col("tech.source_system").alias("tech_source_system"),
            col("tech.company").alias("tech_company"),
            col("tech.company_name_normalised").alias("tech_company_name_normalised"),
            col("tech.industry_std").alias("tech_industry_std"),
            col("tech.country_std").alias("tech_country_std"),
            col("tech.market_cap_usd").alias("tech_market_cap_usd"),
            col("tech.estimated_revenue").alias("tech_estimated_revenue"),
            col("tech.processing_ts").alias("tech_processing_ts"),

            col("reg.record_id").alias("reg_record_id"),
            col("reg.source_system").alias("reg_source_system"),
            col("reg.company").alias("reg_company"),
            col("reg.company_name_normalised").alias("reg_company_name_normalised"),
            col("reg.domain").alias("reg_domain"),
            col("reg.industry_std").alias("reg_industry_std"),
            col("reg.country_std").alias("reg_country_std"),
            col("reg.year_founded").alias("reg_year_founded"),
            col("reg.current_employee_estimate").alias("reg_current_employee_estimate"),
            col("reg.total_employee_estimate").alias("reg_total_employee_estimate"),
            col("reg.processing_ts").alias("reg_processing_ts")
        )
    )

    joined_df = joined_df.withColumn(
        "processing_ts",
        current_timestamp()
    )

    return joined_df



def write_golden_corporate_registry(joined_df: DataFrame, path: str):

    result = joined_df.select(
        "corporate_id",

        "tech_record_id",
        "tech_source_system",
        "tech_company",
        "tech_company_name_normalised",
        "tech_industry_std",
        "tech_country_std",
        "tech_market_cap_usd",
        "tech_estimated_revenue",
        "tech_processing_ts",

        "reg_record_id",
        "reg_source_system",
        "reg_company",
        "reg_company_name_normalised",
        "reg_domain",
        "reg_industry_std",
        "reg_country_std",
        "reg_year_founded",
        "reg_current_employee_estimate",
        "reg_total_employee_estimate",
        "reg_processing_ts",

        "processing_ts",
    )

    result.write.mode("overwrite").parquet(path)
def main():

    spark, job, args = init_glue()

    companies_df, tech_df, fuzzy_df = read_inputs(
        spark,
        args["SILVER_COMPANIES_PATH"],
        args["SILVER_TECH_PATH"],
        args["FUZZY_MATCHES_PATH"],
    )

    vertices = build_vertices(
        companies_df,
        tech_df,
    )

    edges = build_edges(fuzzy_df)

    graph = build_graph(
        spark,
        vertices,
        edges,
        args["CHECKPOINT_PATH"],
    )

    record_to_corporate = generate_corporate_ids(graph)

    companies_df = (
    companies_df
    .join(record_to_corporate, on="record_id", how="left")
)

    tech_df = (
    tech_df
    .join(record_to_corporate, on="record_id", how="left")
)

    joined_df = join_sources(tech_df, companies_df)

    write_golden_corporate_registry(
    joined_df,
    args["GOLDEN_CORPORATE_REGISTRY"],
   )

    job.commit()


if __name__ == "__main__":
    main()

