import sys
import re
from typing import List, Dict

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    lit,
    when,
    regexp_replace,
    sha2,
    concat_ws,
    coalesce,
    levenshtein,
    substring,
    current_timestamp,
    udf,
)
from pyspark.sql.types import DoubleType, StringType

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

def init_glue():

    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME",
            "BRONZE_COMPANIES_PATH",
            "BRONZE_TECH_PATH",
            "FUZZY_OUTPUT_PATH",
            "SILVER_COMPANIES_PATH",
            "SILVER_TECH_PATH"
        ],
    )

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    return spark, job, args


SUFFIX_REGEX = re.compile(
    r"\b("
    r"ltd|limited|"
    r"inc|incorporated|"
    r"llc|"
    r"corp|corporation|"
    r"co|company|"
    r"plc|"
    r"gmbh|sarl|sa|nv|bv|pte|pty|"
    r"ag|kg|kgaa|"
    r"holdings?|group"
    r")\b",
    re.IGNORECASE,
)


def normalize_company_name_str(name: str) -> str:

    if name is None:
        return ""

    name = name.lower()

    name = re.sub(r"[^\w\s]", " ", name)

    name = SUFFIX_REGEX.sub(" ", name)

    name = re.sub(r"\s+", " ", name)

    return name.strip()


def lowercase_and_trim_string_columns(df: DataFrame) -> DataFrame:

    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(field.name, trim(lower(col(field.name))))

    return df


def add_record_id(
    df: DataFrame,
    source_system: str,
    id_columns: List[str],
) -> DataFrame:

    canonical_cols = [
        trim(lower(coalesce(col(c), lit("")))) for c in id_columns
    ]

    return df.withColumn(
        "record_id",
        sha2(
            concat_ws("||", lit(source_system), *canonical_cols),
            256,
        ),
    )


def normalize_market_cap(
    df: DataFrame,
    col_name: str = "market_cap",
) -> DataFrame:

    return df.withColumn(
        "market_cap_usd",
        when(
            col(col_name).endswith("t"),
            regexp_replace(col(col_name), "[^0-9.]", "").cast(DoubleType()) * 1e12,
        )
        .when(
            col(col_name).endswith("b"),
            regexp_replace(col(col_name), "[^0-9.]", "").cast(DoubleType()) * 1e9,
        )
        .when(
            col(col_name).endswith("m"),
            regexp_replace(col(col_name), "[^0-9.]", "").cast(DoubleType()) * 1e6,
        )
        .otherwise(
            regexp_replace(col(col_name), "[^0-9.]", "").cast(DoubleType())
        ),
    )


def normalize_company_name(
    df: DataFrame,
    input_col: str,
    output_col: str,
) -> DataFrame:

    normalize_udf = udf(
        normalize_company_name_str,
        StringType(),
    )

    return df.withColumn(
        output_col,
        normalize_udf(col(input_col))
    )


def standardize_industry(
    df: DataFrame,
    mapping: Dict[str, str],
) -> DataFrame:

    expr = F.create_map(
        *[F.lit(x) for x in sum(mapping.items(), ())]
    )

    return df.withColumn(
        "industry_std",
        F.coalesce(expr[col("industry")], lit("N/A")),
    )


def standardize_country(
    df: DataFrame,
    valid: List[str],
) -> DataFrame:

    return df.withColumn(
        "country_std",
        when(col("country").isin(valid), col("country"))
        .otherwise(lit("N/A")),
    )


def add_blocking_key(df: DataFrame) -> DataFrame:

    return df.withColumn(
        "blocking_key",
        substring(col("company_name_normalised"), 1, 3),
    )


def fuzzy_match_batches(
    df_left: DataFrame,
    df_right: DataFrame,
    output_path: str,
    batch_size: int = 100_000,
    threshold: int = 3,
    min_confidence: float = 0.80,
):

    total = df_right.count()
    batches = (total // batch_size) + 1

    df_right_indexed = (
        df_right.rdd
        .zipWithIndex()
        .toDF()
        .selectExpr("_1.*", "_2 as row_idx")
    )

    for i in range(batches):

        start = i * batch_size
        end = start + batch_size

        df_batch = (
            df_right_indexed
            .filter((col("row_idx") >= start) & (col("row_idx") < end))
            .drop("row_idx")
        )

        joined = (
            F.broadcast(df_left).alias("l")
            .join(
                df_batch.alias("r"),
                on="blocking_key",
                how="inner",
            )
            .where(col("l.record_id") < col("r.record_id"))
        )

        matches = (
            joined
            .withColumn(
                "name_distance",
                levenshtein(
                    col("l.company_name_normalised"),
                    col("r.company_name_normalised"),
                ),
            )
            .filter(col("name_distance") <= threshold)
            .withColumn(
                "match_confidence",
                lit(1.0) - (col("name_distance") / lit(threshold)),
            )
            .filter(col("match_confidence") >= min_confidence)
            .select(
                col("l.source_system").alias("left_source_system"),
                col("r.source_system").alias("right_source_system"),
                col("l.record_id").alias("left_id"),
                col("r.record_id").alias("right_id"),
                col("name_distance"),
                col("match_confidence"),
                lit("fuzzy").alias("match_type"),
            )
        )

        matches.write.mode("append").parquet(output_path)


def write_silver_tech(df: DataFrame, path: str):

    result = (
        df.select(
            "record_id",
            "source_system",
            "company",
            "company_name_normalised",
            "industry_std",
            "country_std",
            "market_cap_usd",
            "estimated_revenue",
        )
        .withColumn("processing_ts", current_timestamp())
    )

    result.write.mode("overwrite").parquet(path)


def write_silver_companies(df: DataFrame, path: str):

    result = (
        df.select(
            "record_id",
            "source_system",
            "company",
            "company_name_normalised",
            "domain",
            "industry_std",
            "country_std",
            "year_founded",
            "current_employee_estimate",
            "total_employee_estimate",
        )
        .withColumn("processing_ts", current_timestamp())
    )

    result.write.mode("overwrite").parquet(path)




def main():

    spark, job, args = init_glue()

    companies_df = spark.read.parquet(
        args["BRONZE_COMPANIES_PATH"]
    )

    tech_df = spark.read.parquet(
        args["BRONZE_TECH_PATH"]
    )

    companies_df = lowercase_and_trim_string_columns(companies_df)
    tech_df = lowercase_and_trim_string_columns(tech_df)

    companies_df = companies_df.withColumnRenamed("name", "company")

    companies_df = add_record_id(
        companies_df,
        source_system="company_registry",
        id_columns=["company", "country", "industry"],
    )

    tech_df = add_record_id(
        tech_df,
        source_system="tech_companies",
        id_columns=["company", "country", "industry"],
    )

    tech_df = normalize_market_cap(tech_df)

    tech_df = tech_df.withColumn(
        "estimated_revenue",
        when(col("market_cap_usd").isNotNull(), col("market_cap_usd") / 8),
    )

    companies_df = normalize_company_name(
        companies_df,
        "company",
        "company_name_normalised",
    )

    tech_df = normalize_company_name(
        tech_df,
        "company",
        "company_name_normalised",
    )

    industry_mapping = {
        "information technology and services": "information technology services",
        "retail": "electronics & computer distribution",
        "computer software": "software—application",
        "telecommunications": "communication equipment",
        "internet": "software—infrastructure",
    }

    companies_df = standardize_industry(
        companies_df,
        industry_mapping,
    )

    tech_df = tech_df.withColumnRenamed("industry", "industry_std")

    valid_countries = [
        "united states",
        "taiwan",
        "south korea",
        "netherlands",
        "ireland",
        "germany",
        "japan",
        "canada",
        "india",
        "france",
        "australia",
        "china",
        "switzerland",
        "united kingdom",
    ]

    companies_df = standardize_country(companies_df, valid_countries)
    tech_df = standardize_country(tech_df, valid_countries)

    companies_df = add_blocking_key(companies_df)
    tech_df = add_blocking_key(tech_df)

    companies_df = companies_df.withColumn(
        "source_system", lit("company_registry")
    )

    tech_df = tech_df.withColumn(
        "source_system", lit("tech_companies")
    )

    fuzzy_match_batches(
        tech_df,
        companies_df,
        args["FUZZY_OUTPUT_PATH"],
    )

    write_silver_tech(
        tech_df,
        args["SILVER_TECH_PATH"],
    )

    write_silver_companies(
        companies_df,
        args["SILVER_COMPANIES_PATH"],
    )

    job.commit()


if __name__ == "__main__":
    main()

