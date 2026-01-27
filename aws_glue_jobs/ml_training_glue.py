import sys
import json
import boto3
from typing import Tuple

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    nullif,
    lit,
    year,
    current_date,
    log1p,
)

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
)
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

from datetime import datetime

def init_glue():

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "GOLDEN_CORPORATE_REGISTRY",
            "OUTPUT_DATASET_PATH",
            "MODEL_OUTPUT_PATH",
            "MODEL_REPORT_PATH"
        ],
    )

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    return spark, job, args


def engineer_features(df: DataFrame) -> DataFrame:

    df = df.withColumn(
        "industry_std",
        nullif(
            coalesce(
                nullif(col("tech_industry_std"), lit("N/A")),
                col("reg_industry_std"),
            ),
            lit("N/A"),
        ),
    )

    df = df.withColumn(
        "company_age",
        year(current_date()) - col("reg_year_founded"),
    )

    df = df.withColumn(
        "log_company_age",
        log1p(col("company_age")),
    )

    df = df.withColumn(
        "log_reg_total_employee_estimate",
        log1p(col("reg_total_employee_estimate")),
    )

    df = df.withColumn(
        "log_tech_estimated_revenue",
        log1p(col("tech_estimated_revenue")),
    )

    return df


def build_pipeline() -> Pipeline:

    industry_indexer = StringIndexer(
        inputCol="industry_std",
        outputCol="industry_idx",
        handleInvalid="keep",
    )

    industry_encoder = OneHotEncoder(
        inputCols=["industry_idx"],
        outputCols=["industry_ohe"],
        dropLast=True,
    )

    assembler = VectorAssembler(
        inputCols=[
            "log_reg_total_employee_estimate",
            "log_company_age",
            "industry_ohe",
        ],
        outputCol="features",
    )

    lr = LinearRegression(
        labelCol="log_tech_estimated_revenue",
        featuresCol="features",
        maxIter=200,
        regParam=0.1,
        elasticNetParam=0.5,
    )

    return Pipeline(
        stages=[
            industry_indexer,
            industry_encoder,
            assembler,
            lr,
        ]
    )



def train_model(
    df: DataFrame,
    pipeline: Pipeline,
):

    clean_df = df.dropna(
        subset=[
            "industry_std",
            "log_reg_total_employee_estimate",
            "log_company_age",
            "log_tech_estimated_revenue",
        ]
    )

    train_df, test_df = clean_df.randomSplit(
        [0.8, 0.2],
        seed=42,
    )

    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)

    evaluator = RegressionEvaluator(
        labelCol="log_tech_estimated_revenue",
        predictionCol="prediction",
        metricName="mae",
    )

    mae = evaluator.evaluate(predictions)

    return model, mae, train_df.count(), test_df.count()


def get_run_date():
    return datetime.utcnow().strftime("%Y%m%d")

def add_date_partition(base_path: str, run_date: str) -> str:

    if not base_path.startswith("s3://"):
        raise ValueError("Path must be S3")

    return base_path.rstrip("/") + f"/{run_date}/"

def save_model(model, path: str, run_date: str):

    final_path = add_date_partition(path, run_date)

    model.write().overwrite().save(final_path)


def save_dataset(df: DataFrame, path: str, run_date: str):

    final_path = add_date_partition(path, run_date)

    df.write.mode("overwrite").parquet(final_path)


def save_report(report: dict, path: str, run_date: str):

    if not path.startswith("s3://"):
        raise ValueError("MODEL_REPORT_PATH must be an S3 path")

    s3 = boto3.client("s3")

    # Remove s3://
    path = path.replace("s3://", "").rstrip("/")

    bucket, prefix = path.split("/", 1)


    final_key = f"{prefix}/{run_date}/report.json"

    try:
        s3.put_object(
            Bucket=bucket,
            Key=final_key,
            Body=json.dumps(report, indent=2),
            ContentType="application/json",
        )

        print(f"Report saved to: s3://{bucket}/{final_key}")

    except Exception as e:
        raise RuntimeError(f"Failed to upload report: {e}")


def main():

    spark, job, args = init_glue()

   
    joined_df = spark.table(
       args["GOLDEN_CORPORATE_REGISTRY"],
    )

    features_df = engineer_features(joined_df)

    pipeline = build_pipeline()

    model, mae, train_rows, test_rows = train_model(
        features_df,
        pipeline,
    )

    run_date = get_run_date()

    save_model(
        model,
        args["MODEL_OUTPUT_PATH"],
        run_date,
    )

    save_dataset(
        features_df,
        args["OUTPUT_DATASET_PATH"],
        run_date
    )

     # Build report
    report = {
        "model": "LinearRegression",
        "train_rows": train_rows,
        "test_rows": test_rows,
        "metrics": {
         "mae": mae
  }
    }

    # Save report
    save_report(
        report,
        args["MODEL_REPORT_PATH"],
        run_date
    )

    print(json.dumps(report, indent=2))

    job.commit()


if __name__ == "__main__":
    main()

