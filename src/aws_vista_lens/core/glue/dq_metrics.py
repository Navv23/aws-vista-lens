import pyspark.sql.functions as F
import boto3
import json
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import NumericType, StringType, DecimalType
from datetime import datetime
from typing import Dict, Any, List
from awsglue.utils import getResolvedOptions


class GenerateMetrics:
    def __init__(self, spark: SparkSession, s3_path: str):
        self.file_path = s3_path
        self.df = spark.read.csv(s3_path, header=True, inferSchema=True).cache()
        
        self.df.count()

        self.columns = self.df.columns
        self.schema = self.df.schema
        self.height = self.df.count()
        self.width = len(self.columns)
        self.report: Dict[str, Any] = {}

    def _get_bulk_stats(self) -> Dict[str, Any]:
        exprs = []
        
        num_cols = [f.name for f in self.schema.fields if isinstance(f.dataType, (NumericType, DecimalType))]
        txt_cols = [f.name for f in self.schema.fields if isinstance(f.dataType, StringType)]

        for col in num_cols:
            exprs.extend([
                F.sum(F.col(col).isNull().cast("int")).alias(f"num_{col}_null"),
                F.round(F.mean(col), 1).alias(f"num_{col}_mean"),
                F.round(F.stddev(col), 1).alias(f"num_{col}_std"),
                F.round(F.min(col), 1).alias(f"num_{col}_min"),
                F.round(F.max(col), 1).alias(f"num_{col}_max"),
                F.round(F.percentile_approx(col, 0.5), 1).alias(f"num_{col}_median"),
                F.round(F.skewness(col), 1).alias(f"num_{col}_skew"),
                F.round(F.kurtosis(col), 1).alias(f"num_{col}_kurt")
            ])

        for col in txt_cols:
            exprs.extend([
                F.sum(F.col(col).isNull().cast("int")).alias(f"txt_{col}_null"),
                F.countDistinct(col).alias(f"txt_{col}_dist"),
                F.min(F.length(col)).alias(f"txt_{col}_minlen"),
                F.max(F.length(col)).alias(f"txt_{col}_maxlen")
            ])

        for col in self.columns:
            if col not in num_cols and col not in txt_cols:
                exprs.append(F.sum(F.col(col).isNull().cast("int")).alias(f"gen_{col}_null"))

        return self.df.select(exprs).first().asDict()

    def _outlier_analysis(self, df: DataFrame) -> List[Dict[str, Any]]:
        num_cols = [f.name for f in self.schema.fields if isinstance(f.dataType, (NumericType, DecimalType))]
        outlier_data = []

        for col in num_cols:
            quantiles = df.stat.approxQuantile(col, [0.25, 0.75], 0.05)
            if len(quantiles) < 2:
                continue

            q1, q3 = quantiles
            iqr = q3 - q1
            lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr

            outliers_df = df.filter((F.col(col) < lower) | (F.col(col) > upper))
            count = outliers_df.count()

            if count > 0:
                stats = outliers_df.select(
                    F.round(F.min(col), 1).alias("min"),
                    F.round(F.max(col), 1).alias("max")
                ).first()

                outlier_data.append({
                    "column": col,
                    "num_outliers": count,
                    "min_outlier": stats["min"],
                    "max_outlier": stats["max"]
                })

        return outlier_data

    def _get_duplicate_stats(self, df: DataFrame) -> Dict[str, Any]:
        if self.height == 0:
            return {"total_duplicates": 0, "duplicate_percentage": 0}

        distinct_count = df.distinct().count()
        dupe_count = self.height - distinct_count

        return {
            "total_duplicates": dupe_count,
            "duplicate_percentage": round((dupe_count / self.height) * 100, 1)
        }

    def _get_value_counts(self, df: DataFrame) -> Dict[str, List[Dict[str, Any]]]:
        non_num = [f.name for f in self.schema.fields if not isinstance(f.dataType, (NumericType, DecimalType))]
        counts = {}

        for col in non_num:
            top_5 = df.groupBy(col).count().orderBy(F.col("count").desc()).limit(5).collect()
            counts[col] = [row.asDict() for row in top_5]

        return counts

    def generate_report(self) -> Dict[str, Any]:
        bulk_results = self._get_bulk_stats()

        num_cols = [f.name for f in self.schema.fields if isinstance(f.dataType, (NumericType, DecimalType))]
        self.report["numeric_stats"] = [{
            "column": c,
            "null_count": bulk_results[f"num_{c}_null"],
            "mean": bulk_results[f"num_{c}_mean"],
            "stddev": bulk_results[f"num_{c}_std"],
            "min": bulk_results[f"num_{c}_min"],
            "max": bulk_results[f"num_{c}_max"],
            "median": bulk_results[f"num_{c}_median"],
            "skew": bulk_results[f"num_{c}_skew"],
            "kurtosis": bulk_results[f"num_{c}_kurt"]
        } for c in num_cols]

        txt_cols = [f.name for f in self.schema.fields if isinstance(f.dataType, StringType)]
        text_stats = []

        for c in txt_cols:
            mode_val = (
                self.df.groupBy(c).count().orderBy(F.col("count").desc()).first()[0]
                if self.height > 0 else None
            )

            text_stats.append({
                "column": c,
                "null_count": bulk_results[f"txt_{c}_null"],
                "distinct_count": bulk_results[f"txt_{c}_dist"],
                "min_length": bulk_results[f"txt_{c}_minlen"],
                "max_length": bulk_results[f"txt_{c}_maxlen"],
                "most_occurred": mode_val
            })

        self.report["text_stats"] = text_stats

        total_missing = sum(v for k, v in bulk_results.items() if "_null" in k)
        total_cells = self.height * self.width

        self.report["summary"] = {
            "rows": self.height,
            "columns": self.width,
            "duplicates": self._get_duplicate_stats(self.df),
            "missing": {
                "total_missing_cells": total_missing,
                "missing_percentage": round((total_missing / total_cells) * 100, 1) if total_cells > 0 else 0
            }
        }

        self.report["sample"] = [row.asDict() for row in self.df.limit(10).collect()]
        self.report["outliers"] = self._outlier_analysis(self.df)
        self.report["value_counts"] = self._get_value_counts(self.df)

        self.df.unpersist()

        return self.report


def main():
    spark = SparkSession.builder.getOrCreate()
    s3 = boto3.client("s3")

    args = getResolvedOptions(sys.argv, ["s3_path", "run_id"])
    s3_path = args["s3_path"]
    run_id = args["run_id"]

    gm = GenerateMetrics(spark, s3_path)
    report = gm.generate_report()

    file_name = s3_path.split("/")[-1]
    bucket = s3_path.split("/")[2]

    report["job_info"] = {
        "run_id": run_id,
        "file_name": file_name,
        "s3_path": s3_path,
        "processed_at": datetime.now().isoformat()
    }

    key = f"metrics/{run_id}.json"

    s3.put_object(Bucket=bucket,
                  Key=key,
                  Body=json.dumps(report, default=str))

    print(f"Saved: s3://{bucket}/{key}")


if __name__ == "__main__":
    main()