import pandas as pd
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
)
from pyspark.sql.dataframe import DataFrame
import logging
from typing import Final


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PreprocessorTarget:
    def __init__(
        self, target_data_path: str, spark_context: object, sql_context: object
    ) -> None:
        logger.info("Loading target data...")
        # Load target data to a pandas dataframe
        with open(target_data_path, "rb") as f:
            self.target_pd_df = pd.read_excel(f, sheet_name="Sheet1")

        self.spark_context = spark_context
        self.sql_context = sql_context

    def get_spark_df(self) -> DataFrame:
        target_pd_df = self.target_pd_df.astype(
            {
                "carType": "string",
                "color": "string",
                "condition": "string",
                "currency": "string",
                "drive": "string",
                "city": "string",
                "country": "string",
                "make": "string",
                "mileage_unit": "string",
                "model": "string",
                "model_variant": "string",
                "type": "string",
                "zip": "string",
                "fuel_consumption_unit": "string",
                "manufacture_month": "Int64",  # define as int64 to avoid null errors
                "manufacture_year": "Int64",
            }
        )

        final = StructType(fields=self.target_schema)
        target_pd_df["manufacture_month"] = target_pd_df["manufacture_month"].fillna(
            0
        )  # pyspark complaints about <NA> values, fill with 0

        spark_df:Final = self.sql_context.createDataFrame(target_pd_df, schema=final)
        logger.info("Target data loaded")
        return spark_df

    @property
    def target_schema(self):
        target_schema = StructType(
            [
                StructField("carType", StringType(), True),
                StructField("color", StringType(), True),
                StructField("condition", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("drive", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("make", StringType(), True),
                StructField("manufacture_year", IntegerType(), True),
                StructField("mileage", FloatType(), True),
                StructField("milage_unit", StringType(), True),
                StructField("model", StringType(), True),
                StructField("model_variant", StringType(), True),
                StructField("price_on_request", BooleanType(), True),
                StructField("type", StringType(), True),
                StructField("zip", StringType(), True),
                StructField("manufacture_month", IntegerType(), True),
                StructField("fuel_consumption_unit", StringType(), True),
            ]
        )
        return target_schema
