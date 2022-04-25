from utils.source_preprocessor import PreprocessorSource
from utils.source_normalizer import SourceNormalizer
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pathlib import Path
from utils.target_preprocessor import PreprocessorTarget
from pyspark.sql import functions as F
from pyspark.sql.functions import when
from pyspark.sql.types import (
    StringType,
    FloatType,
    BooleanType,
)
from typing import Final
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


sc = SparkContext("local[*]", "SparkExample")
sql = SQLContext(sc)
ss = SparkSession.builder.master("local[*]").appName("SparkExample").getOrCreate()


sup_data_path = (
    f"{str(Path(__file__).parent.parent.absolute())}/assets/supplier_car.json"
)
target_data_path = (
    f"{str(Path(__file__).parent.parent.absolute())}/assets/Target_Data.xlsx"
)

tf = PreprocessorTarget(target_data_path, sc, sql)
sf = PreprocessorSource(sup_data_path, ss)

tf_df = tf.get_spark_df()

def main():
    # ------- Preprocessing --------
    pre_df = sf.preprocess_source_df()
    # ------- Normalization --------
    nrml = SourceNormalizer.normalize_pre_df(pre_df, sc)

    
    # ------ Feature Extraction-------
    spark_df = ss.createDataFrame(nrml)
    logger.info("Back to pyspark")

    logger.info("Extracting features...")

    with_extracts = spark_df.withColumn(
        "extracted-value-ConsumptionTotalText",
        F.split(F.col("ConsumptionTotalText"), " ").getItem(0),
    ).withColumn(
        "extracted-unit-ConsumptionTotalText",
        F.split(F.col("ConsumptionTotalText"), " ").getItem(1),
    )

    with_extracts.toPandas().to_csv("results/task_3.csv", header=True,encoding="utf-8")
    logger.info("Extraction complete, features written to csv")

    # ---- Integration------
    logger.info("Integrating...")

    renamed_df = (
        with_extracts.withColumnRenamed("ModelText", "model")
        .withColumnRenamed("BodyColorText", "color")
        .withColumnRenamed("BodyTypeText", "carType")
        .withColumnRenamed("Ccm", "ccm")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("Co2EmissionText", "co2")
        .withColumnRenamed("ConsumptionTotalText", "consumption")
        .withColumnRenamed("FirstRegYear", "manufacture_year")
        .withColumnRenamed("FirstRegMonth", "manufacture_month")
        .withColumnRenamed("TypeName", "model_variant")
        .withColumnRenamed("ConditionTypeText", "condition")
    )

    redudant_columns = [
        "Seats",
        "TransmissionTypeText",
        "extracted-unit-ConsumptionTotalText",
        "extracted-value-ConsumptionTotalText",
        "ModelText",
        "ModelTypeText",
        "TypeName",
        "TypeNameFull",
        "FuelTypeText",
        "Properties",
        "InteriorColorText",
        "Hp",
        "Doors",
        "Properties",
        "DriveTypeText",
        "ConsumptionRatingText",
        "Km",
        "co2",
    ]

    pruned_df = renamed_df.drop(*redudant_columns)


    integrated_df = (
        pruned_df.withColumn("mileage", F.lit("0.0").cast(FloatType()))
        .withColumn("price_on_request", F.lit(False).cast(BooleanType()))
        .withColumn("zip", F.lit("").cast(StringType()))
        .withColumn("milage_unit", F.lit(None).cast(StringType()))
        .withColumn("country", F.lit(None).cast(StringType()))
        .withColumn("drive", F.lit(None).cast(StringType()))
        .withColumn("currency", F.lit(None).cast(StringType()))
    )


    # some last transformations
    integrated_df = integrated_df.withColumn(
        "condition",
        when(F.col("condition") == "Neu", "New")
        .when(F.col("condition") == "Oldtimer", "Restored")
        .when(F.col("condition") == "Vorführmodell", "Demonstration")
        .otherwise(F.col("condition")),
    )


    integrated_df = integrated_df.withColumn(
        "carType",
        when(F.col("carType").like("%SUV%"), "SUV")
        .when(F.col("carType").like("%Kleinwagen%"), "Small Car")
        .when(F.col("carType").like("%Minivan%"), "Minivan")
        .when(F.col("carType") == "Sattelschlepper", "Truck")
        .when(F.col("carType") == "Kombi", "Station Wagon")
        .when(F.col("carType") == "Wohnkabine", "Camper Pick-up")
        .otherwise(F.col("carType")),
    )
    integrated_df = integrated_df.withColumn(
        "type", when(F.col("carType").like("%Truck%"), "truck").otherwise(F.lit("car"))
    )
    integrated_df = integrated_df.withColumn(
        "fuel_consumption_unit",
        when(F.col("consumption").like("%km%"), "l_km_consumption").otherwise(F.lit(None)),
    )


    # Reorder
    integrated_df = integrated_df.select(
        "carType",
        "color",
        "condition",
        "currency",
        "drive",
        "city",
        "country",
        "make",
        "manufacture_year",
        "mileage",
        "milage_unit",
        "model",
        "model_variant",
        "price_on_request",
        "type",
        "zip",
        "manufacture_month",
        "fuel_consumption_unit",
    )
    # I had to switch to pandas and recreate the dataframe with the target schema,
    pd_integrated_df = integrated_df.toPandas()
    integrated_df : Final = ss.createDataFrame(pd_integrated_df, schema=tf.target_schema)


    if set(integrated_df.schema).symmetric_difference(set(tf_df.schema)) == set():
        logger.info("Integration complete, writing to csv")
        integrated_df.toPandas().to_csv("results/task_4.csv", header=True,encoding="utf-8")
        # extended = tf_df.union(integrated_df)
        # extended.toPandas().to_excel("task_5.xlsx", header=True)
        
        ss.stop()
    else:
       
        raise Exception("Schema of source dataframe does not match target  schema")

if __name__ == "__main__":
    main()