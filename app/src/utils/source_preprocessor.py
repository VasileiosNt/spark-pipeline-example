from pyspark.sql import functions as F
import logging
import pandas as pd


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PreprocessorSource:
    def __init__(self, source_file_path, spark_session)-> None:
        logger.info("Loading source data...")
        self.source_file_path = source_file_path
        self.session = spark_session
        self.source_df = self.session.read.json(self.source_file_path)
        logger.info(f"Loaded {self.source_df.count()} rows from source data.")


    def _get_trimmed_col(self) -> "DataFrame":
        columns = self.source_df.columns
        for colname in columns:
            trimmed_df = self.source_df.withColumn(colname, F.trim(F.col(colname)))

        logger.info(f"{trimmed_df.count()} rows after trimming.")

        return trimmed_df

    def preprocess_source_df(self)-> "pd.DataFrame":
        logger.info("Preprocessing source data...")

        # Naive approach with statically defined the columns, best here would be to dynamically define columns.
        columns = [
            "TypeName",
            "make",
            "ModelText",
            "BodyColorText",
            "BodyTypeText",
            "Ccm",
            "City",
            "Co2EmissionText",
            "ConditionTypeText",
            "ConsumptionRatingText",
            "ConsumptionTotalText",
            "Doors",
            "DriveTypeText",
            "FirstRegMonth",
            "FirstRegYear",
            "FuelTypeText",
            "Hp",
            "InteriorColorText",
            "Km",
            "Properties",
            "Seats",
            "TransmissionTypeText",
        ]

        trimmed_df = self._get_trimmed_col()
        source_df = trimmed_df.withColumnRenamed("MakeText", "make")

        logger.info("Switching to pandas.")
        pandas_df = source_df.toPandas()




        grpd_rows_dict = self._group_and_pivot_source(pandas_df)
        logger.info(f"{len(grpd_rows_dict)} rows after grouping and pivoting.")
        verfied_columns = self._verify_column_integrity(grpd_rows_dict, columns)


        loaded_dataset = self._create_gran_dataset(verfied_columns, columns)
        
        
        loaded_dataset.to_csv("results/task_1.csv", header=True,encoding="utf-8")
        logger.info(f"Preprocessing complete, source data rows {len(loaded_dataset)}")

        return loaded_dataset

    @staticmethod
    def _verify_column_integrity(data:list[dict[str,str]], columns:list[str])->list[dict[str,str]]:
        for i in data:
            for col in columns:
                if col not in i.keys():
                    i.update({col: "null"})
        return data

    def _group_and_pivot_source(self, df:pd.DataFrame)->list[dict[str,str]]:
        df = df.groupby("ID")
        rows_dict = []
        for key, _ in df:
            # for each group, pivot the into index TypeName, make, ModelText and create Attribute Names and Values
            #Getting the first row since the rest are identical
            grouped = df.get_group(key)
            pivot = grouped.pivot(
                index=["TypeName", "make", "ModelText"],
                columns="Attribute Names",
                values="Attribute Values",
            )

            pivot = pivot.rename_axis(None, axis="columns")
            pivot = pivot.reset_index()
            
            pivot = pivot.fillna("null")
            data_dict = pivot.to_dict("records")
            rows_dict.extend(data_dict)
            continue
        return rows_dict

    def _create_gran_dataset(self, rows_dict, columns) -> pd.DataFrame:
        return pd.DataFrame.from_records(rows_dict, columns=columns)

    
