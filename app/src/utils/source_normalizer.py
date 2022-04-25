import translators as ts
import pandas as pd
from tqdm import tqdm
import logging
from pyspark.sql import DataFrame


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


tqdm.pandas()


class SourceNormalizer:
    @classmethod
    def normalize_pre_df(cls, pre_df, sc: "SqlContext") -> "pd.DataFrame":
        """
        Normalize the source dataframe. Switching to pandas.
        """

        logger.info("Normalizing source dataframe.")

        all_columns = pre_df.columns

        pre_df[all_columns] = pre_df[all_columns].replace(
            {'"': "", "'": "", r"\\": ""}, regex=True
        )
        logger.info("Translating columns...")
        
        pre_df["color"] = pre_df.progress_apply(cls._translate_color_to_english, axis=1)
        pre_df.drop(columns=["BodyColorText"], inplace=True)
        pre_df["make"] = pre_df.apply(cls._normalize_make_column, axis=1)

        pre_df = pre_df.astype({"FirstRegYear": "int", "FirstRegMonth": "int"})
        pre_df.style.set_properties(**{"text-align": "left"})
        logger.info("Normalization done, writting to csv")

        pre_df.to_csv("results/task_2.csv", header=True,encoding="utf-8")

        return pre_df

    

    
    @staticmethod
    def _translate_color_to_english(df: pd.DataFrame) -> str:
        """
         calls a free service to translate the color to english,
         NOTE: it is taking up to 20 minutes to run through the whole dataset
        """
        if df["BodyColorText"]: # Here
 
            color_splt = df["BodyColorText"].split(" ")
            color_to_trslt = color_splt[0].title()
            trnslted :str = ts.bing(color_to_trslt)

            return trnslted #Here
        return "Other"

    @staticmethod
    def _normalize_make_column(df: pd.DataFrame) -> str:
        """
        BMW -> BMW (less or equal than 3),
        MERCEDES-BENZ -> Mercedes-Benz
        LAND ROVER -> Land Rover
        AUDI -> Audi
        """
        if df["make"]:
            lower = df["make"].lower()
            if len(lower) <= 3:
                return lower.upper()
            elif "-" in lower:
                splt = lower.split("-")
                if len(splt) == 2:

                    left = splt[0].title()
                    right = splt[1].title()
                    return "-".join([left, right])
            elif "(" in lower:
                splt = lower.split("(")
                return splt[0].title()
            elif " " in lower:
                splt = lower.split(" ")
                if len(splt) == 2:
                    left = splt[0].title()
                    right = splt[1].title()
                    return " ".join([left, right])
            
            elif len(lower) > 3:
                return lower.title()
        return df["make"]
