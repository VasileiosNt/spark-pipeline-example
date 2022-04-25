

from src.utils.source_normalizer import SourceNormalizer
import pandas as pd

import unittest
from pyspark.sql import  SparkSession
from pathlib import Path


sup_data_path = (
    f"{str(Path(__file__).parent.absolute())}/assets/test_source.json"
)

class DatePlaceExtractorTest(unittest.TestCase):
    @classmethod
    def setUp(cls):
        
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())
        

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    

    def test_normalizing_make_column(self):
       
        test_data = ["BMW","AUDI","MERCEDES-BENZ","PORSCHE","VOLKSWAGEN"]
        test_df = pd.DataFrame(test_data, columns = ['make'])
        nrml = test_df.apply(SourceNormalizer._normalize_make_column, axis = 1)
        
        self.assertEqual(nrml.count(), 5)
        self.assertEqual(nrml.iloc[0], "BMW")
        self.assertEqual(nrml.iloc[1], "Audi")
        self.assertEqual(nrml.iloc[2], "Mercedes-Benz")
        self.assertEqual(nrml.iloc[3], "Porsche")

    def test_translation_column(self):

        test_data = ["Weiss","Schwarz"]
        test_df = pd.DataFrame(test_data, columns = ['BodyColorText'])
        nrml  = test_df.apply(SourceNormalizer._translate_color_to_english, axis = 1)
        self.assertEqual(nrml.count(), 2)
        self.assertEqual(nrml.iloc[0], "White")
        self.assertEqual(nrml.iloc[1],"Black")



