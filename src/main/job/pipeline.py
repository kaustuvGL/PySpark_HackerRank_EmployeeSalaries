from main.base import (PySparkJobInterface)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import coalesce, lit
from pyspark.sql.functions import avg


class PySparkJob(PySparkJobInterface):

    @staticmethod
    def init_spark_session() -> SparkSession:
        spark = SparkSession.builder.getOrCreate()
        return spark

    def average_salary(self, employee: DataFrame) -> int:
        # TODO: Put your code here
        


