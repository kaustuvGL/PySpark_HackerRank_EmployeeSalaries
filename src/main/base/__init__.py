import abc

from pyspark.sql import SparkSession, DataFrame


class PySparkJobInterface(abc.ABC):

    def __init__(self):
        self.spark = self.init_spark_session()

    @abc.abstractmethod
    def init_spark_session(self) -> SparkSession:
        """Create spark session"""
        # spark = SparkSession.builder.getOrCreate()
        # return spark
        raise NotImplementedError

    def read_csv(self, input_path: str) -> DataFrame:
        return self.spark.read.options(header=True, inferSchema=True).csv(input_path)

    @abc.abstractmethod
    def average_salary(self, employee: DataFrame) -> int:
        raise NotImplementedError

    def stop(self) -> None:
        self.spark.stop()
