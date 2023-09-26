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
        employee = employee.filter((employee["salary"].isNotNull()) & (employee["salary"] > 0))
        avg_salary = employee.select(avg("salary").alias("avg_salary")).first()["avg_salary"]
        return int(avg_salary)


