import pytest
from main.job.pipeline import PySparkJob
from pyspark.sql import SparkSession

job = PySparkJob()

employee_schema = ["first_name", "last_name", "salary"]
employee_sample_1 = [
    ("John", "Doe", 50000),
    ("Jane", "Smith", 60000),
    ("Bob", "Johnson", 75000),
    ("Alice", "Williams", 55000),
    ("Charlie", "Brown", 80000)
]

employee_sample_2 = [("John", "Doe", 50000),
                     ("Jane", "Smith", 60000),
                     ("Bob", "Johnson", 75000),
                     ("Alice", "Williams", 55000),
                     ("Charlie", "Brown", 80000),
                     ("Eve", "Taylor", None),
                     ("Frank", "Wilson", -10000),
                     ("Grace", "Adams", 90000),
                     ("Harry", "Clark", 40000),
                     ("Ivy", "Davis", 30000)]

employee_sample_3 = [("John", "Doe", 50000),
                     ("Jane", "Smith", 60000),
                     ("Bob", "Johnson", 75000),
                     ("Alice", "Williams", 55000),
                     ("Charlie", "Brown", 80000),
                     ("Eve", "Taylor",0 ),
                     ("Frank", "Wilson", None),
                     ("Grace", "Adams", 90000),
                     ("Harry", "Clark", 40000),
                     ("Ivy", "Davis", 30000)]
def create_sample(sample, data_schema):
    return job.spark.createDataFrame(data=sample, schema=data_schema)


# @pytest.mark.filterwarnings("ignore")
# def test_init_spark_session():
#     assert isinstance(job.spark, SparkSession), "-- spark session not implemented"

# Test case 1 : Easy
@pytest.mark.filterwarnings("ignore")
def test_average_salary_1():
    employee_df = create_sample(employee_sample_1, employee_schema)
    average_salary = job.average_salary(employee_df)
    print(average_salary)
    assert (64000 == average_salary)


# Test Case 2 : Medium
@pytest.mark.filterwarnings("ignore")
def test_average_salary_2():
    employee_df = create_sample(employee_sample_2, employee_schema)
    average_salary = job.average_salary(employee_df)
    print(average_salary)
    assert (60000 == average_salary)


# Test Case 3 : Hard
@pytest.mark.filterwarnings("ignore")
def test_average_salary_3():
    employee_df = create_sample(employee_sample_3, employee_schema)
    average_salary = job.average_salary(employee_df)
    print(average_salary)
    assert (60000 == average_salary)