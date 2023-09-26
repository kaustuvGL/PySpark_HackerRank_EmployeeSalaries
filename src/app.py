from main.job.pipeline import PySparkJob
import sys


def main():
    job = PySparkJob()

    # Load input data to DataFrame
    print("<<Reading CSV>>")
    employee = job.read_csv(sys.argv[1])

    # Get number of distinct IDs
    print("<<Average Salary>>")
    avg_salary = job.average_salary(employee)
    print(avg_salary)
    job.stop()


if __name__ == '__main__':
    main()
