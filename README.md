## Environment:
- Spark Version: `3.4.1`
- Python Version: `3.11`

##  Problem Description:

The HR department of `LTI MindTree` seeks assistance in computing the average salary of employees within the organization. They have provided a classified and encrypted dataset containing information about employee salaries. 

Also, It has been told that, in the dataset, employee salaries are marked as zero or 'None,'   values if they have left the organization and if they have left and owe money to the company then that ammount is mentioned in negative.
 

A snapshot of dataset is given in `employees.csv`  

- The dataset is structured with columns: 
   `employee_id`, 
    `first_name`, 
    `last_name`, and 
   `salary`.
   
- Each row in the CSV file represents an `employee_id`.

 
## Task:
Your task is to complete a Spark job that calculates the average salary of current employees within the organization. The project is partially implemented, and the only the below mention method needs implementation. It is located in the file `src/main/job/pipeline.py`:

 
- `def average_salary(self, employee: DataFrame) -> int:`


Your goal is to complete the implementation of this method to ensure that it accurately computes the average salary of current employees. You can use the provided unit tests to validate your solution and track your progress while solving the problem.

## Commands
- run: 
```bash
source venv/bin/activate; python3 src/app.py data/employee.csv
```
- install: 
```bash
bash make.sh __install; source venv/bin/activate; pip3 install -r requirements.txt
```
- test: 
```bash
source venv/bin/activate; py.test -p no:warnings