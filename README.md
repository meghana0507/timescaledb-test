# timescaledb-test

## Pre-requisites:
1. Python virtual environment setup:
  a. Install virtualenv if not already installed:
    > python -m pip install --user virtualenv
  b. Navigate to project directory:
    > git clone https://github.com/meghana0507/timescaledb-test.git/
    > cd timescaledb-test
  c. Create virtual environment:
    > python -m virtualenv env
  d. Activate virtual environment:
    > source env/bin/activate
    
  Make sure `(env)` is displayed to the left side of the shell to ensure project runs in virtual environment.
  
2. Install pip packages from requirements.txt file:
  > pip install -r requirements.txt
  
3. Create a .env file in the project folder to store database connection details like below:
  db_user=postgres
  db_host=localhost
  db_name=homework
  
## How to run:
The tool takes two arguments from the command line, like below:
  python query-tool.py -q <query-input-file> -n <number-of-workers>
  
In this case for example, try running:
  > python query-tool.py -q query_params.csv -n 3

## Other notes:
1. This project was setup using Python 3.8.5
2. The tool generates `output.log` file for more details on query results
