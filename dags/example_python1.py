from datetime import datetime, timedelta
import requests
import os
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="example_python1",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1}
):
    def print_hello():
        print("Hello world")

    task1 = PythonOperator(
        task_id="task1",
        python_callable=print_hello
    )

    task2 = BashOperator(
        task_id="task2",
        bash_command="echo 'Hello world'"
    )


    def http_request(url):
        response = requests.get(url)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Print the content of the response
            print(response.json())  # Assuming it returns JSON data
            return response.json()
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")

            # Raise an error to terminate the DAG
            raise ValueError(f"Failed to retrieve data. Status code: {response.status_code}")
    

    def get_data():
        url = "https://meowfacts.herokuapp.com/"
        response = http_request(url)
        data = {}
        data['data'] = response['data']
        # Get current date and time
        now = datetime.now()
        data['timestamp'] = now.strftime("%Y-%m-%d %H:%M:%S")

        print(data)
        return data

    def get_filename():
        # Define target directory
        target_directory = "~/data/staging/"
        expanded_directory = os.path.expanduser(target_directory)

        # Create the directory if it doesn't exist
        os.makedirs(expanded_directory, exist_ok=True)

        # Get current date and time
        now = datetime.now()

        # Format date and time as a string
        filename = now.strftime("file_%Y%m%d_%H%M.json")

        # Join the directory and filename
        filepath = expanded_directory + filename
        return filepath
    
    def get_data_and_save():
        data = get_data()
        filepath = get_filename()

        with open(filepath, "w") as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to {filepath}")

    task3 = PythonOperator(
        task_id="task3",
        python_callable=get_data_and_save
    )




    task1 >> task2 >> task3

    '''
        def print_hello():
            print("Hello world")

        task1 = PythonOperator(
            task_id="task1",
            python_callable=print_hello
        )

        task2 = BashOperator(
            task_id="task2",
            bash_command="echo 'Hello world'"
        )

        task1 >> task2

    '''