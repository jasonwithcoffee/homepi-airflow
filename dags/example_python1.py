from datetime import datetime, timedelta
import requests
import os
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="example_python1",
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(minutes=15),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
):

    def get_datetime_now(ti):

        # get time now and pass to xcom
        now = datetime.now()
        now_str = now.isoformat()

        ti.xcom_push(key="datetime_now", value=now_str)

    task1_get_time = PythonOperator(
        task_id="task1_get_time", python_callable=get_datetime_now, provide_context=True
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
            raise ValueError(
                f"Failed to retrieve data. Status code: {response.status_code}"
            )

    def get_data(ti):
        url = "https://meowfacts.herokuapp.com/"
        response = http_request(url)
        data = {}
        data["data"] = response["data"][0]
        # Get current date and time
        # now = datetime.now()
        datetime_str = ti.xcom_pull(task_ids="task1_get_time", key="datetime_now")
        now = datetime.fromisoformat(datetime_str)
        data["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")

        print(data)
        return data

    def get_filename(ti):
        # Define target directory
        target_directory = "/opt/airflow/data/staging/"
        # expanded_directory = os.path.expanduser(target_directory)

        # Create the directory if it doesn't exist
        os.makedirs(target_directory, exist_ok=True)
        contents = os.listdir(target_directory)
        print(target_directory)
        print(contents)

        # Get current date and time
        datetime_str = ti.xcom_pull(task_ids="task1_get_time", key="datetime_now")
        now = datetime.fromisoformat(datetime_str)

        # Format date and time as a string
        filename = now.strftime("file_%Y%m%d_%H%M.json")

        print(filename)
        # Join the directory and filename
        filepath = target_directory + filename
        return filepath

    def get_data_and_save(ti):
        data = get_data(ti)
        filepath = get_filename(ti)

        with open(filepath, "w") as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to {filepath}")

    task2_api_call = PythonOperator(
        task_id="task2_api_call",
        python_callable=get_data_and_save,
        provide_context=True,
    )

    task1_get_time >> task2_api_call
