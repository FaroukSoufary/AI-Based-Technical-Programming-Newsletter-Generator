import os
import sys
import time
import requests
import json
from utils import fetch_data, load_into_cloud, copy_into_snowflake_table, fill_final_table
import subprocess

def run_pipeline(quota_limit):

    with open('parameters.json', 'r') as file:
        params = json.load(file)

   
    file_format_name = params['file_format_name']
    schedule_path = params['schedule_path']
    target_q_dir = params['target_q_dir']
    target_a_dir = params['target_a_dir']
    checkpoint_path = params['checkpoint_path']
    copy_log = params['copy_log']
    dbt_project_path = params['dbt_project_path']

    
    snowflake_user = os.getenv('SNOWFLAKE_USER')
    snowflake_acc = os.getenv('SNOWFLAKE_ACC')
    snowflake_wh = os.getenv('SNOWFLAKE_WH')
    snowflake_db = os.getenv('SNOWFLAKE_DB')
    snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')
    
    account_name = os.getenv('AZURE_ACC_NAME')
                             
    api_key = os.getenv('STACKEXCHANGE_API_KEY')
    snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
    account_key = os.getenv('AZURE_ACC_KEY')


    
    while True:
        # time.sleep(3)
        session = requests.Session()
        return_val = None
        try:
            print("Retrieving Data...")
            with open(schedule_path, 'r+') as f:
                sch = json.load(f)

            target_tag = None
            for key, val in sch.items():
                if val == '0':
                    target_tag = key
                    break

            fetch_t0 = time.time()
            return_val = fetch_data(session, [target_tag], schedule_path, target_q_dir, target_a_dir, checkpoint_path, api_key, quota_limit)
            fetch_tf = time.time()

            print("Fetching duration : " + str(round(fetch_tf-fetch_t0, 2)))

            load_t0 = time.time()
            load_into_cloud(target_q_dir, target_a_dir, copy_log, account_name, account_key)
            load_tf = time.time()

            print("Loading duration : " + str(round(load_tf - load_t0, 2)))

            copy_t0 = time.time()
            copy_into_snowflake_table(copy_log, file_format_name, snowflake_user, snowflake_password, snowflake_acc, snowflake_wh, snowflake_db, snowflake_schema)
            copy_tf = time.time()

            print("Copying duration : " + str(round(copy_tf - copy_t0, 2)))
        except Exception as e:
            print(f"An exception was raised: {e}")

        if return_val == True:
            behaviour = "run_dbt"
        elif return_val == False:
            behaviour = "stop"
        else:
            behaviour = "rerun"
    
        if behaviour == "run_dbt":
            print("Running dbt...")

            command = [
                "dbt", "run",
                "--project-dir", dbt_project_path
            ]

            try:
                result = subprocess.run(command, check=True, capture_output=True, text=True)
                print("DBT model run successfully.")
            except subprocess.CalledProcessError as e:
                print("Error running DBT model:", e.stderr)

            fill_final_table(snowflake_user, snowflake_password, snowflake_acc, snowflake_wh, snowflake_db, snowflake_schema)
        elif behaviour == "rerun":
            print("Waiting for 60 seconds before rerunning...")
            time.sleep(60)
        elif behaviour == "stop":
            print("Stopping the process.")
            break


if len(sys.argv) < 2:
    print("Usage: python3 scheduler.py <quota_limit>")
    print("Description: quota_limit refers to the amount of daily calls that will remain after the execution of the pipeline")
    print("""Example : - type 0 to consume the entire daily quota (10000 api calls)
          - type 5000 to consume half the daily quota and keep half of it""")
    sys.exit(1)

quota_limit = int(sys.argv[1])


run_pipeline(quota_limit)
