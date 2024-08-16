from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import snowflake.connector
import pandas as pd
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 23), 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'stackoverflow_weekly_ingestion',
    default_args=default_args,
    description='Retrieve StackOverflow questions and answers from the last week and load them into Snowflake',
    schedule_interval='0 9 * * 1' # Cron expression for every Monday at 9:00 AM
)


def fetch_stackoverflow_data():

    api_key = os.getenv('STACKEXCHANGE_API_KEY')
    last_week = int((datetime.now() - timedelta(days=7)).timestamp())
    api_url = "https://api.stackexchange.com/2.3/answers"
    params = {
        'key':api_key,
        'pagesize':100,
        'fromdate': last_week,
        'todate': int(datetime.now().timestamp()),
        'order': 'asc',
        'sort': 'creation',
        'site': 'stackoverflow',
        'filter': '!)qXWnHAIkwK7PiwNONT6'  # Custom filter to include needed fields
    }


    response = requests.get(api_url, params=params)
    data = response.json()

    # Filtering questions to include only those with an accepted answer
    answers = []
    for item in data['items']:
        if item['is_accepted'] == True:
            answers.append(item)

    return answers





def load_data_to_snowflake(**kwargs):
    ti = kwargs['ti']
    # Getting the questions data from the previous task
    answers = ti.xcom_pull(task_ids='fetch_stackoverflow_data')
    api_key = os.getenv('STACKEXCHANGE_API_KEY')


    records = []
    for answer in answers:
        record = {
            'tags': '',
            'title': '',
            'question_body': '',
            'question_id': answer['question_id'],
            'answer_creation_date': answer['creation_date'],
            'question_creation_date':'',
            'question_score':'',
            'favorite_count':'',
            'question_upvote_count':'',
            'view_count':'',
            'answer_body': answer['body_markdown'], 
            'answer_upvote_count':answer['up_vote_count'],
            'answer_score':answer['score'],
            'answer_id':answer['answer_id']
        }
        # Fetching the accepted answer
        question_url = "https://api.stackexchange.com/2.3/questions/" + str(answer['question_id'])
        question_params = {
            'key':api_key,
            'site': 'stackoverflow',
            'filter': '!LbeL0UFTwm63R2NM(EP17R'
        }
        question_response = requests.get(question_url, params=question_params)
        question_data = question_response.json()
        if 'items' in question_data and len(question_data['items']) > 0:
            record['question_body'] = question_data['items'][0]['body_markdown']
            record['question_upvote_count'] = question_data['items'][0]['up_vote_count']
            record['question_score'] = question_data['items'][0]['score']
            record['question_creation_date'] = question_data['items'][0]['creation_date']
            record['title'] = question_data['items'][0]['title']
            record['tags'] = ','.join(question_data['items'][0]['tags'])
            record['favorite_count'] = question_data['items'][0]['favorite_count']
            record['view_count'] = question_data['items'][0]['view_count']

        records.append(record)

    df = pd.DataFrame(records)

    snowflake_user = os.getenv('SNOWFLAKE_USER')
    snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
    snowflake_acc = os.getenv('SNOWFLAKE_ACCOUNT')
    snowflake_wh = os.getenv('SNOWFLAKE_WH')
    snowflake_db = os.getenv('SNOWFLAKE_DB')
    snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')

    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_acc,
        warehouse=snowflake_wh,
        database=snowflake_db,
        schema=snowflake_schema
    )

    cursor = conn.cursor()

    # Truncate the table to remove old data and replace with new data of the last week
    cursor.execute("""
        Truncate TABLE stackoverflow_weekly
    """)


    for index, row in df.iterrows():
        print(row)
        cursor.execute("""
            INSERT INTO stackoverflow_weekly (tags, title, question_body, answer_body, question_id, question_creation_date, question_score, favorite_count, question_upvote_count, view_count, answer_upvote_count, answer_score, answer_id, answer_creation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['tags'], row['title'], row['question_body'], row['answer_body'], row['question_id'], row['question_creation_date'],
              row['question_score'], row['favorite_count'], row['question_upvote_count'], row['view_count'],
            row['answer_upvote_count'], row['answer_score'], row['answer_id'], row['answer_creation_date']))

    cursor.close()
    conn.close()





def generate_newsletter():


    snowflake_user = os.getenv('SNOWFLAKE_USER')
    snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
    snowflake_acc = os.getenv('SNOWFLAKE_ACCOUNT')
    snowflake_wh = os.getenv('SNOWFLAKE_WH')
    snowflake_db = os.getenv('SNOWFLAKE_DB')
    snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')

    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_acc,
        warehouse=snowflake_wh,
        database=snowflake_db,
        schema=snowflake_schema
    )

    cursor = conn.cursor()
    cursor.execute("""
        CALL GENERATE_NEWSLETTER();
    """)
    cursor.close()
    conn.close()




# Defining the tasks
fetch_task = PythonOperator(
    task_id='fetch_stackoverflow_data',
    python_callable=fetch_stackoverflow_data,
    dag=dag,
    provide_context=True
)

load_task = PythonOperator(
    task_id='load_data_to_snowflake',
    python_callable=load_data_to_snowflake,
    provide_context=True,
    dag=dag,
)

generate_task = PythonOperator(
    task_id='generate_newsletter',
    python_callable=generate_newsletter,
    provide_context=True,
    dag=dag,
)

fetch_task >> load_task >> generate_task
