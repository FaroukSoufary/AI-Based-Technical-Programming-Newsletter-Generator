import requests
import snowflake.connector
import pandas as pd
import json
import os
import shutil
from azure.storage.blob import BlobServiceClient
import time
from bs4 import BeautifulSoup


"""
This function is called by fetch_data() as a helper to retrieve the best answers using their ids in the response of the API call 
for question retrieval

Input : 
    - id_list : a list of answer ids (ideally the accepted_answer_id from the question recording), (should not exceed 100 elements)
    - api_key : the stackexchange api personal key

process :
    - Uses a specific stackexchange api endpoint for answer retrieval
    - Uses the id_list as a parameter to target the wanted answers
    - Returns the response if the call is successful, returns None object otherwise
"""

def get_answers_by_id(session, id_list, api_key):
    
    params = {
        'key':api_key,
        'site':'stackoverflow',
        'filter':'!apyOS4)o)GxWu6',
        'pagesize':100
    }

    url=f"https://api.stackexchange.com/2.3/answers/"

    url = url + ';'.join(map(lambda x: str(int(x)), id_list))
    try:
        response = session.get(url, params=params)
        response.raise_for_status()

    except requests.exceptions.RequestException as err:
        print("Encountered an error while fetching answers:",err)
        return None

    return response


"""
Input :
    - tags : a list of the tags that should all be associated to the question in order to retrieve it
    - checkpoint_path : the path to the the checkpoint file (json) --if it doesn't exist, the function will create it--
    - quota_limit : the threshold of quota credit under which the function shall stop
    - api_key : the stackexchange api personal key

Process :
    - Uses the stackexchange API to retrieve questions and their answers (that meet specific requirements) historically from oldest to newest.
    - In the while loop, each iteration retrieves 100 questions as well as their accepted answers
    - Uses a checkpoint file to keep track of the date of the latest retrieved question in order to use it as a starting point in the next call
    - Saves the questions in a csv file with the following columns :
            ['tags', 'accepted_answer_id', 'answer_count', 'score', 'creation_date', 'question_id', 'title', 'body']
    - Saves the answers in a csv file with the following columns :
            ['answer_id', 'question_id', 'body']
"""

def fetch_data(session, tags, schedule_path, target_q_dir, target_a_dir, checkpoint_path, api_key, quota_limit=0):

    
    url='https://api.stackexchange.com/2.3/questions'
    tag_value = ";".join(tags)
    # Check if the checkpoint file exists and load the latest date, otherwise create the checkpoint file
    try:
        with open(checkpoint_path, 'r+') as f:
            if(os.path.getsize(checkpoint_path) == 0):
                data = {}
            else:
                data = json.load(f)
    except FileNotFoundError:
        with open(checkpoint_path, 'w') as fp:
            pass
        data = {}
    
    if tag_value in data:
        from_date = int(data[tag_value])
    else:
        from_date = 0
        with open(checkpoint_path, 'w') as f1:
            data[tag_value] = "0"
            json.dump(data , f1)

    # api request parameters
    params = {
        'key':api_key,
        'pagesize':100,
        'sort':'creation', # Use creation date for sorting questions (needed to load data historically and gradually)
        'order':'asc', 
        'site':'stackoverflow', 
        'tagged':tag_value, # Select only questions that are tagged with this specific set of tags
        'fromdate':from_date, # Select only questions that were created after this data (date in timestamp)
        'filter':'!)riR7ZJuB__VlNdi-mPJ' # This filter specifies the attributes that we want, I made it using the API's documentation ( https://api.stackexchange.com/docs/questions#&filter=!)riR7ZJuB__VlNdi.(a2&site=stackoverflow&run=true )
    }

    num_time_outs = 0 # Count the number of consecutive time outs before terminating the process
    """
        The first request that initializes the initial batch of questions as a pandas dataframe
        In case there is a timeout error, we keep retrying until we reach 1000 consecutive timeouts then we terminate the process
    """   
    while(1):
        time.sleep(2)
        try:
            response = session.get(url, params=params)
            response.raise_for_status()
            break

        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:",errt)
            if(num_time_outs<10):
                num_time_outs += 1
                continue
            else:
                print("10 consecutive time outs (Initial Batch), terminating process ..")
                raise SystemExit(errt)

        except requests.exceptions.RequestException as err:
            print("Initial batch retrieval error :")
            if(response.json()['error_name']=='throttle_violation'):
                print("Encountered a throttle violation error, pausing for 30 seconds ...")
                time.sleep(30)
            print(response.json())
            return None

    quota_remaining = response.json()['quota_remaining']
    questions_dict = response.json()['items']
    df0 = pd.DataFrame(questions_dict, columns=['tags', 'accepted_answer_id', 'answer_count', 'score', 'creation_date', 'question_id', 'title', 'body_markdown'])
    from_date = df0['creation_date'].max() + 1
    df0 = df0[df0['accepted_answer_id'].notna()] # We keep only questions that have accepted answers
    """
    We use the get_answers_by_id function to retrieve answers for the questions we just retrieved above
    In case the call fails, we keep trying a 100 times at most before abandoning the process
    If the call for answers fails, we terminate the process, no need to continue if the initial batch of answers can not be retrieved
    """
    answer_retries = 10

    while(answer_retries > 0):
        # time.sleep(2)
        answer_response = get_answers_by_id(session, df0['accepted_answer_id'].to_list())
        if(answer_response is not None):
            break
        answer_retries -= 1

    if(answer_response is None):
        print("Failed to load answers of the initial batch")
        return None
        
    quota_remaining = answer_response.json()['quota_remaining']
    if(quota_remaining < 2):
        print("No more quota !")
        return False
    answers_dict = answer_response.json()['items']
    df0_a = pd.DataFrame(answers_dict, columns=['last_activity_date', 'answer_id', 'question_id', 'body_markdown'])
    df0_a.drop('last_activity_date', axis=1, inplace=True)

    num_time_outs = 0
    """ 
    - Keep making requests until the quota_limit parameter is reached
    - Same error handling for timeout exception as before
    - In case there is an error :
        * if the number of questions retrieved that far is < 1000, terminate the process without saving
        * if it exceeds 1000, save the questions and update the checkpoint
    """
    while (quota_remaining > quota_limit):
        time.sleep(9)
        params['fromdate'] = from_date

        try:
            response = session.get(url, params=params)
            response.raise_for_status()

        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:",errt)
            if(num_time_outs<1):
                num_time_outs += 1
                continue
            else:
                print("10 Consecutive Timeout errors")
                if(len(df0) > 1000):
                    print("Saving data ...")
                    break
                print("No data will be saved, terminating the process..")
                return None
        except requests.exceptions.RequestException as err:
            print("Error while retrieving questions :")
            if(response.json()['error_name']=='throttle_violation'):
                print("Encountered a throttle violation error, pausing for 30 seconds ...")
                time.sleep(30)
            print(response.json())
            if(len(df0) > 1000):
                print("Error : ", err)
                print("Saving data ...")
                break
            print("No data will be saved, terminating the process..")
            print("Encountered an error while retrieving questions in the loop :", err)
            return None


        if 'has_more' not in response.json(): # In case the response json is an error message (do not append it to data)
            print("Unexpected Response : ", response.json())
            if(len(df0) > 1000):
                print("Saving data ...")
                break
            print("No data will be saved, terminating the process..")
            raise SystemExit()

        if 'quota_remaining' in response.json():
            quota_remaining = response.json()['quota_remaining']
            questions_dict = response.json()['items']
            df = pd.DataFrame(questions_dict, columns=['tags', 'accepted_answer_id', 'answer_count', 'score', 'creation_date', 'question_id', 'title', 'body_markdown'])

        # Update from_date ONLY if the answers were successfully retrieved 

        tmp_from_date = df['creation_date'].max() + 1

        df = df[df['accepted_answer_id'].notna()]

        """
        We use the get_answers_by_id function to retrieve answers for the questions we just retrieved above
        In case the call fails, we keep trying a 100 times at most before abandoning the process
        If the call for answers fails, we don't append the last batch of questions and we exit the loop to save the answered questions
        """
        
        answer_retries = 1
        while(answer_retries > 0):
            # time.sleep(2)
            answer_response = get_answers_by_id(session, df['accepted_answer_id'].to_list())
            if(answer_response is not None):
                break
            answer_retries -= 1


        if answer_response is None:
            print('Error: answer retrieval failed after 10 retries')
            break

        # Update from_date ONLY if the answers were successfully retrieved 
        from_date = tmp_from_date

        quota_remaining = answer_response.json()['quota_remaining']
        answers_dict = answer_response.json()['items']
        df_a = pd.DataFrame(answers_dict, columns=['last_activity_date', 'answer_id', 'question_id', 'body_markdown'])
        df_a.drop('last_activity_date', axis=1, inplace=True)


        df0 = pd.concat([df0, df], ignore_index=True)
        df0_a = pd.concat([df0_a, df_a], ignore_index=True)


        if response.json()['has_more']:
            time.sleep(2) # Sleep 2s not to abuse the API, 0.1s works fine but it can probably go lower
        # In case there is no more data for the tag
        else:
            print("No more data for : " + tag_value)
            with open(schedule_path, 'r+') as f:
                sch = json.load(f)
                sch[tag_value] = "1"
            # We can change the tag_value right here in order to make a smart while loop that only stops when all the quota is consumed and switches from tag to tag
            with open(schedule_path, 'w') as file:
                json.dump(sch, file, indent=4)
            break
        num_time_outs = 0



    """
    Update the checkpoint file with the latest date
    """

    with open(checkpoint_path, 'r+') as f:
        if(os.path.getsize(checkpoint_path) == 0):
            data ={}
        else:
            data = json.load(f)
        
        data[tag_value] = str(from_date)

    with open(checkpoint_path, 'w') as f1:
        json.dump(data, f1)


    if not os.path.exists(target_q_dir):
        os.makedirs(target_q_dir)
    if not os.path.exists(target_a_dir):
        os.makedirs(target_a_dir)

    target_q_csv = target_q_dir + '/' + tag_value + '_' + 'questions' + '_' +  str(from_date) + ".csv"
    target_a_csv = target_a_dir + '/' + tag_value + '_'  + 'answers' + '_' + str(from_date) + '.csv'



    df0.columns = ['tags', 'accepted_answer_id', 'answer_count', 'score', 'creation_date', 'question_id', 'title', 'body']
    df0_a.columns = ['answer_id', 'question_id', 'body']

    df0['body'] = df0['body'].apply(lambda x: BeautifulSoup(x, "html.parser").get_text())
    df0_a['body'] = df0_a['body'].apply(lambda x: BeautifulSoup(x, "html.parser").get_text())


    df0.to_csv(target_q_csv, sep=',', index=False, mode='a', header=not os.path.exists(target_q_csv))
    df0_a.to_csv(target_a_csv, sep=',', index=False, mode='a', header=not os.path.exists(target_a_csv))

    if(quota_remaining < 2):
        return False

    return True

    


"""
- Input : 
    - file_path : path of the file we want to load into our Azure container
    - account_name : the name of the azure storage account we will use
    - container_name : target container for blob loading
    - account_key : key of the account to grant access

- Process : 
    - Uses the parameters to connect to the Azure account
    - Terminates the process if the target file was already loaded in the cloud
    - Verifies if the blob is already in the container and terminates the process in that case
    - Otherwise, creates a new blob on Azure and fills it with the content of target file (gives the blob the same name as the file)

"""


def load_into_cloud(questions_dir, answers_dir, copy_log, account_name, account_key):

    storage_connection_string = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'

    qsts_files = os.listdir(questions_dir)
    ans_files = os.listdir(answers_dir)

    files = {}

    for file_name in qsts_files:
        file_path = os.path.join(questions_dir, file_name)
        files[file_path] = "question"

    for file_name in ans_files:
        file_path = os.path.join(answers_dir, file_name)
        files[file_path] = "answer"

    try:
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    except Exception as e:
        print("Error while creating the blob service client")
        raise SystemExit(e)

    try:
        qst_container_client = blob_service_client.get_container_client("questions2")
        ans_container_client = blob_service_client.get_container_client("answers2")
    except Exception as e:
        print("Error while creating the containers clients")
        raise SystemExit(e)


    for file_path, file_type in files.items():

        container_client = qst_container_client if file_type == "question" else ans_container_client

        try:

            blob_list0 = container_client.list_blobs()

            blob_list = [item['name'] for item in blob_list0]

            blob_name = file_path.split('/')[-1]

            if blob_name in blob_list:
                print(blob_name + " is already in Container : " + file_type + 's')
                return -1

            blob_client = container_client.get_blob_client(blob_name)

            with open(file_path, "r") as local_file:
                if not blob_client.exists():
                    blob_client.upload_blob(local_file.read(), overwrite=True)
                else:
                    continue

        except Exception as e:
                print(f"Error while listing, checking or creating blob: {e}")
                raise SystemExit(e)

    qst_list0 = qst_container_client.list_blobs()
    ans_list0= ans_container_client.list_blobs()

    qst_list = []
    ans_list = []

    for item in qst_list0:
        qst_list.append(item['name'])

    for item in ans_list0:
        ans_list.append(item['name'])

    for file_path, file_type in files.items():
        file_name = file_path.split('/')[-1]
        if file_type == "question":
            if file_name in qst_list:
                continue
            else:
                print(file_name + ' was not saved in questions container')
                raise SystemExit
        else:
            if file_name in ans_list:
                continue
            else:
                print(file_name + ' was not saved in answers container')
                raise SystemExit


    shutil.rmtree(questions_dir)
    shutil.rmtree(answers_dir)

    list_of_names = [file_name.split('/')[-1] for file_name, file_type in files.items()]

    with open(copy_log, 'a') as f:
        for file_path, file_type in files.items():
            f.write(file_path.split('/')[-1] + '\n')

    try:
        container_client = blob_service_client.get_container_client("metadata")
        blob_client = container_client.get_blob_client(copy_log)

        with open(copy_log, "r") as local_file:
            # if not blob_client.exists():
            blob_client.upload_blob(local_file.read(), overwrite=True)

    except Exception as e:
            print(f"Error opening, appending or creating copy log in metadata container: {e}")
            raise SystemExit(e)


"""
Uses the snowflake python connector to create a snowflake stage and then links it to the azure storage account

"""

def create_snowflake_stage(stage_name, snowflake_acc, snowflake_user, snowflake_password, snowflake_db, snowflake_schema, snowflake_wh, azure_container_name, azure_storage_acc, azure_sas_token):

    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_acc,
        warehouse=snowflake_wh,
        database=snowflake_db,
        schema=snowflake_schema
    )

    create_stage_sql = f"""CREATE OR REPLACE STAGE {stage_name} URL = 'azure://{azure_storage_acc}.blob.core.windows.net/{azure_container_name}'
    CREDENTIALS = (
        AZURE_SAS_TOKEN='{azure_sas_token}'
    );"""

    conn.cursor().execute(create_stage_sql)

    conn.close()


"""
Uses the snowflake python connector to copy data from an external stage into a table in snowflake

"""

    
def copy_into_snowflake_table(copy_log, file_format_name, snowflake_user, snowflake_password, snowflake_acc, snowflake_wh, snowflake_db, snowflake_schema):

    conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_acc,
            warehouse=snowflake_wh,
            database=snowflake_db,
            schema=snowflake_schema
        )


    try:
        conn.cursor().execute("""TRUNCATE TABLE pfe2024.stackoverflow.temp_questions;""")
        conn.cursor().execute("""TRUNCATE TABLE pfe2024.stackoverflow.temp_answers;""")

        with open(copy_log, "r") as f:
            for file_path in f:

                if 'questions' in file_path:
                    table_name = 'pfe2024.stackoverflow.temp_questions'
                    stage_name = 'azure_questions_stage'
                else:
                    table_name = 'pfe2024.stackoverflow.temp_answers'
                    stage_name = 'azure_answers_stage'     


                copy_into_sql = f"""COPY INTO {table_name} FROM @{stage_name}/{file_path} ON_ERROR=CONTINUE FILE_FORMAT = (FORMAT_NAME = {file_format_name});"""

                conn.cursor().execute(copy_into_sql)

    except Exception as e:
        conn.close()
        print("Error while copying data into snowflake tables, terminating the process..")
        raise SystemExit(e)
    
    finally:
        conn.close()

    os.remove(copy_log)

  
"""
- Input : 
    - Snowflake account credentials

- Process : 
    - Uses the credentials to connect to Snowflake account
    - Uses the python connector to insert data from the stg_question_answer table (staging table) into the final table (question_answer)
    - Calls the EMBED_TEXT_768 function inside the query on the concatenation of question_body and answer_body fields to fill the vector column

"""

def fill_final_table(snowflake_user, snowflake_password, snowflake_acc, snowflake_wh, snowflake_db, snowflake_schema):
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_acc,
        warehouse=snowflake_wh,
        database=snowflake_db,
        schema=snowflake_schema
    )
    
    try:
        cursor = conn.cursor()

        update_query = f"""
        INSERT INTO question_answer (TAG_LIST, QUESTION_ID, ANSWER_COUNT, SCORE, CREATION_DATE, TITLE, QUESTION_BODY, ANSWER_ID, ANSWER_BODY,
	    QUESTION_ANSWER_EMBEDDING)
        SELECT
            tag_list,
            question_id,
            answer_count,
            score,
            creation_date,
            title,
            question_body,
            answer_id,
            answer_body,
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', CONCAT(question_body, ' ', answer_body)) AS question_answer_embedding
        FROM stg_question_answer;
        """
        
        cursor.execute(update_query)
        
        conn.commit()
        
        print("Data Moved to the final table successfully.")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        cursor.close()
        conn.close()
