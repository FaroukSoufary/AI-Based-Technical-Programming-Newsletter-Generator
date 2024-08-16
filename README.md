
---

# AI-Based Technical Programming Newsletter Generator

### End-of-Study Internship for Engineer's Degree (Master) in Computer Science (AI Focus) with EVIDEN

## Project Overview

This project aims to create an AI-based technical programming newsletter generator, leveraging StackOverflow posts as the core content. The solution is divided into two main components:

1. **Data Ingestion from StackOverflow API using an ELT Pipeline**
   - **Technologies Used:** Python, SQL, Snowflake, dbt, Airflow, Azure

2. **Implementation of a Retrieval-Augmented Generation (RAG) System**
   - **Technologies Used:** Snowflake (Cortex AI service), Large Language Models (LLMs)

## Prerequisites

To reproduce this work, you will need the following:

- A Snowflake account in a region that offers Cortex AI service (please check Snowflake's website).
- An Azure Cloud Portal account.
- Apache Airflow.
- dbt (Data Build Tool).
- Python.
- A StackExchange API key (create an account on StackExchange and generate an API key from the settings section).
- Snowflake python connector
     ```bash
     pip install snowflake-connector-python
     ```

## Setup Instructions

### 1. Snowflake Setup

1. **Create a Snowflake Account:** Ensure your account is in a region that offers Cortex AI.
   
2. **Database and Schema Setup:**
   - Create a database (e.g., `pfe2024`).
   - Inside the database, create a schema (e.g., `stackoverflow`).

3. **Run SQL Scripts:**
   - Copy and paste the contents of the SQL files in the `Snowflake_ddl` folder into a SQL sheet on the Snowflake UI, and execute the queries.

4. **Azure Setup:**
   - Create a Resource Group and a Storage Account on Azure.
   - Inside the Storage Account, create two containers: `questions` and `answers`.

5. **Stage Setup:**
   - Copy the function from `stages.py` into a Python script on your machine.
   - Run the function twice, replacing `container_name` with `questions` and `answers` respectively. (Ensure you have an Azure SAS token, which can be generated from the Azure Portal under the storage account's "Shared access signature" section).


### 1-bis. Set Your Environment Variables

Before running the project, set the following environment variables:

```bash
export SNOWFLAKE_USER='username'
export SNOWFLAKE_PASSWORD='password'
export SNOWFLAKE_ACCOUNT='your_account'
export SNOWFLAKE_DB='your_database'
export SNOWFLAKE_SCHEMA='your_schema'
export SNOWFLAKE_WH='your_warehouse'
export STACKEXCHANGE_API_KEY='stackexchange_api_key'
export AZURE_ACC_KEY='azure_account_key'
export AZURE_ACC_NAME='azure_account_name'
```

### 2. dbt Model Setup

1. **Install dbt:**
   - Install dbt using pip:
     ```bash
     pip install dbt
     ```

2. **Start a New dbt Project:**
   - Run the following command to start a new project :
     ```bash
     dbt init your_dbt_project_name
     ```

3. **Configure Snowflake Connection:**
   - During setup, choose Snowflake as the connection type and input your credentials:
     ```bash
     dbt connection setup
     ```

4. **Copy dbt Files:**
   - Copy `schema.yml` and `sources.yml` into:
     ```
     your_dbt_project/models/your_dbt_project/
     ```
   - Copy `stg_question_answer.sql` into:
     ```
     your_dbt_project/models/your_dbt_project/marts
     ```

### 3. Airflow Setup

1. **Install Airflow:**
   - Install Airflow by following the official installation guide.

2. **Configure Airflow DAG:**
   - Copy the `weekly_stackoverflow.py` file into the following folder:
     ```
     airflow/dags/
     ```

3. **Initialize Airflow:**
   - Initialize the Airflow database:
     ```bash
     airflow db init
     ```
   - Start the Airflow webserver:
     ```bash
     airflow webserver
     ```
   - Start the Airflow scheduler:
     ```bash
     airflow scheduler
     ```

4. **Access Airflow:**
   - Open the Airflow web interface and find the DAG in the DAGs section.

### 4. Historical Data Pipeline

This pipeline ingests data from StackOverflow into Snowflake tables and duplicates it as CSV files in Azure containers for redundancy and backup.

1. **Control Tags:**
   - To control which questions are retrieved based on tags, modify `scheduler.json` by adding your tags as keys with a value of `"0"`. The pipeline updates these to `"1"` once all questions of a tag are retrieved.

2. **Update Parameters:**
   - Modify `parameters.json` by adding the path to your dbt model in the `"FIX ME"` field.

3. **Run the Pipeline:**
   - Run the pipeline with the following command:
     ```bash
     python3 pipeline.py [quota_limit]
     ```
   - The `quota_limit` parameter controls the StackOverflow API quota usage (e.g., setting it to 0 will use the entire quota).

4. **Adjust API Call Frequency:**
   - In `utils.py`, adjust the `time.sleep()` value in the `fetch_data` function to change the delay between API calls and avoid throttle violation errors.

## Notes

- This project is designed to be flexible, allowing you to adjust the API call frequency and the tags for data retrieval to suit your needs.
- Ensure that all environment variables and configurations are correctly set up before running the pipeline.

## 5. Streamlit Folder

This folder contains the implementation of the Retrieval-Augmented Generation (RAG) system using Streamlit.

### Steps:

1. **Create a Streamlit Project on Snowflake UI:**
   - Log in to your Snowflake account.
   - Navigate to the Streamlit section and create a new Streamlit project.
   - Click on **EDIT** to open the Streamlit app editor.

2. **Copy and Paste the RAG Code:**
   - Copy the content of the `RAG.py` file from this repository.
   - Paste it into the Streamlit app editor on Snowflake.

3. **Streamlit Interface:**
   - Once you save and run the app, you'll see an interface that allows you to choose the LLM and select the mode for generating the newsletter.

4. **Newsletter Modes:**
   - **Theme-based Mode:** 
     - If selected, the newsletter is generated using the RAG system based on the specified theme that was typed by the user on the interface.
     - The generated newsletter is displayed on the interface and stored in the `newsletters` table in Snowflake.
   
   - **News-based Mode:**
     - This mode generates the newsletter based on the posts from the last week on StackOverflow, loaded into the `stackoverflow_weekly` table via the Airflow DAG.
     - The newsletter is displayed on the interface and saved in the `newsletters` table.

## 6. Streamlit Public App

I created a public GitHub-based Streamlit app that displays the latest newsletter generated by the Airflow DAG. This DAG runs every Monday at 9 AM, and refreshing the Streamlit web page after that time will display the most recent newsletter. This approach ensures that the newsletter is accessible not only to users with access to the Snowflake Streamlit project but also to anyone on the web.

### Steps to Create Your Own Streamlit Public App:

1. **Sign in to Streamlit:**
   - Go to the [Streamlit website](https://streamlit.io/) and sign in using your GitHub account.

2. **Start a New App:**
   - After signing in, start a new app. You can use the codespace provided by GitHub in your browser or clone the repository locally.

3. **Copy the Public App Code:**
   - Copy the content of the `streamlit_app.py` file from this repository.
   - Paste it into the `streamlit_app.py` file of your Streamlit app.

4. **Configure Snowflake Credentials:**
   - To allow your Streamlit app to access the `newsletters` table in Snowflake, you'll need to provide your Snowflake credentials.
   - Add these credentials in the secrets section of the app's settings on the UI, using the TOML format. The `secrets.toml` file provided in this repository can serve as a template.

5. **Visualize the App:**
   - Once everything is set up, click on the app to visualize its web page. The latest newsletter will be displayed according to the weekly updates.


## Note 

- Make sure airflow is running every monday before 9 AM on your computer
- For an easier use, you can use an Azure Virtual Machine and program it to run every monday before 9 AM, run airflow db and scheduler and then shut down after minutes (be generous to make sure the machine shuts down after the DAG finishes running -if everything is set correctly, it shouldn't run for more than 5 minutes-)

---


## Developed by

**Farouk Soufary**  
*Final Year Computer Science Engineering Student @ ENSEIRB-MATMECA*  
*Data Science & AI Enthusiast*  
**Email:** farouksoufary@gmail.com

---
