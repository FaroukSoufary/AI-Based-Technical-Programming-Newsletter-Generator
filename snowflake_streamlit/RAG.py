import streamlit as st
import time
from snowflake.snowpark.context import get_active_session
session = get_active_session()

import pandas as pd

pd.set_option("max_colwidth", None)
num_chunks = 5  # Num-chunks provided as context.

def create_prompt(myquestion, rag, df_context=None):
    if rag == 1:    
        if df_context is None:
            cmd = """
            with results as
            (SELECT DISTINCT
                QUESTION_ID,
                VECTOR_COSINE_SIMILARITY(question_answer.question_answer_embedding,
                        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', ?)) as similarity,
                question_body, answer_body
            from question_answer
            order by similarity desc
            limit ?)
            select question_body, answer_body, question_id from results 
            """
            df_context = session.sql(cmd, params=[myquestion, num_chunks]).to_pandas()
        
        context_length = len(df_context)
        prompt_context = ""
        question_ids = []

        for i in range(context_length):
            prompt_context += df_context._get_value(i, 'QUESTION_BODY')
            prompt_context += "\n"
            prompt_context += df_context._get_value(i, 'ANSWER_BODY')
            question_ids.append(df_context._get_value(i, 'QUESTION_ID'))

        prompt_context = prompt_context.replace("'", "")


        prompt = f"""
        Generate a StackOverflow Weekly Newsletter using the following structure:
        Start with this introduction:
        "Welcome to this issue of the StackOverflow Weekly Newsletter! This week, we explore some of the most engaging discussions in the programming community. Whether you're a seasoned developer or just starting out, we hope you find these insights both informative and inspiring."
        
        Then, smoothly transition into the content by summarizing and reflecting on the top questions and answers of the week. Weave the following context into the body of the newsletter, blending them into a natural, long and detailed cohesive narrative:
        
        Context: {prompt_context}
        
        Finally, conclude with the following closing remark:
        "That's all for this week's newsletter! Stay tuned for next week's edition, where we'll continue to share more knowledge from the world of StackOverflow."
        
        Avoid using explicit section headers like "Introduction," "Content," or "Conclusion." Ensure the content flows naturally as a single, unified piece of writing.

        """
    else:
        prompt = f"""
        'Topic:  
        {myquestion} 
        Newsletter: '
        """

    return prompt, question_ids


def get_top_questions():
    cmd = """
    SELECT QUESTION_BODY, ANSWER_BODY, QUESTION_ID
    FROM pfe2024.stackoverflow.stackoverflow_weekly
    ORDER BY VIEW_COUNT DESC
    LIMIT 5
    """
    df_top_questions = session.sql(cmd).to_pandas()
    return df_top_questions


def create_prompt2():
    df_context = get_top_questions()
    context_length = len(df_context)
    prompt_context = ""
    question_ids = []

    for i in range(context_length):
        prompt_context += df_context._get_value(i, 'QUESTION_BODY')
        prompt_context += "\n"
        prompt_context += df_context._get_value(i, 'ANSWER_BODY')
        question_ids.append(df_context._get_value(i, 'QUESTION_ID'))

    prompt_context = prompt_context.replace("'", "")


    prompt = f"""
        Generate a StackOverflow Weekly Newsletter using the following structure:
        Start with this introduction:
        "Welcome to this issue of the StackOverflow Weekly Newsletter! This week, we explore some of the most engaging discussions in the programming community of stackoverflow that were posted during the last week. Whether you're a seasoned developer or just starting out, we hope you find these insights both informative and inspiring."
        
        Then, smoothly transition into the content by summarizing and reflecting on the top questions and answers of the week. Weave the following context into the body of the newsletter, blending them into a natural, long and detailed cohesive narrative:
        
        Context: {prompt_context}
        
        Finally, conclude with the following closing remark:
        "That's all for this week's newsletter! Stay tuned for next week's edition, where we'll continue to share more knowledge from the world of StackOverflow."
        
        Avoid using explicit section headers like "Introduction," "Content," or "Conclusion." Ensure the content flows naturally as a single, unified piece of writing.

        """
    
    return prompt, question_ids
    

def complete(myquestion, model_name, mode, rag=1, df_context=None):
    if mode == "Theme-based":
        prompt, question_ids = create_prompt(myquestion, rag, df_context)
    else:
        prompt, question_ids = create_prompt2()
    cmd = f"""
    select SNOWFLAKE.CORTEX.COMPLETE(?,?) as response
    """
    df_response = session.sql(cmd, params=[model_name, prompt]).collect()
    return df_response, question_ids

def display_response(question, model, mode, rag=0, df_context=None):
    response, question_ids = complete(question, model, mode, rag, df_context)
    res_text = response[0].RESPONSE
    st.markdown(res_text)
    st.markdown("Relevant questions:")
    st.markdown(question_ids)
    return response



# Main code
st.title("StackOverflow based Newsletter Generator")
st.write("""Choose the mode of newsletter generation: either based on a specific theme or the latest top questions from StackOverflow.""")

generation_type = st.radio("Choose the type of newsletter generation:", ("Theme-based", "News-based"))

# Here you can choose what LLM to use. Please note that they will have different cost & performance
model = st.sidebar.selectbox('Select your model:',(
                                    'mixtral-8x7b',
                                    'snowflake-arctic',
                                    'mistral-large',
                                    'llama3-8b',
                                    'llama3-70b',
                                    'reka-flash',
                                     'mistral-7b',
                                     'llama2-70b-chat',
                                     'gemma-7b'))

response = None

if generation_type == "Theme-based":
    question = st.text_input("Enter the theme of the newsletter", placeholder="Ex: classification with data imbalance", label_visibility="collapsed")

    if question:
        response = display_response(question, model, mode="Theme-based", rag=1)
        session.sql(f"""
        INSERT INTO pfe2024.stackoverflow.newsletters (query, model, creation_date, newsletter_body)
        VALUES (?, ?, CURRENT_TIMESTAMP, ?);
        """, params=[question, model, response[0].RESPONSE]).collect()

elif generation_type == "News-based":
        
        response = display_response("Latest Top Questions", model, "News-based", rag=1)
        session.sql(f"""
        INSERT INTO pfe2024.stackoverflow.newsletters (query, model, creation_date, newsletter_body)
        VALUES (?, ?, CURRENT_TIMESTAMP, ?);
        """, params=["Latest Top Questions", model, response[0].RESPONSE]).collect()
