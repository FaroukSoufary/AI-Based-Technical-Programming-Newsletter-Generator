CREATE OR REPLACE PROCEDURE PFE2024.STACKOVERFLOW.GENERATE_NEWSLETTER()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','requests')
HANDLER = 'generate_content'
EXECUTE AS CALLER
AS '
import snowflake.snowpark as snowpark
import requests


def get_top_questions(session):
    cmd = """
    SELECT QUESTION_BODY, ANSWER_BODY, QUESTION_ID
    FROM pfe2024.stackoverflow.stackoverflow_weekly
    ORDER BY VIEW_COUNT DESC
    LIMIT 5
    """
    df_top_questions = session.sql(cmd).to_pandas()
    return df_top_questions


def create_prompt(session):
    df_context = get_top_questions(session)

    context_length = len(df_context)
    prompt_context = ""
    question_ids = []

    for i in range(context_length):
        prompt_context += df_context._get_value(i, ''QUESTION_BODY'')
        prompt_context += "\\n"
        prompt_context += df_context._get_value(i, ''ANSWER_BODY'')
        question_ids.append(df_context._get_value(i, ''QUESTION_ID''))

    prompt_context = prompt_context.replace("''", "")

    prompt =  f"""
        Generate a StackOverflow Weekly Newsletter using the following structure:
        Start with this introduction:
        "Welcome to this issue of the StackOverflow Weekly Newsletter! This week, we explore some of the most engaging discussions in the programming community. Whether you''re a seasoned developer or just starting out, we hope you find these insights both informative and inspiring."
        
        Then, smoothly transition into the content by summarizing and reflecting on the top questions and answers of the week. Weave the following context into the body of the newsletter, blending them into a natural, long and detailed cohesive narrative:
        
        Context: {prompt_context}
        
        Finally, conclude with the following closing remark:
        "That''s all for this week''s newsletter! Stay tuned for next week''s edition, where we''ll continue to share more knowledge from the world of StackOverflow."
        
        Avoid using explicit section headers like "Introduction," "Content," or "Conclusion." Ensure the content flows naturally as a single, unified piece of writing.

        """
    
    return prompt, question_ids



def complete(session):
    prompt, question_ids = create_prompt(session)
    cmd = f"""
    select SNOWFLAKE.CORTEX.COMPLETE(?,?) as response
    """
    df_response = session.sql(cmd, params=[''llama3-70b'', prompt]).collect()
    return df_response, question_ids
    

def generate_content(session: snowpark.Session) -> str:

    df_response, question_ids = complete(session)
    model_name = ''llama3-70b''
    query = "Weekly newsletter"
    res_text = df_response[0].RESPONSE
    session.sql(f"""
        INSERT INTO pfe2024.stackoverflow.newsletters (query, model, creation_date, newsletter_body)
        VALUES (?, ?, CURRENT_TIMESTAMP, ?);
        """, params=[query, model_name, res_text]).collect()

    return res_text
';
