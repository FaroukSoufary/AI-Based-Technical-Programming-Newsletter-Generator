-- models/question_answer.sql

{{ config(materialized='table') }}

SELECT
    q.tag_list,
    q.question_id,
    q.answer_count,
    q.score,
    q.creation_date,
    q.title,
    q.body AS question_body,
    a.answer_id,
    a.body AS answer_body
FROM
    temp_questions q
JOIN
    temp_answers a
ON
    q.question_id = a.question_id
