use database pfe2024;
use schema stackoverflow;
use warehouse compute_wh;

CREATE OR REPLACE FILE FORMAT stackexchange_ff
TYPE=CSV
SKIP_HEADER=1
FIELD_DELIMITER=','
TRIM_SPACE=FALSE
FIELD_OPTIONALLY_ENCLOSED_BY='"'
REPLACE_INVALID_CHARACTERS=FALSE
DATE_FORMAT=AUTO
TIME_FORMAT=AUTO
TIMESTAMP_FORMAT=AUTO
error_on_column_count_mismatch=false;
