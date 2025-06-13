CREATE TABLE IF NOT EXISTS TDP_PROD.STAGING.SENDGRID_EVENTS_RAW_cicd (
	ID VARCHAR(16777216) DEFAULT UUID_STRING(),
	JSON_CONTENT VARIANT,
	PROCESSED BOOLEAN DEFAULT FALSE,
	CREATED_AT_UTC TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	PROCESSED_AT_UTC TIMESTAMP_LTZ(9)
);;

CREATE TABLE IF NOT EXISTS TDP_PROD.bronze.sendgrid_events_flattened_cicd (
  id                 VARCHAR,
  email              VARCHAR,
  event              VARCHAR,
  ip                 VARCHAR,
  response           VARCHAR,
  tls                VARCHAR,
  url                VARCHAR,
  sg_content_type    VARCHAR,
  sg_event_id        VARCHAR,
  sg_machine_open    BOOLEAN,
  sg_message_id      VARCHAR,
  sg_template_id     VARCHAR,
  sg_template_name   VARCHAR,
  event_time         TIMESTAMP_LTZ,
  useragent          VARCHAR
);;

CREATE STREAM IF NOT EXISTS TDP_PROD.staging.sendgrid_raw_stream_cicd 
  ON TABLE TDP_PROD.staging.SENDGRID_EVENTS_RAW_cicd 
  APPEND_ONLY = TRUE;;

CREATE TASK IF NOT EXISTS TDP_PROD.staging.sendgrid_process_stream_task_cicd
  WAREHOUSE = DE_WAREHOUSE
  SCHEDULE = '1 Minute'
  AS
  BEGIN
    INSERT INTO TDP_PROD.bronze.sendgrid_events_flattened_cicd (
          id,
          email,
          event,
          ip,
          response,
          tls,
          url,
          sg_content_type,
          sg_event_id,
          sg_machine_open,
          sg_message_id,
          sg_template_id,
          sg_template_name,
          event_time,
          useragent
        )
        SELECT
          r.id                                           AS id,
          r.json_content:"email"::VARCHAR                     AS email,
          r.json_content:"event"::VARCHAR                     AS event,
          r.json_content:"ip"::VARCHAR                        AS ip,
          r.json_content:"response"::VARCHAR                  AS response,
          r.json_content:"tls"::VARCHAR                       AS tls,
          r.json_content:"url"::VARCHAR                       AS url,
          r.json_content:"sg_content_type"::VARCHAR           AS sg_content_type,
          r.json_content:"sg_event_id"::VARCHAR               AS sg_event_id,
          r.json_content:"sg_machine_open"::BOOLEAN           AS sg_machine_open,
          r.json_content:"sg_message_id"::VARCHAR             AS sg_message_id,
          r.json_content:"sg_template_id"::VARCHAR            AS sg_template_id,
          r.json_content:"sg_template_name"::VARCHAR          AS sg_template_name,
          TO_TIMESTAMP_LTZ(r.json_content:"timestamp"::NUMBER) AS event_time,
          r.json_content:"useragent"::VARCHAR                 AS useragent
        FROM TDP_PROD.staging.sendgrid_raw_stream_cicd AS r
        WHERE METADATA$ACTION = 'INSERT';
        
        RETURN 'Inserted ' || SQLROWCOUNT || ' rows';
      END;;

ALTER TASK TDP_PROD.staging.sendgrid_process_stream_task_cicd RESUME;;


