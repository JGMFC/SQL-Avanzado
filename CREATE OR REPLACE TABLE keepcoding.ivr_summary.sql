CREATE OR REPLACE TABLE keepcoding.ivr_summary AS
WITH 
    documents
  AS (SELECT
          detail.calls_ivr_id as ivr_id
        , NULLIF(detail.document_type,"NULL") AS document_type
        , NULLIF(detail.document_identification,"NULL") AS document_identification
        , NULLIF(detail.customer_phone, "NULL") AS customer_phone
        , NULLIF(detail.billing_account_id, "NULL") AS billing_account
      FROM keepcoding.ivr_detail detail
      GROUP BY 
          calls_ivr_id
        , document_type
        , document_identification
        , customer_phone
        , billing_account
      QUALIFY ROW_NUMBER() 
        OVER(
          PARTITION BY CAST(detail.calls_ivr_id AS STRING) 
          ORDER BY detail.calls_ivr_id,document_type DESC,document_identification DESC, customer_phone DESC, billing_account DESC) = 1
      )
  ,
    llamadas 
  AS (SELECT 
        ivr_id
        , LAG(start_date) OVER (PARTITION BY phone_number ORDER BY start_date) AS primera_llamada
        , LEAD(start_date) OVER (PARTITION BY phone_number ORDER BY start_date) AS segunda_llamada
      FROM keepcoding.ivr_calls)
  , 
  info_by_phone
  AS (SELECT 
          calls_ivr_id
        , count(*) AS info_by_phone
      FROM keepcoding.ivr_detail
      WHERE 
            step_name = "CUSTOMERINFOBYPHONE.TX"
        AND step_description_error = "NULL"
      GROUP BY calls_ivr_id,step_name,step_description_error)
  ,
  info_by_dni
  AS (SELECT
          calls_ivr_id
        , count(*) AS info_by_dni
      FROM keepcoding.ivr_detail
      WHERE 
            step_name = "CUSTOMERINFOBYDNI.TX"
        AND step_description_error = "NULL"
      GROUP BY calls_ivr_id,step_name,step_description_error)

SELECT 
    detail.calls_ivr_id
  , calls_phone_number
  , calls_ivr_result
  , CASE WHEN STARTS_WITH(calls_vdn_label, "ATC") THEN "FRONT"
         WHEN STARTS_WITH(calls_vdn_label, "TECH") THEN "TECH"
         WHEN calls_vdn_label = "ABSORPTION" THEN "ABSORPTION"
    ELSE "RESTO"
    END AS calls_vdn_aggregation
  , calls_start_date
  , calls_end_date
  , calls_total_duration
  , calls_customer_segment
  , calls_ivr_language
  , calls_steps_module
  , calls_module_aggregation
  , documents.document_type
  , documents.document_identification
  , documents.customer_phone
  , documents.billing_account
  , IF(CONTAINS_SUBSTR(calls_module_aggregation, "AVERIA_MASIVA"), 1, 0) AS masiva_lg
  , COALESCE(info_by_phone.info_by_phone, 0) as info_by_phone_lg
  , COALESCE(info_by_dni.info_by_dni, 0) as  info_by_dni_lg
  , IF(DATETIME_DIFF(detail.calls_start_date, llamadas.primera_llamada,HOUR)<24,1,0) AS repeated_phone_24H
  , IF(DATETIME_DIFF(llamadas.segunda_llamada,detail.calls_end_date,HOUR)<24,1,0) AS cause_recall_phone_24H
FROM keepcoding.ivr_detail detail
LEFT 
  JOIN keepcoding.ivr_steps steps
  ON steps.ivr_id = detail.calls_ivr_id
LEFT
  JOIN llamadas
  ON detail.calls_ivr_id = llamadas.ivr_id
LEFT
  JOIN documents
  ON detail.calls_ivr_id = documents.ivr_id
LEFT 
  JOIN info_by_phone
  ON info_by_phone.calls_ivr_id = detail.calls_ivr_id
LEFT 
  JOIN info_by_dni
  ON info_by_dni.calls_ivr_id = detail.calls_ivr_id
QUALIFY ROW_NUMBER() 
  OVER(
    PARTITION BY CAST(detail.calls_ivr_id AS STRING) 
    ORDER BY detail.calls_ivr_id,detail.calls_start_date DESC
  ) = 1
