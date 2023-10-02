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
  , CASE WHEN SUM(IF((detail.step_name = 'CUSTOMERINFOBYPHONE.TX') AND (detail.step_description_error = 'NULL' ), 1, 0)) > 0 THEN 1 ELSE 0 END AS info_by_phone_lg
  , CASE WHEN SUM(IF((detail.step_name = 'CUSTOMERINFOBYDNI.TX') AND (detail.step_description_error = 'NULL' ), 1, 0)) > 0 THEN 1 ELSE 0 END AS info_by_dni_lg
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
  GROUP BY
    detail.calls_ivr_id
  , calls_phone_number
  , calls_ivr_result
  , calls_vdn_aggregation
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
  , llamadas.primera_llamada
  , llamadas.segunda_llamada;

