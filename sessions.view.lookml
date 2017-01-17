- view: sessions
  derived_table:
    sql_trigger_value: SELECT DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))
    distkey: user_id
    sortkeys: [session_start]
    sql: |
      WITH find_idle_time AS
        (SELECT
                  events.created_at
                , events.user_id
                , DATEDIFF(
                    minute, 
                    LAG(events.created_at) OVER ( PARTITION BY events.user_id ORDER BY events.created_at)
                  , events.created_at) AS idle_time
              FROM events
              -- optional limit of events table to only past 30 days
              WHERE ((events.created_at) >= (DATEADD(day,-29, DATE_TRUNC('day',GETDATE()) )) 
                    AND (events.created_at) < (DATEADD(day,30, DATEADD(day,-29, DATE_TRUNC('day',GETDATE()) ) ))) 
              )
        SELECT
          find_idle_time.created_at AS session_start
          , find_idle_time.idle_time AS idle_time
          , find_idle_time.user_id AS user_id
          , ROW_NUMBER () OVER (ORDER BY find_idle_time.created_at) AS unique_session_id
          , ROW_NUMBER () OVER (PARTITION BY COALESCE(find_idle_time.user_id) ORDER BY find_idle_time.created_at) AS session_sequence
          , COALESCE(
                LEAD(find_idle_time.created_at) OVER (PARTITION BY find_idle_time.user_id ORDER BY find_idle_time.created_at)
              , '3000-01-01') AS next_session_start
        FROM find_idle_time
        -- set session thresholds (currently set at 30 minutes) 
        WHERE (find_idle_time.idle_time > 30 OR find_idle_time.idle_time IS NULL)


  fields:
  
  - measure: count
    type: count
    drill_fields: detail*

  - dimension_group: session_start
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.session_start

  - dimension: idle_time
    type: number
    value_format: '0'
    sql: ${TABLE}.idle_time

  - dimension: user_id
    type: number
    sql: ${TABLE}.user_id

  - dimension: unique_session_id
    type: number
    primary_key: true
    sql: ${TABLE}.unique_session_id

  - dimension: session_sequence
    type: number
    sql: ${TABLE}.session_sequence

  - dimension_group: next_session_start
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.next_session_start

  - measure: count_distinct_sessions
    type: count_distinct
    sql: ${unique_session_id}
  
  sets:
    detail:
      - session_start_time
      - idle_time_time
      - user_id
      - unique_session_id
      - session_sequence
      - next_session_start_time