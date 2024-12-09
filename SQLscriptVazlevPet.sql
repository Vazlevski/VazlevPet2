-- Этот CTE определяет блоки сообщений по идентификатору сделки и определяет тип и время предыдущего сообщения в каждом диалоге.
WITH message_sequence AS (
  SELECT 
        entity_id,
        created_by,
        -- Переводим время сообщения в формат UTC
        TO_TIMESTAMP(created_at) AT TIME ZONE 'UTC' AS message_utc_time, 
        type,
        -- Определяем тип предыдущего сообщения в диалоге, сортируя по времени
        LAG(type) OVER (PARTITION BY entity_id ORDER BY created_at) AS previous_message_type,
        -- Определяем время предыдущего сообщения
        LAG(TO_TIMESTAMP(created_at) AT TIME ZONE 'UTC') OVER (PARTITION BY entity_id ORDER BY created_at) AS previous_message_utc_time 
    FROM 
        test.chat_messages
),
-- Этот CTE отбирает исходящие сообщения из последовательности, где тип предыдущего сообщения отличается, определяя, когда ответ от менеджера произошел после сообщения клиента.
outgoing_messages AS (
  SELECT 
        entity_id,
        created_by,
        message_utc_time,
        type,
        previous_message_type,
        previous_message_utc_time AS client_message_time
    FROM 
        message_sequence
    WHERE 
        -- Оставляем только исходящие сообщения менеджера
        type = 'outgoing_chat_message' 
        AND (previous_message_type IS NULL OR previous_message_type != 'outgoing_chat_message')
),
-- Этот CTE корректирует время сообщений с учетом нерабочих часов. Рассчитывает скорректированное время с учетом установленного рабочего графика.
working_hours_adjustment AS (
SELECT  
        entity_id,
        created_by,
        type,
        -- Корректируем время сообщения клиента, чтобы оно соответствовало рабочим часам (начало в 09:30, если вне рабочего времени)
        CASE 
            WHEN (EXTRACT(HOUR FROM client_message_time) > 9) OR (EXTRACT(HOUR FROM client_message_time) = 9 AND EXTRACT(MINUTE FROM client_message_time) >= 30)
            THEN client_message_time
            ELSE date_trunc('day', client_message_time) + INTERVAL '9 hours 30 minutes'
        END AS adjusted_client_time,
         -- Аналогично корректируем время ответа менеджера, чтобы оно соответствовало рабочим часам
        CASE 
            WHEN (EXTRACT(HOUR FROM message_utc_time) > 9) OR (EXTRACT(HOUR FROM message_utc_time) = 9 AND EXTRACT(MINUTE FROM message_utc_time) >= 30)
            THEN message_utc_time
            ELSE date_trunc('day', message_utc_time) + INTERVAL '9 hours 30 minutes'
        END AS adjusted_manager_time
    FROM 
        outgoing_messages
    WHERE 
        -- Исключаем сообщения от клиентов (created_by != 0 означает, что это сообщение от менеджера)
        created_by != 0    
),
-- В этом CTE вычисляется время ответа в минутах между ответом менеджера и сообщением клиента, а также производится подсчет разницы в днях для учета нерабочего времени.
response_time_calculation AS (
SELECT  
        entity_id,
        created_by,
        adjusted_manager_time,
        adjusted_client_time,
        type,
        -- Вычисляем время ответа в минутах
        EXTRACT(EPOCH FROM(adjusted_manager_time - adjusted_client_time)) / 60 AS response_time_minutes,
        -- Вычисляем разницу в днях между отправкой клиентом и ответом менеджера
        EXTRACT(DAY FROM adjusted_manager_time) - EXTRACT(DAY FROM adjusted_client_time) AS day_difference
FROM 
        working_hours_adjustment
WHERE 
        adjusted_client_time IS NOT NULL
),
-- Рассчитываем среднее время ответа в минутах для каждого менеджера, учитывая нерабочее время, которое вычитается из общей разницы времени.
average_response_time AS (
SELECT  
        created_by,
        -- Вычисляем среднее время ответа в минутах, учитывая нерабочее время
        ROUND(AVG(CASE 
            WHEN day_difference = 0 THEN response_time_minutes
            ELSE response_time_minutes - (day_difference * 570) -- 570 минут, тк в рабочем дне = (9.5 * 60)
        END), 2) AS average_response_time
    FROM 
        response_time_calculation 
    GROUP BY 
        created_by
    ORDER BY 
        created_by
)
-- Итоговый вывод таблицы со средней скоростью ответа для каждого менеджера
SELECT 
        mgr.mop_id,
        mgr.name_mop,
        avg_rt.average_response_time
FROM 
        average_response_time AS avg_rt
    JOIN 
        test.managers AS mgr ON avg_rt.created_by = mgr.mop_id;