-- Скрипт написан для актуальной версии Clickhouse

WITH data_analysts_april AS (
	SELECT
		FL.user_id,
		MIN(FL.lesson_datetime) AS first_lesson
	FROM
		lesson_index_test AS LS
		JOIN finished_lesson_test AS FL
			ON FL.lesson_id = LS.lesson_id
	WHERE
		LS.profession_name = 'data-analyst'
	GROUP BY
		FL.user_id
	HAVING
		date_trunc('month', first_lesson) = '2020-04-01'
),

next_lessons_delta AS (
	SELECT
		DA.user_id,
		LS.profession_name,
		FL.lesson_id,
		FL.date_created AS lesson_datetime,
		leadInFrame(FL.date_created, 1) OVER (
			PARTITION BY FL.user_id
			ORDER BY FL.date_created
			ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
			) AS next_lesson_datetime,
		date_diff('second', lesson_datetime, next_lesson_datetime) AS delta_seconds
	FROM
		data_analysts_april AS DA
		JOIN finished_lesson_test AS FL
			ON FL.user_id = DA.user_id
		JOIN lesson_index_test AS LS
			ON LS.lesson_id = FL.lesson_id
	WHERE
		LS.profession_name = 'data-analyst'
)

SELECT
	delta_seconds,
	lesson_datetime,
	lesson_id,
	next_lesson_datetime,
	profession_name,
	user_id
FROM
	next_lessons_delta
WHERE
	delta_seconds >= 0
	AND delta_seconds < 5;