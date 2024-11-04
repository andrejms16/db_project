from utils import print_menu
from database import *

q0 = """
SELECT
	YEAR_NUMBER AS YEAR,
	COUNT(*) AS NUMBER_OF_FIRES
FROM
	FIRES.FIRE
GROUP BY
	YEAR_NUMBER
"""

q1 = """
SELECT
	AREA_TYPE.DESCRIPTION AS AREA_TYPE,
	round(AVG(BURNED_AREA.BURNED_AREA), 2) AS AVERAGE_BURNED_AREA
FROM
	BURNED_AREA
	JOIN AREA_TYPE ON BURNED_AREA.AREA_TYPE_ID = AREA_TYPE.ID
GROUP BY
	AREA_TYPE.DESCRIPTION
"""

q2 = """
SELECT
	DISTRICT.NAME AS DISTRICT,
	COUNT(*) AS NUMBER_OF_WILDFIRES
FROM
	FIRE
	JOIN NEIGHBORHOOD ON FIRE.NEIGHBORHOOD_ID = NEIGHBORHOOD.ID
	JOIN MUNICIPALITY ON NEIGHBORHOOD.MUNICIPALITY_ID = MUNICIPALITY.ID
	JOIN DISTRICT ON MUNICIPALITY.DISTRICT_ID = DISTRICT.ID
GROUP BY
	DISTRICT.NAME
ORDER BY
	NUMBER_OF_WILDFIRES DESC
"""

q3 = """
SELECT
	FIRE_STATION.ADDRESS AS FIRE_STATION,
	COUNT(*) AS NUMBER_OF_VEHICLES
FROM
	VEHICLE
	JOIN FIRE_STATION ON VEHICLE.FIRESTATION_ID = FIRE_STATION.ID
GROUP BY
	FIRE_STATION.ADDRESS
ORDER BY
	NUMBER_OF_VEHICLES DESC
"""

q4 = """
SELECT
	DISTRICT.NAME AS DISTRICT,
	ROUND(
		AVG(
			EXTRACT(
				EPOCH
				FROM
					(EXTINCTION - ALERT_TIME)
			) / 3600
		),
		2
	) AS AVG_HOURS_TO_EXTINGUISH
FROM
	FIRE
	JOIN NEIGHBORHOOD ON FIRE.NEIGHBORHOOD_ID = NEIGHBORHOOD.ID
	JOIN MUNICIPALITY ON NEIGHBORHOOD.MUNICIPALITY_ID = MUNICIPALITY.ID
	JOIN DISTRICT ON MUNICIPALITY.DISTRICT_ID = DISTRICT.ID
GROUP BY
	DISTRICT.NAME
ORDER BY
	AVG_HOURS_TO_EXTINGUISH DESC
"""

q5 = """
SELECT
	CAUSE_GROUP.DESCRIPTION AS CAUSE_GROUP,
	COUNT(*) AS NUMBER_OF_WILDFIRES
FROM
	FIRE
	LEFT JOIN CAUSE ON FIRE.CAUSE_ID = CAUSE.CODCAUSA
	LEFT JOIN CAUSE_GROUP ON CAUSE.CAUSE_GROUP_ID = CAUSE_GROUP.ID
GROUP BY
	CAUSE_GROUP.DESCRIPTION
ORDER BY
	NUMBER_OF_WILDFIRES DESC
"""

q6 = """
SELECT
	FIRE.YEAR_NUMBER AS YEAR,
	FIRE.MONTH_NUMBER AS MONTH,
	ROUND(AVG(FIRE_RISK_INDEX.INDEX_VALUE), 2) AS AVG_FWI
FROM
	FIRE
	INNER JOIN FIRE_RISK_INDEX ON FIRE.ID = FIRE_RISK_INDEX.FIRE_ID
	JOIN CANADIAN_FIRE_INDEX ON FIRE_RISK_INDEX.CANADIAN_FIRE_INDEX_ID = CANADIAN_FIRE_INDEX.ID
WHERE
	CANADIAN_FIRE_INDEX.ACRONYM = 'FWI' and
	FIRE_RISK_INDEX.INDEX_VALUE is not null
GROUP BY
	FIRE.YEAR_NUMBER,
	FIRE.MONTH_NUMBER
"""

q7 = """
WITH
	RANKEDWILDFIRES AS (
		SELECT
			DISTRICT.NAME AS DISTRICT,
			CAUSE_GROUP.DESCRIPTION AS CAUSE_GROUP,
			COUNT(*) AS NUMBER_OF_WILDFIRES,
			RANK() OVER (
				PARTITION BY
					DISTRICT.NAME
				ORDER BY
					COUNT(*) DESC
			) AS RNK
		FROM
			FIRE
			LEFT JOIN CAUSE ON FIRE.CAUSE_ID = CAUSE.CODCAUSA
			LEFT JOIN CAUSE_GROUP ON CAUSE.CAUSE_GROUP_ID = CAUSE_GROUP.ID
			JOIN NEIGHBORHOOD ON FIRE.NEIGHBORHOOD_ID = NEIGHBORHOOD.ID
			JOIN MUNICIPALITY ON NEIGHBORHOOD.MUNICIPALITY_ID = MUNICIPALITY.ID
			JOIN DISTRICT ON MUNICIPALITY.DISTRICT_ID = DISTRICT.ID
		GROUP BY
			DISTRICT.NAME,
			CAUSE_GROUP.DESCRIPTION
	)
SELECT
	DISTRICT,
	CAUSE_GROUP,
	NUMBER_OF_WILDFIRES
FROM
	RANKEDWILDFIRES
WHERE
	RNK = 1;
"""

q8 = """
SELECT
	FIRE.*,
	BURNED_AREA.BURNED_AREA
FROM
	FIRE
	JOIN BURNED_AREA ON FIRE.ID = BURNED_AREA.FIRE_ID
	JOIN AREA_TYPE ON BURNED_AREA.AREA_TYPE_ID = AREA_TYPE.ID
WHERE
	AREA_TYPE.DESCRIPTION = 'TOT'
ORDER BY
	BURNED_AREA.BURNED_AREA DESC 
LIMIT
	10
"""

q9 = """
SELECT	
	CANADIAN_FIRE_INDEX.ACRONYM,
	FIRE_RISK_INDEX.INDEX_VALUE,
	FIRE.*
FROM
	FIRE
	INNER JOIN FIRE_RISK_INDEX ON FIRE.ID = FIRE_RISK_INDEX.FIRE_ID
	JOIN CANADIAN_FIRE_INDEX ON FIRE_RISK_INDEX.CANADIAN_FIRE_INDEX_ID = CANADIAN_FIRE_INDEX.ID
WHERE FIRE_RISK_INDEX.INDEX_VALUE IS NOT NULL
AND CANADIAN_FIRE_INDEX.ACRONYM = 'FWI'
ORDER BY
	FIRE_RISK_INDEX.INDEX_VALUE DESC
LIMIT
	10	
"""

def queries(db_instance):
    options = [
        {'index': 0, 'text': 'How many fires were recorded in a specific year?'},
        {'index': 1, 'text': 'What is the average burned area in fires of a specific area type?'},
        {'index': 2, 'text': 'Which districts have the highest number of recorded fires?'},
        {'index': 3, 'text': 'How many vehicles are assigned to each fire station?'},
        {'index': 4, 'text': 'What is the average time between the alert and the extinguishing of the fires?'},
        {'index': 5, 'text': 'How many fires were caused by a specific cause group?'},
        {'index': 6, 'text': 'What is the average fire risk index value for fires in a specific month?'},
        {'index': 7, 'text': 'What is the most common cause of fires in a specific district?'},
        {'index': 8, 'text': 'Which fires had the largest burned area?'},
        {'index': 9, 'text': 'Which are the top 10 fires with the highest FWI index recorded?'}
    ]
    selected = print_menu(options, 'queries menu', 'Please select one of the previous options')
    if selected == 0:
        db_instance.custom_query(q0, True)
        print("Press Enter to continue...")
        input()
    if selected == 1:
        db_instance.custom_query(q1, True)
        print("Press Enter to continue...")
        input()
    if selected == 2:
        db_instance.custom_query(q2, True)
        print("Press Enter to continue...")
        input()
    if selected == 3:
        db_instance.custom_query(q3, True)
        print("Press Enter to continue...")
        input()
    if selected == 4:
        db_instance.custom_query(q4, True)
        print("Press Enter to continue...")
        input()
    if selected == 5:
        db_instance.custom_query(q5, True)
        print("Press Enter to continue...")
        input()
    if selected == 6:
        db_instance.custom_query(q6, True)
        print("Press Enter to continue...")
        input()
    if selected == 7:
        db_instance.custom_query(q7, True)
        print("Press Enter to continue...")
        input()
    if selected == 8:
        db_instance.custom_query(q8, True)
        print("Press Enter to continue...")
        input()
    if selected == 9:
        db_instance.custom_query(q9, True)
        print("Press Enter to continue...")
        input()
    return None