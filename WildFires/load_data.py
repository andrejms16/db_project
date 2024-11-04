import pandas as pd
import random
from tabulate import tabulate
from utils import *
from queries import *
from datetime import datetime, timedelta
import math


def load_wildfires(db_instance):
    try:
        # Load data from the Excel file into a DataFrame
        df = pd.read_excel('wildfires.xlsx')

        # Display the first 10 rows of the DataFrame for a quick preview
        print(tabulate(df.head(10), headers='keys', tablefmt='psql'))

        # Delete existing data in relevant tables
        delete_db(db_instance)

        # Load each category of data into the database, ensuring unique values where necessary
        load_alert_source(db_instance, df['FonteAlerta'].unique())
        load_cause_group(db_instance, df['GrupoCausa'].unique())
        load_rnap(db_instance, df['RNAP'].unique())
        load_rnmpf(db_instance, df['RNMPF'].unique())
        load_district(db_instance, df['Distrito'].unique())

        # Load municipality data, removing duplicate rows to avoid redundant entries
        load_municipality(db_instance, df[['Distrito', 'Concelho']].drop_duplicates())

        # Load neighborhood data, ensuring uniqueness based on multiple columns
        load_neighborhood(db_instance, df[['Concelho', 'DTCCFR', 'Freguesia']].drop_duplicates())

        # Load static data that might not require processing from the DataFrame
        load_canadian_fire_index(db_instance)
        load_fire_station(db_instance)
        load_vehicle(db_instance)
        load_fire_fighter(db_instance)

        # Load cause types and causes, removing duplicates for unique entries
        load_cause_type(db_instance, df['TipoCausa'].unique())
        load_cause(db_instance, df[['TipoCausa', 'GrupoCausa', 'DescricaoCausa', 'CodCausa']].drop_duplicates())

        # Load area types and finally, the main fires data
        load_area_type(db_instance)
        load_fires(db_instance, df)
    except Exception as e:
        print('Error while loading the excel file: ', e)

    return None


def delete_db(db_instance):
    # List of tables to be truncated
    tables = [
        'fires.fire', 'area_type', 'cause', 'cause_type', 'firefighter',
        'vehicle', 'fire_station', 'canadian_fire_index', 'neighborhood',
        'municipality', 'district', 'rnmpf', 'rnap', 'cause_group', 'alert_source'
    ]

    # Truncate each table in the list to remove all rows
    for table in tables:
        db_instance.truncate(table)

    # Print confirmation message
    print('Database deleted')


def load_fires(db_instance, data):
    for index, row in data.iterrows():
        # Fetch the alert source ID based on the standardized alert source description
        alert_source_id = db_instance.custom_query(
            f"""SELECT id FROM fires.alert_source 
                    WHERE description = '{text_standardize(row['FonteAlerta'])}'""",
            False
        )

        # Fetch the RNMPF ID based on the standardized RNMPF name
        rnmpf_id = db_instance.custom_query(
            f"""SELECT id FROM fires.rnmpf 
                    WHERE name = '{text_standardize(row['RNMPF'])}'""",
            False
        )

        # Fetch the RNAP ID based on the standardized RNAP name
        rnap_id = db_instance.custom_query(
            f"""SELECT id FROM fires.rnap 
                    WHERE name = '{text_standardize(row['RNAP'])}'""",
            False
        )

        # Fetch the neighborhood ID by joining on municipality name and neighborhood name
        neighborhood_id = db_instance.custom_query(
            f"""SELECT fires.neighborhood.id FROM fires.neighborhood 
                    INNER JOIN fires.municipality ON fires.municipality.id = fires.neighborhood.municipality_id
                    WHERE fires.neighborhood.name = '{text_standardize(row['Freguesia'])}' 
                    AND fires.municipality.name = '{text_standardize(row['Concelho'])}'""",
            False
        )

        # Debug output for the standardized SGIF code
        print('Inserting wildfire:', text_standardize(row['Codigo_SGIF']))

        # Insert fire incident data into the 'fires.fire' table
        db_instance.insert_data('fires.fire', {
            'id_SGIF': text_standardize(row['Codigo_SGIF']),
            'id_ANEPC': int(row['Codigo_ANEPC']),
            'year_number': int(row['Ano']),
            'month_number': int(row['Mes']),
            'day_number': int(row['Dia']),
            'alert_time': None if row['DataHoraAlerta'] is pd.NaT else row['DataHoraAlerta'],
            'first_intervention': None if row['DataHora_PrimeiraIntervencao'] is pd.NaT else row['DataHora_PrimeiraIntervencao'],
            'extinction': None if row['DataHora_Extincao'] is pd.NaT else row['DataHora_Extincao'],
            'address': str(row['Local']),
            'x_militar_position': float(row['X_Militar']),
            'y_militar_position': float(row['Y_Militar']),
            'latitude': float(row['Latitude']),
            'longitude': float(row['Longitude']),
            'x_etrs89': float(row['X_ETRS89']),
            'y_etrs89': float(row['Y_ETRS89']),
            'alert_source_id': int(alert_source_id.iloc[0, 0]),
            'cause_id': None if math.isnan(row['CodCausa']) else int(row['CodCausa']),
            'neighborhood_id': int(neighborhood_id.iloc[0, 0]),
            'rnmpf_id': int(rnmpf_id.iloc[0, 0]),
            'rnap_id': int(rnap_id.iloc[0, 0])
        })

        # Fetch the fire ID based on the SGIF code to use in related data loading
        fire = db_instance.custom_query(
            f"""SELECT id FROM fire WHERE id_sgif = '{text_standardize(row['Codigo_SGIF'])}'""",
            False
        ).iloc[0, 0]

        # Load Canadian fire index data related to the fire
        load_canadian_index(db_instance, fire, [
            {'index': 'DSR', 'value': None if math.isnan(row['DSR']) else float(row['DSR'])},
            {'index': 'FWI', 'value': None if math.isnan(row['FWI']) else float(row['FWI'])},
            {'index': 'ISI', 'value': None if math.isnan(row['ISI']) else float(row['ISI'])},
            {'index': 'DC', 'value': None if math.isnan(row['DC']) else float(row['DC'])},
            {'index': 'DMC', 'value': None if math.isnan(row['DMC']) else float(row['DMC'])},
            {'index': 'FFMC', 'value': None if math.isnan(row['FFMC']) else float(row['FFMC'])},
            {'index': 'BUI', 'value': None if math.isnan(row['BUI']) else float(row['BUI'])}
        ])

        # Load wildfire area data related to the fire
        load_wildfire_areas(db_instance, fire, [
            {'area': 'POV', 'value': None if math.isnan(row['AreaPov_ha']) else float(row['AreaPov_ha'])},
            {'area': 'MAT', 'value': None if math.isnan(row['AreaMato_ha']) else float(row['AreaMato_ha'])},
            {'area': 'AGR', 'value': None if math.isnan(row['AreaAgric_ha']) else float(row['AreaAgric_ha'])},
            {'area': 'TOT', 'value': None if math.isnan(row['AreaTotal_ha']) else float(row['AreaTotal_ha'])}
        ])

        # Load associated vehicles and firefighters related to the fire
        load_fires_vehicles(db_instance, fire)
        load_fires_firefighter(db_instance, fire)

        # Final confirmation message
    print('Fires Inserted')
    return None


def load_wildfire_areas(db_instance, fire, areas):
    for row in areas:
        area_id = db_instance.custom_query(f"""SELECT id FROM area_type WHERE description = '{row['area']}'""",
                                           False)
        db_instance.insert_data('burned_area', {'fire_id': int(fire),
                                                'area_type_id': int(area_id.iloc[0, 0]),
                                                'burned_area': row['value']
                                                })


def load_canadian_index(db_instance, fire, indexes):
    for row in indexes:
        index_id = db_instance.custom_query(f"""SELECT id FROM canadian_fire_index WHERE acronym = '{row['index']}'""",
                                            False)
        db_instance.insert_data('fire_risk_index', {'fire_id': int(fire),
                                                    'canadian_fire_index_id': int(index_id.iloc[0, 0]),
                                                    'index_value': row['value']
                                                    })


def load_fires_vehicles(db_instance, fire):
    random_vehicle = db_instance.custom_query("""SELECT id from vehicle""", False)
    random_number = random.randint(1, 5)
    for i in range(random_number):
        selected_vehicle = random_vehicle.sample(n=1)
        selected_vehicle_id = selected_vehicle['id'].item()
        db_instance.insert_data('fire_vehicle', {'vehicle_id': int(selected_vehicle_id),
                                                 'fire_id': int(fire)
                                                 })
        random_vehicle = random_vehicle.drop(selected_vehicle.index)


def load_fires_firefighter(db_instance, fire):
    random_firefighter = db_instance.custom_query("""SELECT id from firefighter""", False)
    random_number = random.randint(1, 5)
    for i in range(random_number):
        selected_firefighter = random_firefighter.sample(n=1)
        selected_firefighter_id = selected_firefighter['id'].item()
        db_instance.insert_data('fire_firefighter', {'firefighter_id': int(selected_firefighter_id),
                                                     'fire_id': int(fire)
                                                     })
        random_firefighter = random_firefighter.drop(selected_firefighter.index)


def load_neighborhood(db_instance, data):
    for index, row in data.iterrows():
        municipality_id = db_instance.custom_query(f"""SELECT id
                                                       FROM fires.municipality
                                                       WHERE name = '{text_standardize(row['Concelho'])}'""",
                                                   False)
        db_instance.insert_data('neighborhood', {'name': text_standardize(row['Freguesia']),
                                                 'municipality_id': int(municipality_id.iloc[0, 0]),
                                                 'freguesia_ine': int(row['DTCCFR'])})
    print('Neighborhood Inserted')


def load_municipality(db_instance, data):
    for index, row in data.iterrows():
        district_id = db_instance.custom_query(f"""SELECT id
                                                   FROM fires.district
                                                   WHERE name = '{text_standardize(row['Distrito'])}'""",
                                               False)
        db_instance.insert_data('municipality', {'name': text_standardize(row['Concelho']),
                                                 'district_id': int(district_id.iloc[0, 0])})
    print('Municipalities Inserted')


def load_district(db_instance, data):
    for item in data:
        db_instance.insert_data('district', {'name': text_standardize(item)})
    print('District Inserted')


def load_alert_source(db_instance, data):
    for item in data:
        db_instance.insert_data('alert_source', {'description': text_standardize(item)})
    print('Alert Source Inserted')


def load_rnap(db_instance, data):
    for item in data:
        db_instance.insert_data('rnap', {'name': text_standardize(item)})
    print('RNAP Inserted')


def load_rnmpf(db_instance, data):
    for item in data:
        db_instance.insert_data('rnmpf', {'name': text_standardize(item)})
    print('RNMPF Inserted')


def load_cause_group(db_instance, data):
    for item in data:
        db_instance.insert_data('cause_group', {'description': text_standardize(item)})
    print('Cause Group Inserted')


def load_cause_type(db_instance, data):
    for item in data:
        db_instance.insert_data('cause_type', {'description': text_standardize(item)})
    print('Cause Type Inserted')


def load_fire_station(db_instance):
    db_instance.insert_data('fire_station', {'address': text_standardize('123 Maple St, Toronto, ON')})
    db_instance.insert_data('fire_station', {'address': text_standardize('456 Oak St, Vancouver, BC')})
    db_instance.insert_data('fire_station', {'address': text_standardize('789 Pine St, Calgary, AB')})
    db_instance.insert_data('fire_station', {'address': text_standardize('101 Birch St, Montreal, QC')})
    db_instance.insert_data('fire_station', {'address': text_standardize('202 Cedar St, Ottawa, ON')})
    print('Fire Station Inserted')


def load_fire_fighter(db_instance):
    random_fire_station_id = db_instance.custom_query("""SELECT id from fires.fire_station""", False)
    names = [
        "Patricia Taylor", "James Smith", "Robert Johnson", "Linda Brown", "Barbara Jones", "Michael Davis",
        "William Miller", "Elizabeth Wilson", "David Anderson", "Jennifer Thomas", "Mary Lee", "John Martin",
        "Christopher White", "Daniel Harris", "Matthew Clark", "Susan Lewis", "Joseph Walker", "Jessica Robinson",
        "Sarah King", "George Allen", "Nancy Young", "Karen Hall", "Donald Wright", "Kimberly Scott",
        "Steven Green", "Helen Adams", "Mark Baker", "Lisa Gonzalez", "Paul Nelson", "Sandra Carter",
        "Charles Mitchell", "Donna Perez", "Thomas Roberts", "Emily Turner", "Margaret Phillips", "Jason Campbell",
        "Sharon Parker", "Larry Evans", "Betty Edwards", "Timothy Collins", "Dorothy Stewart", "Frank Sanchez",
        "Rebecca Morris", "Andrew Rogers", "Carol Reed", "Joshua Cook", "Martha Morgan", "Kenneth Bell",
        "Amy Murphy", "Angela Bailey", "Eric Cooper", "Melissa Rivera", "Scott Richardson", "Michelle Cox",
        "Brian Ward", "Laura Howard", "Edward Torres", "Cynthia Peterson", "Kevin Gray", "Maria Ramirez",
        "Linda Simmons", "Patrick Foster", "Rebecca Perry", "Sophia Brooks", "Tyler Ross", "Cheryl Powell",
        "Jason Long", "Sara Butler", "Samuel Bailey", "Rachel Brooks", "Harold Wood", "Virginia Rivera"
    ]
    for name in names:
        db_instance.insert_data('firefighter', {
            'name': text_standardize(name),
            'firestation_id': random_fire_station_id.sample(n=1)['id'].item()
        })
    print('Fire Fighter Inserted')


def load_vehicle(db_instance):
    random_fire_station_id = db_instance.custom_query("""SELECT id from fires.fire_station""", False)
    vehicles = [
        {'car_registration': 'AA11AA', 'model': 'TGM', 'maker': 'MAN'},
        {'car_registration': 'BB22BB', 'model': 'Scania P-series', 'maker': 'Scania'},
        {'car_registration': 'CC33CC', 'model': 'Actros', 'maker': 'Mercedes-Benz'},
        {'car_registration': 'DD44DD', 'model': 'LF 20', 'maker': 'MAN'},
        {'car_registration': 'EE55EE', 'model': 'CF Series', 'maker': 'DAF'},
        {'car_registration': 'FF66FF', 'model': 'Rosenbauer Commander', 'maker': 'Rosenbauer'},
        {'car_registration': 'GG77GG', 'model': 'K Series', 'maker': 'Scania'},
        {'car_registration': 'HH88HH', 'model': 'Ziegler Z-Class', 'maker': 'Ziegler'},
        {'car_registration': 'II99II', 'model': 'Hino 500', 'maker': 'Hino'},
        {'car_registration': 'JJ00JJ', 'model': 'Atego', 'maker': 'Mercedes-Benz'},
        {'car_registration': 'KK11KK', 'model': 'Volvo FL', 'maker': 'Volvo'},
        {'car_registration': 'LL22LL', 'model': 'MAN TGS', 'maker': 'MAN'},
        {'car_registration': 'MM33MM', 'model': 'Iveco Magirus', 'maker': 'Iveco'},
        {'car_registration': 'NN44NN', 'model': 'Firestar', 'maker': 'Dennis'},
        {'car_registration': 'OO55OO', 'model': 'Quint', 'maker': 'Pierce'},
        {'car_registration': 'PP66PP', 'model': 'Predator', 'maker': 'E-One'},
        {'car_registration': 'QQ77QQ', 'model': 'Command', 'maker': 'Rosenbauer'},
        {'car_registration': 'RR88RR', 'model': 'CF Tipper', 'maker': 'DAF'},
        {'car_registration': 'SS99SS', 'model': 'Scania L-series', 'maker': 'Scania'},
        {'car_registration': 'TT00TT', 'model': 'Commander', 'maker': 'Freightliner'}
    ]

    for vehicle in vehicles:
        db_instance.insert_data('vehicle', {
            'car_registration': text_standardize(vehicle['car_registration']),
            'model': text_standardize(vehicle['model']),
            'maker': text_standardize(vehicle['maker']),
            'firestation_id': random_fire_station_id.sample(n=1)['id'].item()
        })

    print('Vehicle Inserted')


def load_canadian_fire_index(db_instance):
    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('DSR'),
                                                    'name': text_standardize('Daily Severity Rating')})

    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('FFMC'),
                                                    'name': text_standardize('Fine Fuel Moisture Code')})

    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('DMC'),
                                                    'name': text_standardize('Duff Moisture Code')})

    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('DC'),
                                                    'name': text_standardize('Drought Code')})

    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('ISI'),
                                                    'name': text_standardize('Initial Spread Index')})

    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('BUI'),
                                                    'name': text_standardize('Build Up Index')})

    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('FWI'),
                                                    'name': text_standardize('Fire Weather Index')})
    print('Fire Fighter Inserted')


def load_cause(db_instance, data):
    for index, row in data.iterrows():
        if not math.isnan(row['CodCausa']):
            cause_type_id = db_instance.custom_query(f"""SELECT id 
                                                      FROM fires.cause_type 
                                                      WHERE description = '{text_standardize(row['TipoCausa'])}'""",
                                                     False)
            cause_group_id = db_instance.custom_query(f"""SELECT id 
                                                      FROM fires.cause_group 
                                                      WHERE description = '{text_standardize(row['GrupoCausa'])}'""",
                                                      False)
            db_instance.insert_data('cause', {'name': text_standardize(row['DescricaoCausa']),
                                              'codcausa': int(row['CodCausa']),
                                              'cause_type_id': int(cause_type_id.iloc[0, 0]),
                                              'cause_group_id': int(cause_group_id.iloc[0, 0])})
    print('Cause Inserted')


def load_area_type(db_instance):
    db_instance.insert_data('area_type', {'description': text_standardize('POV')})
    db_instance.insert_data('area_type', {'description': text_standardize('MAT')})
    db_instance.insert_data('area_type', {'description': text_standardize('AGR')})
    db_instance.insert_data('area_type', {'description': text_standardize('TOT')})
    print('Area Type Inserted')
