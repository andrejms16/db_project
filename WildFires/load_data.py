import pandas as pd
from tabulate import tabulate
from utils import *
from datetime import datetime, timedelta
import math

def load_wildfires(db_instance):
    df = pd.read_excel('WildFires\wildfires.xlsx')
    print(tabulate(df.head(10), headers='keys', tablefmt='psql'))
    deletedb(db_instance)
    load_alert_source(db_instance, df['FonteAlerta'].unique())
    load_cause_group(db_instance, df['GrupoCausa'].unique())
    load_rnap(db_instance, df['RNAP'].unique())
    load_rnmpf(db_instance, df['RNMPF'].unique())
    load_district(db_instance, df['Distrito'].unique())
    load_municipality(db_instance, df[['Distrito', 'Concelho']].drop_duplicates())
    load_neighborhood(db_instance, df[['Concelho', 'DTCCFR', 'Freguesia']].drop_duplicates())
    load_canadian_fire_index(db_instance)
    load_fire_station(db_instance)
    load_vehicle(db_instance)
    load_fire_fighter(db_instance)
    load_cause_type(db_instance, df['TipoCausa'].unique())
    load_cause(db_instance, df[['TipoCausa' , 'GrupoCausa', 'DescricaoCausa', 'CodCausa']].drop_duplicates())
    load_area_type(db_instance)
    load_fires(db_instance, df[['FonteAlerta' , 'RNMPF', 'RNAP', 'Freguesia', 'Concelho', 'Codigo_SGIF','Codigo_ANEPC','Ano','Mes','Dia','Local','X_Militar','Y_Militar','Longitude','Latitude','X_ETRS89','Y_ETRS89','CodCausa']].drop_duplicates())
    

def deletedb(db_instance):
    db_instance.delete('fires.fire')
    db_instance.delete('area_type')
    db_instance.delete('cause')
    db_instance.delete('cause_type')
    db_instance.delete('firefighter')
    db_instance.delete('vehicle')
    db_instance.delete('fire_station')
    db_instance.delete('canadian_fire_index')
    db_instance.delete('neighborhood')
    db_instance.delete('municipality')
    db_instance.delete('district')
    db_instance.delete('rnmpf')
    db_instance.delete('rnap')
    db_instance.delete('cause_group')
    db_instance.delete('alert_source')


def load_fires(db_instance, data):
    for index, row in data.iterrows():
        alert_source_id = db_instance.custom_query(f"""SELECT id
                                                   FROM fires.alert_source
                                                   WHERE description = '{text_standardize(row['FonteAlerta'])}'""",
                                                   False)
        rnmpf_id = db_instance.custom_query(f"""SELECT id
                                                   FROM fires.rnmpf
                                                   WHERE name = '{text_standardize(row['RNMPF'])}'""",
                                                   False)
        rnap_id  = db_instance.custom_query(f"""SELECT id
                                                   FROM fires.rnap
                                                   WHERE name = '{text_standardize(row['RNAP'])}'""",
                                                   False)
        
        neighborhood_id  = db_instance.custom_query(f"""SELECT fires.neighborhood.id
                                                   FROM fires.neighborhood 
                                                   INNER JOIN fires.municipality ON
                                                    fires.municipality.id = fires.neighborhood.municipality_id
                                                   WHERE fires.neighborhood.name  = '{text_standardize(row['Freguesia'])}' AND fires.municipality.name = '{text_standardize(row['Concelho'])}'""",
                                                   False)
        print('id_SGIF: ' + text_standardize(row['Codigo_SGIF']))
        #print('CodCausa: ' + str(int(row['CodCausa'])))
        db_instance.insert_data('fires.fire', {'id_SGIF': text_standardize(row['Codigo_SGIF']),
                                         'id_ANEPC': int(row['Codigo_ANEPC']),
                                         'year_number': int(row['Ano']),
                                         'month_number': int(row['Mes']),
                                         'day_number': int(row['Dia']),
                                         'alert_time': datetime.now() - timedelta(days=1),
                                         'first_intervention': datetime.now() - timedelta(days=1),
                                         'extinction': datetime.now(),
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
    print('Fires Inserted')



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
    db_instance.insert_data('fire_station', {'address': text_standardize('PORTO')})
    db_instance.insert_data('fire_station', {'address': text_standardize('AVEIRO')})
    db_instance.insert_data('fire_station', {'address': text_standardize('VISEU')})
    print('Fire Station Inserted')

def load_fire_fighter(db_instance):
    db_instance.insert_data('firefighter', {'name': text_standardize('firefighter 1'),
                                            'firestation_id': 1 })
    db_instance.insert_data('firefighter', {'name': text_standardize('firefighter 2'),
                                            'firestation_id': 2 })
    db_instance.insert_data('firefighter', {'name': text_standardize('firefighter 3'),
                                            'firestation_id': 3 })
    print('Fire Fighter Inserted')

def load_vehicle(db_instance):
    db_instance.insert_data('vehicle', {'car_registration': text_standardize('AA16AA'),
                                        'model': text_standardize('TGS'),
                                        'maker': text_standardize('MAN'),
                                        'firestation_id': 1 })
    db_instance.insert_data('vehicle', {'car_registration': text_standardize('4572XQ'),
                                        'model': text_standardize('TGS'),
                                        'maker': text_standardize('MAN'),
                                        'firestation_id': 2 })
    db_instance.insert_data('vehicle', {'car_registration': text_standardize('AA20AA'),
                                        'model': text_standardize('TGS'),
                                        'maker': text_standardize('MAN'),
                                        'firestation_id': 3 })
    print('Vehicle Inserted')

def load_canadian_fire_index(db_instance):
    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('DSR'),
                                                    'name': text_standardize('DSR') })
    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('FWI'),
                                                    'name': text_standardize('FWI') })
    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('ISI'),
                                                    'name': text_standardize('ISI') })
    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('DC'),
                                                    'name': text_standardize('DC') })
    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('DMC'),
                                                    'name': text_standardize('DMC') })  
    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('FFMC'),
                                                    'name': text_standardize('FFMC') })   
    db_instance.insert_data('canadian_fire_index', {'acronym': text_standardize('BUI'),
                                                    'name': text_standardize('BUI') })       
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
                                          'cause_type_id': int(cause_type_id.iloc[0,0]),                                          
                                          'cause_group_id': int(cause_group_id.iloc[0,0])})
    print('Cause Inserted')

def load_area_type(db_instance):
    db_instance.insert_data('area_type', {'description': text_standardize('Pov')})
    db_instance.insert_data('area_type', {'description': text_standardize('Mato')})
    db_instance.insert_data('area_type', {'description': text_standardize('Agric')})
    print('Area Type Inserted')  