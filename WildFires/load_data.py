import pandas as pd
from tabulate import tabulate
from utils import *

def load_wildfires(db_instance):
    df = pd.read_excel('wildfires.xlsx')
    print(tabulate(df.head(10), headers='keys', tablefmt='psql'))
    # load_alert_source(db_instance, df['FonteAlerta'].unique())
    # load_cause_group(db_instance, df['GrupoCausa'].unique())
    # load_rnap(db_instance, df['RNAP'].unique())
    # load_rnmpf(db_instance, df['RNMPF'].unique())
    # load_district(db_instance, df['Distrito'].unique())
    # load_municipality(db_instance, df[['Distrito', 'Concelho']].drop_duplicates())
    # load_neighborhood(db_instance, df[['Concelho', 'Local', 'Freguesia']].drop_duplicates())


def load_fires(db_instance, data):
    for index, row in data.iterrows():

        row['Distrito']



def load_neighborhood(db_instance, data):
    for index, row in data.iterrows():
        municipality_id = db_instance.custom_query(f"""SELECT id
                                                       FROM fires.municipality
                                                       WHERE name = '{text_standardize(row['Concelho'])}'""",
                                                       False)
        db_instance.insert_data('neighborhood', {'name': text_standardize(row['Local']),
                                                 'municipality_id': int(municipality_id.iloc[0, 0]),
                                                 'freguesia_ine': text_standardize(row['Freguesia'])})
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
    for index, row in data.iterrows():
        cause_group_id = db_instance.custom_query(f"""SELECT id 
                                                      FROM fires.cause_group 
                                                      WHERE description = '{text_standardize(row['GrupoCausa'])}'""",
                                                  False)
        db_instance.insert_data('cause_type', {'description': text_standardize(row['DescricaoCausa']),
                                               'cause_group_id': int(cause_group_id.iloc[0,0])})
    print('Cause Type Inserted')




