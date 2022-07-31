import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sqlite3
from rightmove_webscraper import RightmoveData

#### DB FUNCTIONS
connection_str = r"C:\project_pricing\databases\rightmove"

if False:
    conn = sqlite3.connect(connection_str)
    c = conn.cursor()
    query_str = """
        CREATE TABLE IF NOT EXISTS rightmove_simple
        ([id_lookup] INTEGER PRIMARY KEY, [sale_type] string, [search_location] string,
        [search_url] string, [price] real, [type] string, [address] string, [url] string, [agent_url] string, 
        [postcode] string, [full_postcode] string, [number_of_bedrooms] real, [search_date] string)
    """
    c.execute(query_str)
    conn.commit()
    conn.close()
    

 

def insert_value_rightmove_simple(query_str):
    conn = sqlite3.connect(connection_str)
    c = conn.cursor()
    c.execute(query_str)
    conn.commit()
    conn.close()

def select_all_values():
    conn = sqlite3.connect(connection_str)
    c = conn.cursor()
    list_of_columns = ['primary_key'] + [
        'sale_type', 'search_location', 'search_url', 'price', 'type', 
        'address', 'url', 'agent_url', 'postcode', 'full_postcode', 
        'number_of_bedrooms', 'search_date'
    ]
    query_str = """select * from rightmove_simple"""
    data = pd.DataFrame(c.execute(query_str).fetchall(), columns = list_of_columns)
    conn.commit()
    conn.close()
    return data

#### STATIC CODES

london_borough = {
    "City of London": "5E61224",
    "Barking and Dagenham": "5E61400",
    "Barnet": "5E93929",
    "Bexley": "5E93932",
    "Brent": "5E93935",
    "Bromley": "5E93938",
    "Camden": "5E93941",
    "Croydon": "5E93944",
    "Ealing": "5E93947",
    "Enfield": "5E93950",
    "Greenwich": "5E61226",
    "Hackney": "5E93953",
    "Hammersmith and Fulham": "5E61407",
    "Haringey": "5E61227",
    "Harrow": "5E93956",
    "Havering": "5E61228",
    "Hillingdon": "5E93959",
    "Hounslow": "5E93962",
    "Islington": "5E93965",
    "Kensington and Chelsea": "5E61229",
    "Kingston upon Thames": "5E93968",
    "Lambeth": "5E93971",
    "Lewisham": "5E61413",
    "Merton": "5E61414",
    "Newham": "5E61231",
    "Redbridge": "5E61537",
    "Richmond upon Thames": "5E61415",
    "Southwark": "5E61518",
    "Sutton": "5E93974",
    "Tower Hamlets": "5E61417",
    "Waltham Forest": "5E61232",
    "Wandsworth": "5E93977",
    "Westminster": "5E93980"
}

 
main_cities_ex_london = {
    'birmingham':'5E162',
    'liverpool':'5E813',
    'sheffield':'5E1195',
    'leeds':'5E787',
    'manchester':'5E904',
    'bristol':'5E219',
    'coventry': '5E368',
    'leicester':'5E789',
    'bradford':'5E198',
    'nottingham':'5E1019',
    'newcastle':'5E984',
    'cardiff':'5E281',
    'swansea':'5E1305',
    'bangor':'5E98',
    'aberdeen':'5E4',
    'inverness':'5E687',
    'edinburgh':'5E475',
    'glasgow':'5E550'
}

 
towns_outside_top_40 = {
    'huddersfield': '5E664',
    'peterborough': '5E1061',
    'oxford': '5E1036',
    'slough': '5E1217',
    'york': '5E1498',
    'blackpool': '5E168',
    'dundee': '5E452',
    'cambridge': '5E274',
    'ipswich': '5E689',
    'birkenhead': '5E161',
    'telford': '5E1323',
    'gloucester': '5E556',
    'sale': '5E1163',
    'watford': '5E1408',
    'newport': '5E991',
    'solihull': '5E1220',
    'high_wycombe': '5E637',
    'gateshead': '5E544',
    'colchester': '5E347',
    'blackburn': '5E167',

}

 
def daily_rightmove_run():
    ''' build queries'''
    rent_terms_dict = {}
    for name, item in zip(['london_borough', 'main_city_general', 'towns_outside_top_40'], [london_borough, main_cities_ex_london, towns_outside_top_40]):
        for k, v in item.items():
            base_url = f"""https://www.rightmove.co.uk/property-to-rent/find.html?searchType=RENT&locationIdentifier=REGION%{v}"""
            rent_terms_dict[name+'_'+k] = base_url

    ''' query and store '''
    rm_storage = {}
    for k, v in rent_terms_dict.items():
        rm = None
        rm = RightmoveData(v)
        raw_rm_data = rm.get_results
        raw_rm_data['sale_type'] = 'rent'
        raw_rm_data['search_location'] = k
        raw_rm_data['search_url'] = v
        rm_storage[k] = raw_rm_data
    ''' unpack from storage and upload'''
    list_of_columns = [
        'sale_type', 'search_location', 'search_url', 'price', 'type', 
        'address', 'url', 'agent_url', 'postcode', 'full_postcode', 
        'number_of_bedrooms', 'search_date'
    ]
    uploader=0
    for k, v in rm_storage.items():
        v_filter = v.fillna('NaN')
        v_filter['search_date'] = v_filter['search_date'].dt.strftime('%Y-%m-%d %T')
        v_filter['address'] = [str(i) for i in v_filter['address']]
        v_filter['number_of_bedrooms'] = v_filter['number_bedrooms'].astype(float, errors = 'ignore')

        for idx, row in v_filter.iterrows():
            query_str = f"""INSERT INTO rightmove_simple ({', '.join(list_of_columns)}) VALUES """
            query_str += '(' + ', '.join(['"'+(row.to_dict()[c])+'"' if type((row.to_dict()[c])) == str else str((row.to_dict()[c]))  for c in list_of_columns]) + ')'

            try:
                insert_value_rightmove_simple(query_str)
            except:
                pass
            uploader+=1
    print('uploaded ', uploader, ' items to db')
    return 'success'


if True:
    daily_rightmove_run()