import sqlite3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import requests, json
from bs4 import BeautifulSoup
import re

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains

import pyperclip
import datetime as datetime




import json
from pprint import pprint


### db constructions 
connection_str = 'databases/test_database'
if True:
    conn = sqlite3.connect(connection_str)
    c = conn.cursor()
    query_str = '''
    CREATE TABLE IF NOT EXISTS test_uk_db_1 ([primary_key] INTEGER PRIMARY KEY, search_term [string], 
        [description] string, [price] string, [vendor] string, [delivery_offering] INTEGER, [sale_offering] INTEGER, [special_offering] INTEGER,
        [in_store] INTEGER, [minimum_order] INTEGER, [compare_prices] INTEGER, [site_link] string, [review_number] string, [page_location] string, 
        [extra_info] string, [search_time] string, [parsed_in_group] REAL)
    '''
    c.execute(query_str)
    conn.commit()

### load food codes
with open(r'food\food_codes.json') as json_data:
    d = json.load(json_data)
    json_data.close()

### build main lookup dict

main_dict_list = []
for k, v in d.items():
    for item in v:
        main_dict_list.append({'target_component': k, 'search_component': item})
main_dict_df = pd.DataFrame(main_dict_list)

### parse function

def parse_collate_list(v):
    delivery_offering = 0
    sale_offering = 0
    special_offer = 0
    in_store = 0
    minimum_order = 0
    compare_prices = 0
    site_link = np.nan
    review_number = np.nan
    description = np.nan
    price = np.nan
    vendor = np.nan
    extra_info = np.nan
    if (len(v)>10 or len(v)<3):
        return {'description': description, 'price': price, 'vendor': vendor, 'delivery_offering': delivery_offering, 'sale_offering': sale_offering, 
                'special_offering': special_offer, 'in_store': in_store, 'minimum_order': minimum_order, 'compare_prices': compare_prices, 'site_link': site_link, 'review_number': review_number, "extra_info": extra_info}
    l_static = v.copy()
    for elem in l_static:
        if 'delivery' in elem.lower():
            delivery_offering = 1
            v.remove(elem)
        elif ('sale' in elem.lower() or 'drop' in elem.lower()):
            sale_offering = 1
            v.remove(elem)
        elif 'special offer' in elem.lower():
            special_offer = 1
            v.remove(elem)
        elif 'in store' in elem.lower():
            in_store = 1
            v.remove(elem)
        elif 'minimum order' in elem.lower():
            minimum_order = 1
            v.remove(elem)
        elif 'compare prices' in elem.lower():
            compare_prices = 1
            v.remove(elem)
        elif 'visit site of ' in elem.lower():
            site_link = elem.replace(". Visit site of ", "").replace("Visit site of ", "").replace(" in a new window", "")
            v.remove(elem)
        elif '(' in elem and ')' in elem:
            try:
                review_number = float(elem.replace('(', '').replace(')', ''))
            except:
                  pass
        elif '£' in elem:
            price = (elem.replace('£', 'XXX', 1).split('£', 1)[0]).replace('XXX', '£')
            v.remove(elem)
        else:
            pass
    try:
        description = v[0]
        vendor = v[1]
    except:
        print('Error', v)
    if len(v)>2:
        extra_info = ', '.join([str(i) for i in v[2:]])
    
    return {'description': description, 'price': price, 'vendor': vendor, 'delivery_offering': delivery_offering, 'sale_offering': sale_offering, 
            'special_offering': special_offer, 'in_store': in_store, 'minimum_order': minimum_order, 'compare_prices': compare_prices, 'site_link': site_link, 'review_number': review_number, 'extra_info': extra_info}
    
def scraper_run():
    connection_str = 'databases/test_database'
    ### boot up scraper    
    driver = webdriver.Chrome( service = Service(ChromeDriverManager().install()))
    driver.get("https://shopping.google.com/")
    driver.find_element(By.NAME, 'q').send_keys("rice")
    driver.find_element(By.NAME, 'q').send_keys(Keys.RETURN)
    for i in [1, 2, 3, 4]:
        actions = ActionChains(driver)
        actions.send_keys(Keys.TAB)
        actions.perform()
    actions = ActionChains(driver)
    actions.send_keys(Keys.RETURN)
    actions.perform()
    
    ### for each item in search terms
    for idx, data in main_dict_df.iterrows():
        print('running for ', data['target_component'], data['search_component'])
        ## data contains two types - 'target_component', 'search_component'
        for i in range(30):
            driver.find_element(By.NAME, 'q').send_keys(Keys.BACKSPACE)
            
        driver.find_element(By.NAME, 'q').send_keys(data['search_component'])
        driver.find_element(By.NAME, 'q').send_keys(Keys.RETURN)
        actions = ActionChains(driver)
        actions.key_down(Keys.CONTROL)
        actions.send_keys("a")
        actions.key_up(Keys.CONTROL)
        actions.perform()
        actions.key_down(Keys.CONTROL)
        actions.send_keys("c")
        actions.key_up(Keys.CONTROL)
        actions.perform()
        page_text = pyperclip.paste()
        
        storage, collate_list = {}, []
        key = '1'
        idx = 1
        for i in page_text.split('\r\n'):
            if i == '':
                storage[str(idx)] = collate_list        
                idx+=1
                collate_list = []
            else:
                collate_list.append(i)

        parsing_run_payloads = {}
        top_ads = True
        bottom_ads = False
        for k, val in storage.items():   
            if 'about these results' in ' '.join(val).lower():
                top_ads = False
            if 'Ads·See' in ' '.join(val) and top_ads == False:
                bottom_ads = True
            data_for_storage = parse_collate_list(val)
            if top_ads:
                data_for_storage['page_location'] = 'top_ads'
            elif bottom_ads:
                data_for_storage['page_location'] = 'bottom_ads'
            else:
                data_for_storage['page_location'] = 'middle_results'        
            parsing_run_payloads[str(k)] = data_for_storage       
            
        full_run_results = pd.DataFrame(parsing_run_payloads)
        full_run_results_t = full_run_results.T.copy()
        full_run_results_t_dropped = full_run_results_t.dropna(subset = ['description', 'price', 'vendor']).fillna('NaN').copy()

        ### inserts 
        target = None
        target = data['target_component']
        search_time = None
        search_time = datetime.datetime.today().strftime('%Y-%m-%d %T')
        parsed_in_group = None
        parsed_in_group = len(full_run_results_t_dropped)
        
        
        for idx, row in full_run_results_t_dropped.iterrows():
            insert_data = row.to_dict()
            insert_data["search_term"] = target
            insert_data["search_time"] = search_time
            insert_data["parsed_in_group"] = parsed_in_group
            scrape_list = ['search_term', 'description', 'price', 'vendor', 'delivery_offering', 'sale_offering', 'special_offering', 'in_store', 
                           'minimum_order', 'compare_prices', 'site_link', 'review_number', 'page_location', 'extra_info', 'search_time', 'parsed_in_group']
            scrape_list_values = ', '.join(['"'+str(insert_data[ref]).replace('"', '')+'"' if ref not in ["delivery_offering", "sale_offering", "in_store", "minimum_order", "compare_prices", "special_offer"] else str(insert_data[ref]) for ref in scrape_list])
            addition_query_str = f"""INSERT INTO test_uk_db_1 ("{'", "'.join(scrape_list)}") VALUES ({scrape_list_values})"""
            print(addition_query_str)
            conn = sqlite3.connect(connection_str)
            c = conn.cursor()
            c.execute(addition_query_str)
            conn.commit()
            conn.close()

scraper_run()