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

driver = webdriver.Chrome( service = Service(ChromeDriverManager().install()))




actions = ActionChains(driver)
actions.send_keys(Keys.RETURN)
actions.perform()

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






