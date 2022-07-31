

import datetime
import schedule
import time 
from rightmove_scrape import daily_rightmove_run

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sqlite3

from rightmove_webscraper import RightmoveData

def job():
	daily_rightmove_run()
	return 'success'

schedule.every().monday().at('05:30').do(job)
schedule.every().tuesday().at('05:30').do(job)
schedule.every().wednesday().at('05:30').do(job)
schedule.every().thursday().at('05:30').do(job)
schedule.every().friday().at('05:30').do(job)
schedule.every().saturday().at('05:30').do(job)
schedule.every().sunday().at('05:30').do(job)

while True:
	schedule.run_pending()
	time.sleep(1)