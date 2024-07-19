from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import json
import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv


options = webdriver.ChromeOptions()
driver = webdriver.Chrome(options=options)

driver.get('https://www.tilastopaja.info/beta/users/login.php')
driver.find_element(By.NAME, 'username').send_keys('aspiretf')
driver.find_element(By.NAME, 'password').send_keys('Qcus4rGA9RaK', Keys.RETURN)
driver.implicitly_wait(10)

driver.get('https://www.tilastopaja.info/api/competitions/latest?&major=true&country=world')

for request in driver.requests:
    if request.response and 'authorization' in request.headers:
        auth_token = request.headers['authorization']

cookies = driver.get_cookies()
driver.quit()

session = requests.Session()
for cookie in cookies:
    session.cookies.set(cookie['name'], cookie['value'])

