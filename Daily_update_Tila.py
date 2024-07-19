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

# Part 1: Logs into Tilastopaja and gets cookies and auth code
def get_cookies_and_auth():
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
    
    return session, auth_token

# Part 2: Fetches the first page of the latest results and saves to a JSON file
def fetch_latest_results(session, auth_token):
    url = 'https://www.tilastopaja.info/api/competitions/all'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Authorization': auth_token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
    }
    
    response = session.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    with open('update_page.json', 'w') as f:
        json.dump(data, f, indent=4)
    
    competition_ids = [comp['competitionId'] for div in data.get('divs', []) for table in div.get('tables', []) for comp in table.get('body', [])]
    print(f"Fetched competition IDs: {competition_ids}")  # Debugging statement
    return competition_ids

# Part 3: Fetches detailed competition data for each ID
def fetch_competition_data(competition_id, session, auth_token):
    try:
        url = f'https://www.tilastopaja.info/api/results/{competition_id}'
        headers = {
            'Authorization': auth_token,
            'Referer': f'https://www.tilastopaja.info/beta/results/{competition_id}'
        }
        
        response = session.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        results = []
        if 'genders' in data:
            for gender in data['genders']:
                for agegroup in gender.get('agegroups', []):
                    for event in agegroup.get('events', []):
                        for round_ in event.get('rounds', []):
                            for heat in round_.get('heats', []):
                                for result in heat.get('results', []):
                                    result_data = {
                                        'competitionId': data.get('competitionId'),
                                        'competitionLong': data.get('competitionLong'),
                                        'startDate': data.get('startDate'),
                                        'endDate': data.get('endDate'),
                                        'venue': data.get('venue'),
                                        'venueCountry': data.get('venueCountry'),
                                        'venueCountryFull': data.get('venueCountryFull'),
                                        'stadion': data.get('stadion'),
                                        'ranking': data.get('ranking'),
                                        'competitionGroup': data.get('competitionGroup'),
                                        'gender': gender.get('title'),
                                        'agegroup': agegroup.get('title'),
                                        'event': event.get('title'),
                                        'round': round_.get('title'),
                                        'heat': heat.get('title'),
                                        'athleteId': result.get('athleteId'),
                                        'country': result.get('country'),
                                        'countryFull': result.get('countryFull'),
                                        'pos': result.get('pos'),
                                        'result': result.get('result'),
                                        'name': result.get('name'),
                                        'dateOfBirth': result.get('dateOfBirth'),
                                        'personalBest': result.get('personalBest')
                                    }
                                    results.append(result_data)
        return pd.DataFrame(results)
    except Exception as e:
        print(f"Error fetching data for competition ID {competition_id}: {e}")
        return pd.DataFrame()

# Part 4: Parallelizes the data fetching
def parallel_fetch_competition_data(competition_ids, session, auth_token):
    all_results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_competition_data, comp_id, session, auth_token): comp_id for comp_id in competition_ids}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching competition data"):
            try:
                result = future.result()
                if not result.empty:
                    all_results.append(result)
            except Exception as e:
                print(f"Error processing competition ID: {e}")
    if all_results:
        return pd.concat(all_results, ignore_index=True)
    else:
        return pd.DataFrame()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Main execution

session, auth_token = get_cookies_and_auth()
competition_ids = fetch_latest_results(session, auth_token)
print(f"Total competition IDs fetched: {len(competition_ids)}")  # Debugging statement
results_df = parallel_fetch_competition_data(competition_ids, session, auth_token)
print(f"Total results fetched: {results_df.shape[0]}")  # Debugging statement

load_dotenv()

# Now deleting and inserting.  I needed conn.commit()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection details
host = 'sportsdb-sports-database-for-web-scrapes.g.aivencloud.com'
port = 16439
user = 'avnadmin'
password = os.getenv('DB_PASSWORD')
database = 'defaultdb'
ca_cert_path = 'ca.pem'

logger.info(f"Connecting to database at {host}:{port}, database: {database}, user: {user}")

# Database engine setup
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', 
                       connect_args={'ssl': {'ca': ca_cert_path}})

# Rename columns
results_df = results_df.rename(columns={
    'competitionId': 'Competition_ID',
    'competitionLong': 'Competition',
    'startDate': 'Start_Date',
    'endDate': 'End_Date',
    'venue': 'Venue',
    'venueCountry': 'Venue_CountryCode',
    'venueCountryFull': 'Venue_Country',
    'stadion': 'Stadium',
    'ranking': 'Ranking',
    'competitionGroup': 'Competition_Group',
    'gender': 'Gender',
    'agegroup': 'Age_Group',
    'event': 'Event',
    'round': 'Round',
    'heat': 'Heat',
    'athleteId': 'Athlete_ID',
    'country': 'Athlete_CountryCode',
    'countryFull': 'Athlete_Country',
    'pos': 'Position',
    'result': 'Result',
    'name': 'Athlete_Name',
    'dateOfBirth': 'Date_of_Birth',
    'personalBest': 'Personal_Best'
})

# Filtering out invalid dates
results_df = results_df[(results_df['Start_Date'] != '0000-00-00') & (results_df['End_Date'] != '0000-00-00')]

# Parsing date columns
results_df['Start_Date'] = pd.to_datetime(results_df['Start_Date'], errors='coerce')
results_df['End_Date'] = pd.to_datetime(results_df['End_Date'], errors='coerce')
results_df['Date_of_Birth'] = pd.to_datetime(results_df['Date_of_Birth'], format='%d %b %y', errors='coerce')

min_date = results_df['Start_Date'].min()
logger.info(f"Minimum Start_Date in results_df: {min_date}")

min_date_str = min_date.strftime('%Y-%m-%d')
logger.info(f"Formatted min_date: {min_date_str}")

print("Now deleting from the database")

try:
    with engine.connect() as conn:
        # Turn off SQL safe updates
        conn.execute(text("SET SQL_SAFE_UPDATES = 0;"))

        # Get total rows before deletion
        total_rows_before = conn.execute(text("SELECT COUNT(*) FROM Tilastopija_results")).scalar()
        logger.info(f"Total rows in database before deletion: {total_rows_before}")

        # Delete rows with Start_Date >= min_date
        delete_query = text("DELETE FROM Tilastopija_results WHERE Start_Date >= :min_date")
        result = conn.execute(delete_query, {'min_date': min_date_str})
        rows_deleted = result.rowcount
        logger.info(f"Rows deleted from database: {rows_deleted}")

        # Commit the deletion
        conn.commit()

        # Turn on SQL safe updates
        conn.execute(text("SET SQL_SAFE_UPDATES = 1;"))

        # Get total rows after deletion
        total_rows_after = conn.execute(text("SELECT COUNT(*) FROM Tilastopija_results")).scalar()
        logger.info(f"Total rows in database after deletion: {total_rows_after}")

except SQLAlchemyError as e:
    logger.error(f"Error processing database operations: {e}")
finally:
    engine.dispose()

# Insertion

# Database engine setup for insertion

print("Now inserting into database")

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', 
                       connect_args={'ssl': {'ca': ca_cert_path}})

try:
    total_rows = len(results_df)
    chunk_size = 1000
    rows_inserted = 0

    with engine.connect() as conn:
        for start in range(0, total_rows, chunk_size):
            end = min(start + chunk_size, total_rows)
            batch = results_df.iloc[start:end]
            batch.to_sql(name='Tilastopija_results', con=conn, if_exists='append', index=False)
            logger.info(f"Rows {start + 1} to {end} inserted into database")
            rows_inserted += len(batch)

        # Commit the insertion
        conn.commit()

        total_rows_after_insertion = conn.execute(text("SELECT COUNT(*) FROM Tilastopija_results")).scalar()
        logger.info(f"Total rows in database after insertion: {total_rows_after_insertion}")

    logger.info(f"Total rows inserted into database: {rows_inserted}")
    rows_added = rows_inserted - rows_deleted
    logger.info(f"Total new rows added to database: {rows_added}")

except SQLAlchemyError as e:
    logger.error(f"Error processing database operations: {e}")
finally:
    engine.dispose()

# Remove temporary file if it exists
if os.path.exists('results_df.parquet'):
    os.remove('results_df.parquet')
