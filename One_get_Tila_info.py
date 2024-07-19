import os
import json
import pandas as pd
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

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
    return competition_ids

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

# Main execution
session, auth_token = get_cookies_and_auth()
competition_ids = fetch_latest_results(session, auth_token)
results_df = parallel_fetch_competition_data(competition_ids, session, auth_token)

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

# Save DataFrame to Parquet and CSV
results_df.to_parquet('results_df.parquet')
results_df.to_csv('results_df.csv', index=False)

print(f"Total results fetched: {results_df.shape[0]}")
