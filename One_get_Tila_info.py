from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import subprocess
import json
import os
from tqdm import tqdm

# Step 1: Selenium setup for logging in and retrieving cookies
options = webdriver.ChromeOptions()
options.headless = False  # Set to True for headless mode
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

print("Logging in to retrieve cookies...")
driver = webdriver.Chrome(options=options)
driver.get('https://www.tilastopaja.info/beta/users/login.php')

# Wait for username field
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, 'username')))

# Input credentials
driver.find_element(By.NAME, 'username').send_keys('aspiretf')
driver.find_element(By.NAME, 'password').send_keys('Qcus4rGA9RaK', Keys.RETURN)
driver.implicitly_wait(10)

# Wait for cookies to be available after login
WebDriverWait(driver, 10).until(lambda d: len(d.get_cookies()) > 0)

# Extract cookies
cookies = driver.get_cookies()
cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}
driver.quit()

# Load the token from the CSV file
df = pd.read_csv('auth_token.csv')
auth_token = df['Authorization Token'].iloc[0]

print("Cookies and session established successfully.")

# Step 2: Function to make requests with `curl`
def fetch_with_curl(url, headers, cookies):
    # Construct the curl command
    command = [
        "curl", "-s", "-X", "GET", url,  # Silent mode, GET method
        "-H", f"Authorization: {headers['Authorization']}",
        "-H", f"Accept: {headers['Accept']}",
        "-H", f"User-Agent: {headers['User-Agent']}",
        "-H", f"Referer: {headers['Referer']}",
        "-H", f"Cookie: {'; '.join([f'{key}={value}' for key, value in cookies.items()])}"
    ]

    # Run the curl command and capture output
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error executing curl: {result.stderr}")
        return None

    return result.stdout

# Step 3: Fetch latest competition IDs
def fetch_competition_ids():
    url = "https://www.tilastopaja.info/api/competitions/all"
    headers = {
        "Authorization": auth_token,
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Referer": "https://www.tilastopaja.info/beta/"
    }

    response_text = fetch_with_curl(url, headers, cookie_dict)
    if not response_text:
        print("Failed to fetch competition IDs.")
        return []

    try:
        data = json.loads(response_text)
        with open("update_page.json", "w") as f:
            json.dump(data, f, indent=4)

        competition_ids = [
            comp["competitionId"]
            for div in data.get("divs", [])
            for table in div.get("tables", [])
            for comp in table.get("body", [])
        ]
        print(f"Fetched competition IDs: {competition_ids}")
        return competition_ids
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return []

# Step 4: Fetch competition data in parallel
def fetch_competition_data_parallel(competition_ids):
    def fetch_single_competition(competition_id):
        try:
            url = f"https://www.tilastopaja.info/api/results/{competition_id}"
            headers = {
                "Authorization": auth_token,
                "Accept": "application/json, text/plain, */*",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                "Referer": f"https://www.tilastopaja.info/beta/results/{competition_id}",
            }

            response_text = fetch_with_curl(url, headers, cookie_dict)
            if not response_text:
                print(f"Failed to fetch competition data for ID {competition_id}.")
                return pd.DataFrame()

            try:
                data = json.loads(response_text)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON for competition ID {competition_id}: {e}")
                return pd.DataFrame()

            # Initialize a list for results
            results = []
            genders = data.get("genders", [])
            for gender in genders:
                gender_title = gender.get("title", "")
                for agegroup in gender.get("agegroups", []):
                    agegroup_title = agegroup.get("title", "")
                    for event in agegroup.get("events", []):
                        event_title = event.get("title", "")
                        for round_ in event.get("rounds", []):
                            round_title = round_.get("title", "")
                            for heat in round_.get("heats", []):
                                heat_title = heat.get("title", "")
                                for result in heat.get("results", []):
                                    result_data = {
                                        "Competition_ID": data.get("competitionId", ""),
                                        "Competition": data.get("competitionLong", ""),
                                        "Start_Date": data.get("startDate", ""),
                                        "End_Date": data.get("endDate", ""),
                                        "Venue": data.get("venue", ""),
                                        "Venue_Country": data.get("venueCountryFull", ""),
                                        "Stadium": data.get("stadion", ""),
                                        "Gender": gender_title,
                                        "Age_Group": agegroup_title,
                                        "Event": event_title,
                                        "Round": round_title,
                                        "Heat": heat_title,
                                        "Athlete_ID": result.get("athleteId", ""),
                                        "Country": result.get("countryFull", ""),
                                        "Position": result.get("pos", ""),
                                        "Result": result.get("result", ""),
                                        "Name": result.get("name", ""),
                                        "Date_of_Birth": result.get("dateOfBirth", ""),
                                        "Personal_Best": result.get("personalBest", ""),
                                    }
                                    results.append(result_data)

            return pd.DataFrame(results)
        except Exception as e:
            print(f"Unexpected error fetching data for competition ID {competition_id}: {e}")
            return pd.DataFrame()

    # Use ThreadPoolExecutor for parallel fetching
    all_results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_single_competition, comp_id): comp_id for comp_id in competition_ids}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching competition data in parallel"):
            result = future.result()
            if not result.empty:
                all_results.append(result)

    return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

# Main script
competition_ids = fetch_competition_ids()
results_df = fetch_competition_data_parallel(competition_ids)

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

# Parsing date columns
results_df['Start_Date'] = pd.to_datetime(results_df['Start_Date'], errors='coerce')
results_df['End_Date'] = pd.to_datetime(results_df['End_Date'], errors='coerce')
results_df['Date_of_Birth'] = pd.to_datetime(results_df['Date_of_Birth'], format='%d %b %y', errors='coerce')

# Save DataFrame to Parquet and CSV
results_df.to_parquet('results_df.parquet')
results_df.to_csv('results_df.csv', index=False)

print(f"Total results fetched: {results_df.shape[0]}")
