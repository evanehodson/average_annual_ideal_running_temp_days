import pandas as pd
from datetime import datetime
import json
import requests
import os

def get_stations_from_networks():
    """Build a station list by using a bunch of IEM networks."""
    stations = []
    states = (
        "AK AL AR AZ CA CO CT DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN "
        "MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT "
        "WA WI WV WY"
    )
    networks = [f"{state}_ASOS" for state in states.split()]

    for network in networks:
        # Get metadata
        uri = f"https://mesonet.agron.iastate.edu/geojson/network/{network}.geojson"
        try:
            response = requests.get(uri)
            response.raise_for_status()  # Check for request errors
            jdict = response.json()
            
            # Extract station IDs
            for site in jdict["features"]:
                stations.append(site["properties"]["sid"])
                
        except requests.RequestException as e:
            print(f"Failed to retrieve data for network {network}: {e}")
    
    return stations

def download_station_data(station_list, output_dir, base_url=None):
    """
    Download data for each station from the specified or default base URL and save as CSV files.

    Parameters:
    - station_list: List of station IDs to download data for.
    - output_dir: Directory to save the CSV files.
    - base_url: (Optional) The base URL with a placeholder for the station ID. Defaults to IEM URL.
    """
    # Set a default base URL if none is provided
    if base_url is None:
        base_url = (
            "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
            "station=STATION_ID&data=feel&year1=1993&month1=1&day1=1&year2=2023&month2=1&day2=1&"
            "tz=Etc%2FUTC&format=onlycomma&latlon=no&elev=no&missing=M&trace=T&direct=no&report_type=3"
        )

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    for station in station_list:
        # Skip invalid entries
        if station is None:
            continue

        # Construct the URL for each station
        url = base_url.replace("STATION_ID", station)
        
        # Download and save the file
        try:
            response = requests.get(url)
            response.raise_for_status()  # Check for request errors
            
            # Define the output file path
            output_file = os.path.join(output_dir, f"{station}_data.csv")
            
            # Write to CSV file
            with open(output_file, "wb") as file:
                file.write(response.content)
                
            print(f"Downloaded data for station: {station}")
        
        except requests.RequestException as e:
            print(f"Failed to download data for station: {station}. Error: {e}")    

def get_file_paths(directory):
    """
    Get all file paths in a specified directory.

    Parameters:
    - directory: The path to the directory to search in.

    Returns:
    - A list of full file paths for all files in the directory.
    """
    dataset = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):  # Only add CSV files
                dataset.append(os.path.join(root, file))
    return dataset

# Function to pull ICAO call sign
def call_sign(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Check if the DataFrame is empty
    if df.empty:
        print(f"Warning: The file {file_path} is empty. Returning a default ICAO code.")
        return "UNKNOWN"  # Return a default or placeholder value if the file is empty

    # If not empty, extract the ICAO code
    icao_code = df.iloc[0, 0]

    # Ensure the ICAO code has the correct format
    if len(icao_code) < 4:
        return f"K{icao_code}"  # Add "K" if less than 4 characters
    elif len(icao_code) == 4:
        return icao_code  # Return as is if it's already 4 characters long
    else:
        print(f"Warning: The ICAO code {icao_code} is invalid. Returning default.")
        return "KUNKNOWN"  # Handle unexpected lengths

# Function to load data from CSV
def load_data(file_path):
    """Load specific columns from a CSV file."""
    # Assuming columns 1 and 2 are date and temp, respectively 
    return pd.read_csv(file_path, usecols=[1, 2])

# Function to preprocess data types
def preprocess_data(data):
    """Convert columns to appropriate data types."""
    data["valid"] = pd.to_datetime(data["valid"], errors="coerce")
    data["feel"] = pd.to_numeric(data["feel"], errors="coerce")
    return data


# Function to localize and convert time to a specific timezone
def convert_to_timezone(data, time_zone):
    """Convert datetime column to specified timezone and remove timezone information."""
    data["valid"] = data["valid"].dt.tz_localize("UTC").dt.tz_convert(time_zone)
    data["valid"] = data["valid"].dt.tz_localize(None)
    return data

# Function to filter data for daylight hours (6 am - 6 pm)
def filter_daylight_hours(data):
    """Filter data for times between 6 am and 6 pm."""
    return data[(data["valid"].dt.hour >= 6) & (data["valid"].dt.hour < 18)]

# Function to filter for ideal running temperatures
def filter_ideal_temps(data):
    """Filter data for temperatures between 50 and 63.5 degrees."""
    return data[(data["feel"] >= 50) & (data["feel"] <= 63.5)]

# Function to identify complete years in the dataset
def get_complete_years(data):
    """Identify years with data spanning from January to December."""
    years = data["valid"].dt.year.unique()
    complete_years = []
    
    for year in years:
        year_data = data[data["valid"].dt.year == year]
        if (year_data["valid"].dt.month.min() == 1) and (year_data["valid"].dt.month.max() == 12):
            complete_years.append(year)
            
    return complete_years

# Function to filter data to include only complete years
def filter_complete_years(data, complete_years):
    """Filter data to include only entries from complete years."""
    return data[data["valid"].dt.year.isin(complete_years)]

# Function to calculate the average annual ideal running temperature days
def calculate_average_ideal_days_per_year(filtered_data, complete_years):
    """Calculate the average number of ideal temperature days per year."""
    count_days = filtered_data["valid"].dt.date.nunique()
    num_years = len(complete_years)
    return count_days / num_years if num_years > 0 else 0

# Main function to execute the full pipeline
def average_annual_ideal_run_temp_days(file_path, time_zone):
    """Calculate the average annual ideal run temperature days for the dataset."""
    data = load_data(file_path)
    data = preprocess_data(data)
    data = convert_to_timezone(data, time_zone)
    data_daylight = filter_daylight_hours(data)
    filtered_data = filter_ideal_temps(data_daylight)
    complete_years = get_complete_years(filtered_data)
    complete_data = filter_complete_years(filtered_data, complete_years)
    return calculate_average_ideal_days_per_year(complete_data, complete_years)


def write_all_results_to_csv(dataset, output_path=None):
    """Iterate over the dataset and write ICAO codes and aairtd to a CSV file."""
    # Set default output path with date and time if none is provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_path = f"results_{timestamp}.csv"
    
    results = []

    for file_path, time_zone in dataset.items():
        icao_code = call_sign(file_path)
        aairtd = average_annual_ideal_run_temp_days(file_path, time_zone)
        
        # Append the result as a dictionary
        results.append({"ICAO": icao_code, "aairtd": aairtd})
    
    # Convert results to a DataFrame and write to CSV
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False)
    print(f"Results written to {output_path}")