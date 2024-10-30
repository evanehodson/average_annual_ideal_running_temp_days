import csv
import os
import requests
import pandas as pd
from datetime import datetime

def get_stations_and_timezones(csv_file):
    """
    Extract station names and timezone IDs from a CSV file by finding columns based on headers.
    
    Parameters:
    - csv_file: Path to the CSV file containing station data.
    
    Returns:
    - A dictionary where the key is the station ID (with the first letter removed)
      and the value is the timezone ID, with spaces replaced by underscores.
    """
    stations = {}
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read the header row
        
        # Locate the columns for station ID and timezone by header name
        station_index = headers.index("ICAO")
        tzid_index = headers.index("TZID")
        
        for row in reader:
            station_id = row[station_index][1:]  # Remove the first character of station ID
            tzid = row[tzid_index].replace(" ", "_")  # Standardize timezone format
            stations[station_id] = tzid
    return stations

def download_station_data(station_dict, station_data_output_dir, base_url=None):
    """
    Download data for each station from the specified or default base URL and save as CSV files.

    Parameters:
    - station_dict: Dictionary where keys are station IDs and values are timezones.
    - station_data_output_dir: Directory to save the CSV files.
    - base_url: (Optional) The base URL with a placeholder for the station ID. Defaults to IEM URL.
    
    Returns:
    - A dictionary with file paths as keys and timezones as values.
    """
    if base_url is None:
        base_url = (
            "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
            "station=STATION_ID&data=feel&data=p01m&year1=1993&month1=1&day1=1&year2=2023&month2=1&day2=1&"
            "tz=Etc%2FUTC&format=onlycomma&latlon=no&elev=no&missing=M&trace=T&direct=no&report_type=3"
        )

    os.makedirs(station_data_output_dir, exist_ok=True)
    station_data_dict = {}
    total_stations = len(station_dict)  # Total number of stations to download

    for index, (station_id, tzid) in enumerate(station_dict.items(), start=1):
        url = base_url.replace("STATION_ID", station_id)
        output_file = os.path.join(station_data_output_dir, f"{station_id}_data.csv")
        
        try:
            response = requests.get(url)
            response.raise_for_status()  # Check for request errors
            
            with open(output_file, "wb") as file:
                file.write(response.content)

            station_data_dict[output_file] = tzid
            
        except requests.RequestException as e:
            print(f"Failed to download data for station: {station_id}. Error: {e}")

        # Progress bar update
        progress = (index / total_stations) * 100
        bar_length = 50  # Length of the progress bar
        block = int(bar_length * progress / 100)
        progress_bar = (
            f"[Data Download] |" + '█' * block + '-' * (bar_length - block) +
            f'| {progress:.2f}% complete ({index}/{total_stations})'
        )
        print(progress_bar, end='\r')  # Use \r to overwrite the line
    
    print()  # Move to the next line after the progress bar
    return station_data_dict

# Load data from CSV with specified columns and handle missing values and dtypes
def load_data(file_path):
    """Load specific columns from a CSV file with defined dtypes."""
    # Specify columns and expected data types
    return pd.read_csv(
        file_path,
        usecols=["valid", "feel", "p01m"],
        dtype={"valid": "str", "feel": "float64", "p01m": "float64"},
        na_values=["M", "T"],  # Treat 'M' and 'T' as NaN for missing/trace values
        low_memory=False  # Avoid dtype warnings for large files
    )

# Convert time to the specific timezone
def convert_to_timezone(data, time_zone):
    """Convert datetime column to specified timezone and remove timezone information."""
    data["valid"] = pd.to_datetime(data["valid"], errors="coerce")
    data["valid"] = data["valid"].dt.tz_localize("UTC").dt.tz_convert(time_zone)
    data["valid"] = data["valid"].dt.tz_localize(None)
    return data

# Filter data for daylight hours (6 am - 6 pm)
def filter_daylight_hours(data):
    """Filter data for times between 6 am and 6 pm."""
    return data[(data["valid"].dt.hour >= 6) & (data["valid"].dt.hour < 18)]

# Filter for ideal running conditions
def filter_ideal_conditions(data):
    """Filter data for temperatures between 50 and 63.5 degrees and no precipitation."""
    return data[(data["feel"] >= 50) & (data["feel"] <= 63.5) & (data["p01m"] == 0.00)]

# Identify complete years in the dataset
def get_complete_years(data):
    """Identify years with data spanning from January to December."""
    years = data["valid"].dt.year.unique()
    complete_years = []
    
    for year in years:
        year_data = data[data["valid"].dt.year == year]
        if (year_data["valid"].dt.month.min() == 1) and (year_data["valid"].dt.month.max() == 12):
            complete_years.append(year)
            
    return complete_years

# Filter data to include only complete years
def filter_complete_years(data, complete_years):
    """Filter data to include only entries from complete years."""
    return data[data["valid"].dt.year.isin(complete_years)]

# Calculate average annual ideal run days
def calculate_average_ideal_days_per_year(filtered_data, complete_years):
    """Calculate the average number of ideal run days per year."""
    count_days = filtered_data["valid"].dt.date.nunique()
    num_years = len(complete_years)
    return round(count_days / num_years if num_years > 0 else 0)

# Main function to execute the full pipeline
def average_annual_ideal_run_days(file_path, time_zone):
    """Calculate the average annual ideal run days for the dataset."""
    data = load_data(file_path)
    data = convert_to_timezone(data, time_zone)
    data_daylight = filter_daylight_hours(data)
    filtered_data = filter_ideal_conditions(data_daylight)
    complete_years = get_complete_years(filtered_data)
    complete_data = filter_complete_years(filtered_data, complete_years)
    return calculate_average_ideal_days_per_year(complete_data, complete_years)

def write_all_results_to_csv(dataset, output_path=None):
    """Iterate over the dataset and write ICAO codes and average ideal run temp days (aairtd) to a CSV file."""
    # Set default output path with date and time if none is provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_path = f"results_{timestamp}.csv"
    
    results = []
    total_files = len(dataset)

    for index, (file_path, time_zone) in enumerate(dataset.items()):
        icao_code = "K" + os.path.basename(file_path).split("_")[0]  # Extract ICAO code and add "K" prefix
        aairtd = average_annual_ideal_run_days(file_path, time_zone)
        
        # Append the result as a dictionary
        results.append({"ICAO": icao_code, "aairtd": aairtd})

        # Progress bar update
        progress = (index + 1) / total_files * 100
        bar_length = 50  # Length of the progress bar
        block = int(bar_length * progress / 100)
        progress_bar = (
            f"[Calculating Results] |" + '█' * block + '-' * (bar_length - block) +
            f'| {progress:.2f}% complete ({index + 1}/{total_files})'
        )
        print(progress_bar, end='\r')  # Use \r to overwrite the line

    print()  # Move to the next line after the progress bar
    
    # Convert results to a DataFrame and write to CSV
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False)
    print(f"Results written to {output_path}")