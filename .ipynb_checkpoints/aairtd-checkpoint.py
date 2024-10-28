import pandas as pd

# Function to pull ICAO call sign
def call_sign(file_path):
    icao_code = pd.read_csv(file_path).iloc[0, 0]
    return f"K{icao_code}"

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


# Function to iterate over dataset and write all results to a CSV
def write_all_results_to_csv(dataset, output_path):
    """Iterate over the dataset and write ICAO codes and aairtd to a CSV file."""
    results = []

    for file_path, time_zone in dataset.items():
        icao_code = call_sign(file_path)
        aairtd = average_annual_ideal_run_temp_days(file_path, time_zone)
        
        # Append the result as a dictionary
        results.append({"icao_code": icao_code, "aairtd": aairtd})
    
    # Convert results to a DataFrame and write to CSV
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False)