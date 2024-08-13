import sys
import requests
import json
import pandas as pd
import logging
import matplotlib.pyplot as plt
import os

from functools import wraps
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

# Determine log file path
log_file_path = os.path.join(os.getcwd(), 'tsla_logger.txt')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',  # Custom date format without milliseconds
    handlers=[
        logging.FileHandler(log_file_path, mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Load environment variables from the .env file
load_dotenv()
API_KEY = os.getenv('API_KEY')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_TABLE_NAME = os.getenv('DB_TABLE_NAME')

BASE_URL = rf'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=TSLA&interval=30min&outputsize=full&apikey={API_KEY}'
# BASE_URL = rf'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TSLA&apikey={API_KEY}'
CONNECTION_STATUS = rf'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@localhost:{DB_PORT}/{DB_NAME}'

# Check if critical environment variables are missing
if not API_KEY or not DB_USER or not DB_PASSWORD or not DB_NAME:
    logger.error("Required environment variables are missing. Please check your .env file.")
    sys.exit(1)


def log_function_completion(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            logger.info(f"The {func.__name__} function has been executed successfully...")
            return result
        except Exception as e:
            logger.error(f"The {func.__name__} function failed to execute. Error: {e}")
            raise  # Re-raise the exception after logging it

    return wrapper


class TeslaDatabase:
    def __init__(self):
        self.df = None
        self.data = None
        self.time_series_key = 'Time Series (30min)'

    def graphing(self):
        # Load the DataFrame from the CSV file
        self.df = pd.read_csv('tsla_daily_from_current_year_data.csv')

        # Ensure the 'Datetime' column is in datetime format
        self.df['Datetime'] = pd.to_datetime(self.df['Datetime'])

        # Ask the user to input a date
        input_date_str = input("Enter a date within the last month (YYYY-MM-DD): ")
        input_date = pd.to_datetime(input_date_str)

        # Filter the DataFrame to include only the data for the input date
        daily_data_df = self.df[self.df['Datetime'].dt.date == input_date.date()]

        # Filter to include only times from 7:30 AM to 6 PM EST
        daily_data_df = daily_data_df[
            (daily_data_df['Datetime'].dt.time >= datetime.strptime('07:30', '%H:%M').time()) &
            (daily_data_df['Datetime'].dt.time <= datetime.strptime('18:00', '%H:%M').time())
            ]

        # Check if there is data for the input date
        if daily_data_df.empty:
            logger.info(f"No data available for {input_date_str}")
            return

        # Extract only the time part from the 'Datetime' column and format it as HHMM
        daily_data_df['Time'] = daily_data_df['Datetime'].dt.strftime('%H%M')

        # Convert 'Time' to integers for plotting
        daily_data_df['Time'] = daily_data_df['Time'].astype(int)

        # Sort the DataFrame by 'Datetime'
        daily_data_df = daily_data_df.sort_values(by='Datetime')

        # Plot the high prices vs. time
        plt.figure(figsize=(20, 16))
        plt.plot(daily_data_df['Time'], daily_data_df['High'], marker='o', label='High Price', color='blue')
        plt.plot(daily_data_df['Time'], daily_data_df['Low'], marker='o', label='Low Price', color='red')
        plt.plot(daily_data_df['Time'], daily_data_df['Open'], marker='o', label='Open Price', color='green')
        plt.plot(daily_data_df['Time'], daily_data_df['Close'], marker='o', label='Close Price', color='purple')
        plt.xlabel('Time During The Day (EST)')
        plt.ylabel('High Price')
        plt.title(f'{input_date_str} - Tesla Stock Price Intraday 30-Minute Interval Data')
        plt.legend()

        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45)

        # Save the plot to the current working directory
        plot_filename = f'tesla_{input_date_str}_30min_high_price.png'
        plt.savefig(plot_filename)
        plt.close()

        # Get the current working directory
        current_directory = os.getcwd()

        # Construct the full file path
        file_path = os.path.join(current_directory, plot_filename)
        logger.info(f"Plot saved to {file_path}")

    def fetch_from_data_source(self):
        # Make the request to Alpha Vantage
        r = requests.get(BASE_URL)

        # Parse the response JSON
        try:
            self.data = r.json()
        except Exception as e:
            logger.error(f"Error decoding JSON response. Response content: {r.text}. Error: {e}")
            raise

        # Save the data to a JSON file
        with open('tsla_daily_from_current_year_data.json', 'w') as json_file:
            json.dump(self.data, json_file)

    @log_function_completion
    def create_json_file(self):
        # Get the current directory that stock.py file is in
        current_directory = os.getcwd()

        # Specify your JSON file name here
        filename = "tsla_daily_from_current_year_data.json"

        # Construct the full file path
        file_path = os.path.join(current_directory, filename)

        # Check if the file exists
        if os.path.isfile(file_path):

            # Load the existing JSON data
            with open(file_path, 'r') as json_file:
                existing_data = json.load(json_file)

            # Extract the latest date from the existing JSON data
            if self.time_series_key in existing_data:
                latest_date = max(existing_data[self.time_series_key].keys())
                today_date = datetime.now().strftime('%Y-%m-%d')

                # Check if the latest date matches today's date
                if latest_date == today_date:
                    logger.info(
                        f"The latest date in this file located at: ({file_path}) matches today's date: ({today_date}).")
                    return

                else:
                    logger.info(f"File exists, but needs updating. Fetching latest data from source...")
                    self.fetch_from_data_source()

            else:
                logger.error(f"Time Series (30min) key not detected. Exiting program...")
                sys.exit(1)

        else:
            logger.info(f"No file exists yet. Fetching latest data from source...")
            self.fetch_from_data_source()

    @log_function_completion
    def create_pd_from_json_file(self):
        # Load the data from the JSON file
        with open('tsla_daily_from_current_year_data.json') as json_file:
            self.data = json.load(json_file)

        # Extract time series data
        try:
            if self.time_series_key in self.data:
                logger.info(f"The {self.time_series_key} key exists in the JSON data. "
                            f"Continuing the {self.create_pd_from_json_file.__name__} function...")
        except Exception as e:
            logger.error(f"Expected key '{self.time_series_key}' not found in JSON data. Error: {e}")
            raise

        time_series_data = self.data[self.time_series_key]

        # Convert the time series data to a pandas DataFrame
        self.df = pd.DataFrame.from_dict(time_series_data, orient='index')

        # Rename columns for easier access
        self.df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        # Convert index to datetime and reset index to add Datetime as a column
        self.df.index = pd.to_datetime(self.df.index)
        self.df.reset_index(inplace=True)

        # Rename index to Datetime
        self.df.rename(columns={'index': 'Datetime'}, inplace=True)

        # Ensure the DataFrame has the desired columns
        self.df = self.df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]

        # Assign data types to the columns
        self.df = self.df.astype({
            'Datetime': 'datetime64[ns]',
            'Open': 'float64',
            'High': 'float64',
            'Low': 'float64',
            'Close': 'float64',
            'Volume': 'int64'
        })

        # Save the organized DataFrame to a CSV file
        self.df.to_csv('tsla_daily_from_current_year_data.csv')

    @log_function_completion
    def save_dataframe_to_mysql(self):
        engine = create_engine(CONNECTION_STATUS)
        self.df.to_sql(DB_TABLE_NAME, engine, if_exists='replace', index=False)


if __name__ == '__main__':
    tesla = TeslaDatabase()
    tesla.create_json_file()
    tesla.create_pd_from_json_file()
    tesla.save_dataframe_to_mysql()
    # tesla.graphing()
