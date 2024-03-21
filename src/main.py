from aws.aws_lambda import WeatherData
from db_utils import *
import logging
import argparse
import os

from typing import Optional

from dotenv import load_dotenv


# Environment variables
load_dotenv()
api_key = os.getenv('OPENWEATHERMAP_API_KEY')
database_url = os.getenv('DATABASE_URL')
logging.basicConfig(level=logging.INFO)


def fetch_weather_stats(data_type, date_filter, database_url, temp_extreme=None):
    """
    Fetches and logs weather statistics based on specified criteria.

    This function determines which type of statistics to fetch (for countries, cities, temperature extremes,
    or rain hours) based on input arguments and logs the results.

    Args:
        data_type (str): The type of weather statistics to fetch ('countries', 'cities', 'extremes', or 'rain').
        date_filter (str): The date range for which to fetch statistics.
        database_url (str): The URL of the database from which to fetch data.
        temp_extreme (Optional[str]): Specifies whether to fetch 'max' or 'min' temperature extremes, applicable only when data_type is 'extremes'.

    Returns:
        None
    """
    db_manager = DatabaseManager(database_url)

    if data_type == 'countries':
        stats = db_manager.get_countries_stats(WeatherData, date_filter)
        for stat in stats:
            logging.info(stat)
    elif data_type == 'cities':
        stats = db_manager.get_cities_stats(WeatherData, date_filter)
        for stat in stats:
            logging.info(stat)
    elif data_type == 'extremes':
        extremes = db_manager.get_temperature_extremes(WeatherData, date_filter, temp_extreme)
        return logging.info(extremes)
    elif data_type == 'rain':
        counted_rain = db_manager.count_rain_hours(WeatherData, date_filter)
        return logging.info(counted_rain)
    else:
        logging.error('Invalid data type specified.')


def main():
    """
   Main function that orchestrates the fetching and logging of weather statistics.

   This function parses command-line arguments to determine the type of weather statistics to fetch
   (e.g., for countries, cities, temperature extremes, or rain presence) and the date range for which to fetch these
   statistics. It supports filtering by specific dates or periods such as today, yesterday, the current week, or the
   last seven days. Additionally, it allows fetching extremes of temperature (maximum or minimum) and counting hours
   with rain presence.

   The function utilizes a DatabaseManager to interact with a database configured via environment variables,
   specifically to retrieve weather data stored in a SQLModel-based schema. It demonstrates the use of command-line
   arguments to influence the flow of the program and the application of database queries to gather meaningful
   statistics from stored weather data.

   Args:
       None, but reads from command-line arguments:
           --countries: To fetch weather statistics for countries.
           --cities: To fetch weather statistics for cities.
           --extremes: To fetch temperature extremes for cities.
           --rain: To count the number of hours with rain presence.
           --date_filter: To specify the date range for fetching weather stats. Choices include 'selected_hour',
                'today', 'yesterday', 'current_week', 'last_seven_days'.
           --temp_extreme: To specify which temperature extreme ('max' or 'min') to fetch, applicable only when
                fetching temperature extremes.

   Returns:
       None. Outputs are logged to the console.
   """


parser = argparse.ArgumentParser(description='Get weather statistics for cities or countries.')
group = parser.add_mutually_exclusive_group()
group.add_argument('--countries', action='store_true', help='Get countries weather stats instead of cities')
group.add_argument('--cities', action='store_true', help='Get cities weather stats instead of countries')
group.add_argument('--extremes', action='store_true', help='Get extreme weather stats of cities')
group.add_argument('--rain', action='store_true', help='Count the number of rows (hours) with rain presence')

parser.add_argument('--date_filter', help='Date range for fetching weather stats',
                    choices=['selected_hour', 'today', 'yesterday', 'current_week', 'last_seven_days'],
                    default='last_seven_days')
parser.add_argument('--temp_extreme', help='Indicate the temperature extreme to fetch (highest or lowest)',
                    choices=['max', 'min'], default='max')
args = parser.parse_args()

if args.countries:
    data_type = 'countries'
elif args.cities:
    data_type = 'cities'
elif args.extremes:
    data_type = 'extremes'
elif args.rain:
    data_type = 'rain'
else:
    raise ValueError('Use flags to get stats: --countries or --cities or --extremes.')

fetch_weather_stats(data_type, args.date_filter, database_url, args.temp_extreme)

if __name__ == "__main__":
    main()
