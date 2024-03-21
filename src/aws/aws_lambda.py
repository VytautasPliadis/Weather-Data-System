import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional, Dict

import requests
from dotenv import load_dotenv
from pydantic_core._pydantic_core import ValidationError
from sqlmodel import SQLModel, Session, create_engine, Field

# Environment variables
load_dotenv()
api_key = os.getenv('OPENWEATHERMAP_API_KEY')
database_url = os.getenv('DATABASE_URL')
logging.basicConfig(level=logging.INFO)


# SQLModel
class WeatherData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True, index=True)
    country: str = Field(index=True, description='Code of the country')
    city: str = Field(index=True, description='Name of the city')
    temperature: float = Field(description='Temperature in Celsius')
    rain_presence: bool = Field(description='Indicates if rain is present')
    weather_description: str = Field(description='Textual description of the weather')
    weather_data_date: datetime = Field(description='Date and time of the weather data')


class WeatherAPI:
    """
    A class to interact with the OpenWeatherMap API to fetch weather data for specified cities.

    Attributes:
        api_key (str): The API key used for authenticating with the OpenWeatherMap API.
        base_url (str): The base URL for the OpenWeatherMap API.
        weather_data (Optional[str]): Placeholder for storing fetched weather data. Currently not used.

    Methods:
        get_weather(city: str) -> Optional[Dict[str, Any]]: Fetches weather data for a specified city.
    """

    def __init__(self, api_key: str):
        """
        Initializes the WeatherAPI with an API key.

        Args:
            api_key (str): The API key for the OpenWeatherMap service.
        """
        self.api_key = api_key
        self.base_url = 'http://api.openweathermap.org/data/2.5/weather?'
        self.weather_data: Optional[str] = None

    def get_weather(self, city: str) -> Optional[Dict[str, float]]:
        """
        Fetches weather data for a specified city from the OpenWeatherMap API.

        Args:
            city (str): The name of the city for which to fetch weather data.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing weather data including country, city, temperature,
            rain presence, weather description, and weather data date. Returns None if an error occurs during the
            API call or if the response status is not 200.
        """
        complete_url = f'{self.base_url}q={city}&appid={self.api_key}&units=metric'
        try:
            response = requests.get(complete_url)
            if response.status_code == 200:
                data = response.json()
                main = data['main']
                temperature = main['temp']
                weather_description = data['weather'][0]['description']
                rain_data = data.get('rain', {})
                rain_last_hour = rain_data.get('1h', 0)
                rain_presence = rain_last_hour > 0
                timestamp = data['dt']
                weather_data_date = datetime.utcfromtimestamp(timestamp)
                country = data['sys']['country']

                return {
                    'country': country,
                    'city': city,
                    'temperature': temperature,
                    'rain_presence': rain_presence,
                    'weather_description': weather_description,
                    'weather_data_date': weather_data_date
                }


            else:
                logging.error('Error in the HTTP request')
                return None
        except requests.RequestException as e:
            logging.error(f'An error occurred while making the request: {e}')
            return None


class DatabaseManager:
    """
    Manages database operations related to weather data, including creating tables and adding records.

    Attributes:
        engine (Any): The database engine used for connections.

    Methods:
        create_tables(): Creates the necessary tables in the database.
        add_record(record: SQLModel): Adds a new record to the database.
    """

    def __init__(self, database_url: str) -> None:
        """
        Initializes the DatabaseManager with a database URL.

        Args:
            database_url (str): The URL of the database to connect to.
        """
        self.engine = create_engine(database_url)

    def create_tables(self) -> None:
        """ Creates the necessary tables in the database using the metadata from the SQLModel classes. """
        SQLModel.metadata.create_all(self.engine)

    def add_record(self, record: SQLModel) -> None:
        """
        Adds a new record to the database.

        Args:
            record (SQLModel): An instance of a SQLModel-derived class representing the record to be added to the database.

        Raises:
            Logs an error if any database operation fails.
        """
        with Session(self.engine) as session:
            try:
                session.add(record)
                session.commit()
                session.refresh(record)
            except Exception as e:
                logging.error(f"Database error: {e}")
                session.rollback()


def fetch_and_store_weather(city: str, api: WeatherAPI, db: DatabaseManager) -> None:
    """
    Fetches weather data for a specified city using the WeatherAPI and stores it in the database using DatabaseManager.

    Args:
        city (str): The name of the city for which to fetch and store weather data.
        api (WeatherAPI): An instance of the WeatherAPI class to use for fetching weather data.
        db (DatabaseManager): An instance of the DatabaseManager class for storing the fetched data into the database.

    This function demonstrates how to integrate external API calls with database operations within a single workflow.
    Errors during data fetching or database operations are logged.
    """
    try:
        data = api.get_weather(city)
        if data:
            try:
                weather_record = WeatherData(**data)
                db.add_record(weather_record)
            except ValidationError as e:
                logging.error(f"Validation error for {city}: {e.json()}")
    except Exception as e:
        logging.error(f"Error processing {city}: {e}")


def lambda_handler(event, context) -> None:
    """
    AWS Lambda handler to fetch and store weather data for predefined cities.

    This function is designed to be triggered by an AWS Lambda event. It initializes a DatabaseManager to connect to a
    database, creates necessary tables using the WeatherData SQLModel class, and utilizes a WeatherAPI instance to
    fetch weather data from the OpenWeatherMap API for a predefined list of cities.

    The weather data fetched for each city includes temperature, presence of rain, weather description, and the date and
    time of the data. This information is then stored in the specified database. The function employs a
    ThreadPoolExecutor to parallelize the fetch and store operations, enhancing efficiency and reducing the overall
    execution time.

    Args:
        event: The event dictionary that AWS Lambda passes to the function. This can contain any information necessary
        to process the event but is not used in this function.
        context: The context runtime information provided by AWS Lambda. This includes information such as the function
        name, memory limit, and time remaining, but is not utilized in this function.

    Returns:
        A dictionary with two keys: 'statusCode', indicating the HTTP status code of the operation, and 'body',
        a message describing the outcome of the operation. On successful execution, it returns a status code of 200 and
        a success message. If an exception occurs, it returns a status code of 500 and an error message.

    This handler demonstrates how to integrate external APIs, database operations, and concurrency within an AWS Lambda
    function to perform scheduled or event-driven data collection and storage tasks.
    """
    try:
        db = DatabaseManager(database_url)
        db.create_tables()
        api = WeatherAPI(api_key)

        cities = [
            "Istanbul", "London", "Saint Petersburg", "Berlin", "Madrid", "Kyiv", "Rome",
            "Bucharest", "Paris", "Minsk", "Vienna", "Warsaw", "Hamburg", "Budapest",
            "Belgrade", "Barcelona", "Munich", "Kharkiv", "Milan"
        ]

        with ThreadPoolExecutor() as executor:
            for city in cities:
                executor.submit(fetch_and_store_weather, city, api, db)

        return {"statusCode": 200, "body": "Weather data updated successfully"}

    except Exception as e:
        logging.error(f"Lambda execution error: {e}")
        return {"statusCode": 500, "body": "Internal server error"}
