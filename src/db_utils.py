import logging

from typing import Optional, List, Tuple
from typing import Type

from sqlmodel import SQLModel, Session, create_engine
from sqlmodel import select, func
from datetime import datetime
from datetime import timedelta


def date_range(start_date: datetime, days: int = 0, hours: int = 0) -> tuple[datetime, datetime]:
    """
        Generates a date range starting from a given date.

        Args:
            start_date (datetime): The starting date for the range.
            days (int, optional): Number of days to add to the start_date. Default is 0.
            hours (int, optional): Number of hours to add to the start_date. Default is 0.

        Returns:
            tuple[datetime, datetime]: A tuple containing the start date and the end date of the range.
        """
    return start_date, start_date + timedelta(days=days, hours=hours)


def parse_date_filter(date_filter: str) -> tuple[datetime, datetime]:
    """
    Parses a date filter string into a tuple representing a start and end datetime.

    Args:
        date_filter (str): A string representing the date filter (e.g., "today", "yesterday").

    Returns:
        tuple[datetime, datetime]: A tuple containing the start and end datetime based on the filter.

    Raises:
        ValueError: If the date_filter value is not recognized.
    """
    now = datetime.now()
    if date_filter == "selected_hour":
        timestamp_str = input("Write interested hour (%Y-%m-%d %H'):")
        start_date = datetime.strptime(timestamp_str, '%Y-%m-%d %H')
        return date_range(start_date, hours=1)
    elif date_filter == "today":
        return date_range(now.replace(hour=0, minute=0, second=0, microsecond=0), days=1)
    elif date_filter == "yesterday":
        start_date = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return date_range(start_date, days=1)
    elif date_filter == "current_week":
        start_date = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        return date_range(start_date, days=7)
    elif date_filter == "last_seven_days":
        end_date = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return date_range(end_date - timedelta(days=7), days=7)
    else:
        raise ValueError("Invalid date filter")


class DatabaseManager:
    """
    A class responsible for managing database operations for a weather data application.

    This class abstracts the database interactions, providing methods to create tables,
    add records, and query statistics related to weather conditions. It uses SQLModel
    for ORM operations, making it easier to work with SQL databases in a Pythonic way.

    Attributes:
        engine (sqlalchemy.engine.Engine): An SQLAlchemy engine instance used for database
                                           connections. It is initialized with the database URL
                                           provided during the instantiation of the class.

    Methods:
        __init__(database_url: str):
            Initializes a new instance of the DatabaseManager class with the given database URL.

        create_tables():
            Creates the necessary tables in the database using the metadata from SQLModel classes.
            This method should be called to ensure all required tables are set up before the application
            attempts to add records or perform queries.

        add_record(record: SQLModel):
            Adds a new record to the database. This method is designed to insert a single record into
            the database, where the record must be an instance of a class derived from SQLModel.
            If the database operation fails, it logs an error and rolls back the transaction.

        query_stats(model: Type[SQLModel], group_by_field: str, date_filter: str = 'today') -> List[Tuple[str, float, float, float]]:
            Queries the database for statistics based on the given model and grouping criteria. It supports
            filtering data based on date ranges (e.g., today, yesterday) and returns aggregated statistics
            such as maximum, minimum, and standard deviation of temperatures.

        get_cities_stats(model: Type[SQLModel], date_filter: str = 'today') -> List[Tuple[str, float, float, float]]:
            Fetches and returns statistics for cities by delegating to the `query_stats` method with the 'city'
            field as the grouping criterion.

        get_countries_stats(model: Type[SQLModel], date_filter: str = 'today') -> List[Tuple[str, float, float, float]]:
            Fetches and returns statistics for countries by delegating to the `query_stats` method with the 'country'
            field as the grouping criterion.

        get_temperature_extremes(model: Type[SQLModel], date_filter: str = 'today', temp_extreme: str = 'max') -> Optional[Tuple[str, float]]:
            Retrieves the city with the extreme (maximum or minimum) temperature for the specified date range. It returns
            a tuple containing the city name and its extreme temperature value.

        count_rain_hours(model: Type[SQLModel], date_filter: str = 'today') -> int:
            Counts the number of hours with rain presence within the specified date range and returns the count. It's useful
            for analyzing precipitation data over time.

    The DatabaseManager class simplifies the management of database operations, abstracting the complexity of direct SQL
    interactions and providing a clear, Pythonic interface for performing common database tasks related to weather data management.
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

    def query_stats(self, model: Type[SQLModel], group_by_field: str, date_filter: str = 'today') -> List[
        Tuple[str, float, float, float]]:
        """
        Queries statistics from the database based on the given model and grouping.

        Args:
        model (Type[SQLModel]): The SQLModel class to query.
            group_by_field (str): The field name to group the results by.
            date_filter (str, optional): The date range filter for the query. Default is 'today'.

        Returns:
            List[Tuple[str, float, float, float]]: A list of tuples containing the grouped field value and statistics (max, min, stddev of temperature).
        """
        start_date, end_date = parse_date_filter(date_filter)
        group_by_column = getattr(model, group_by_field)

        with Session(self.engine) as session:
            query = select(
                group_by_column,
                func.max(model.temperature).label('max_temp'),
                func.min(model.temperature).label('min_temp'),
                func.stddev(model.temperature).label('stddev_temp')
            ).where(
                model.weather_data_date >= start_date,
                model.weather_data_date < end_date
            ).group_by(group_by_column)

            result = session.exec(query).all()
            return result

    def get_cities_stats(self, model: Type[SQLModel], date_filter: str = 'today') -> List[
        Tuple[str, float, float, float]]:
        """ Fetches and returns statistics for cities. """
        return self.query_stats(model, 'city', date_filter)

    def get_countries_stats(self, model: Type[SQLModel], date_filter: str = 'today') -> List[
        Tuple[str, float, float, float]]:
        """ Fetches and returns statistics for countries. """
        return self.query_stats(model, 'country', date_filter)

    def get_temperature_extremes(self, model: Type[SQLModel], date_filter: str = 'today', temp_extreme: str = 'max') -> \
            Optional[Tuple[str, float]]:
        """
        Fetches and returns the city with the extreme temperature (maximum or minimum).

        The return type is a tuple containing:
        - The city name (str)
        - Its extreme temperature (float)

        If no records are found, returns None.
        """
        start_date, end_date = parse_date_filter(date_filter)
        temp_column = model.temperature
        temp_func = func.max(temp_column) if temp_extreme == 'max' else func.min(temp_column)

        with Session(self.engine) as session:
            query = select(
                model.city,
                temp_func.label(f'{temp_extreme}_temp'),
            ).where(
                model.weather_data_date >= start_date,
                model.weather_data_date < end_date
            ).group_by(model.city).order_by(temp_func.desc() if temp_extreme == 'max' else temp_func)

            result = session.exec(query).first()
            return result

    def count_rain_hours(self, model: Type[SQLModel], date_filter: str = 'today') -> int:
        """
        Counts and returns the number of hours with rain presence within the specified date range.

        Args:
            model (Type[SQLModel]): The SQLModel class representing the table to query.
            date_filter (str): A string representing the date range for which to count rain hours. Defaults to 'today'.

        Returns:
            int: The number of hours with rain presence within the specified date range.
        """
        start_date, end_date = parse_date_filter(date_filter)

        with Session(self.engine) as session:
            query = select(
                func.count().label('rain_presence')
            ).where(
                model.weather_data_date >= start_date,
                model.weather_data_date < end_date,
                model.rain_presence == True
            )

            result = session.exec(query).one()
            return result
