# Weather Data System
## Description
This project is designed to provide a cost-free solution for collecting, storing, analyzing, 
and backing up weather data. It leverages AWS Lambda for serverless operations, web-based postgres database, regular database backups, 
and data analysis through a set of Python scripts.

![architecture.png](img%2Farchitecture.png)


**AWS Lambda** is a serverless computing service provided by Amazon Web Services (AWS) that allows developers to run 
code in response to events without provisioning or managing servers. Lambda functions can be triggered by various 
AWS services like EventBridge Scheduler or direct HTTP requests.

**Supabase** is an open-source Firebase alternative, providing developers with a suite of tools to build modern web and 
mobile applications. It offers a scalable PostgreSQL database as its backbone, authentication, storage, and serverless
functions for backend development.



## Project Structure
- src/aws/**aws_lambda.py**: Handles AWS Lambda functions for serverless operations. It includes functionalities for fetching weather data, storing it in a database, and initiating serverless processes.
- src/**backup_db.py**: Manages the regular backup of database contents to ensure data durability and recoverability.
- src/**main.py**: The main script orchestrates the project's core functionalities, including data analysis and reporting on weather statistics.
## Key Features
- Weather Data Collection and Storage: Automate the process of collecting weather data and storing it in a relational database for further analysis.
- Database Backup: Regularly backup database content to avoid data loss and ensure data is recoverable.
- Data Analysis: Analyze weather data to extract meaningful insights, such as temperature extremes, rainfall statistics, and more.

## Installation
To run this project, you will need Python 3.8+ and the following libraries:

- dotenv: For loading environment variables.
- logging: For logging information and errors.
- requests and sqlmodel: For interacting with APIs and databases.
- supabase: For database operations in the backup script.
- Additional libraries for data manipulation and concurrent operations.

Install the necessary Python packages using pip:
```
pip install -r requirements.txt
```


## Usage
- AWS Lambda Operations: Deploy aws_lambda.py to AWS Lambda and set it to run at your desired interval (preferably 1h)
for collecting weather data. 
- Backup Database: Run backup_db.py regularly to backup your database contents. Schedule this script as cron job.
```
# Linux terminal
crontab -e
10 * * * * path/backup_db.py >> path/backup_log.txt 2>&1
```
- Analyze Weather Data: Use main.py to perform various data analysis. It supports multiple command-line arguments for 
those tasks.
  - **--countries**: Fetches weather statistics for countries.
  - **--cities**: Fetches weather statistics for cities.
  - **--extremes**: Fetches temperature extremes for cities.
  - **--rain**: Counts the number of hours with rain presence.
    - **--date_filter**: Specifies the date range for fetching weather stats. Options include 'selected_hour', 'today', 'yesterday', 'current_week', 'last_seven_days'. The default is 'last_seven_days'.
    - **--temp_extreme**: Specifies which temperature extreme ('max' or 'min') to fetch. The default is 'max'.

Here are some examples:

Fetch weather statistics for countries for the last seven days:
```
python main.py --countries --date_filter last_seven_days
```
Fetch temperature extremes (minimum) for cities for today:
```
python main.py --extremes --temp_extreme min
```
Count the number of hours with rain presence for the current week:
```
python main.py --rain --date_filter current_week
```
## RDBMS administration
```
CREATE EXTENSION pg_cron;

-- Vacuum every day at 23:30
SELECT cron.schedule('nightly-vacuum', '30 23 * * *', 'VACUUM');

-- Delete old data at 3:00
SELECT cron.schedule('0 3 * * *', $$DELETE FROM weatherdata WHERE weather_data_date < now() - interval '1 week'$$);
```
## Configuration
Ensure you have an .env file with the necessary API keys, database credentials, and other configurations as required by each script.
