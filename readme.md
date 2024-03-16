# README

## Overview
This script processes server logs and stores parsed data into a database. It uses PySpark for log analysis and transformation, and Power BI for visuallisation.

## Configuration Files
Ensure the following configuration files are correctly set up before running the program:
- `config.yml`: Contains configuration settings for the Spark session, database connection, and other parameters.
- `.env`: Contains environment variables (like database passwords).

## Environment Setup
1. Install all required Python packages from `requirements.txt`.
2. Set up the `.env` file with the necessary environment variables.

## Usage
Execute the script with the following command from the project root directory:

python src/transform.py

## Database Setup
Execute schema/user_requests.sql to create the required DB table.

## Steps Performed by the Script
1. Initialize environment variables.
2. Load configurations from `config.yml`.
3. Start a Spark session.
4. Read raw logs using the configured path.
5. Parse logs into structured format.
6. Remove duplicates and drop raw logs column.
7. Write the cleaned data into the configured database.
8. Stop the Spark session.

## Prerequisites
- Python 3.x
- PySpark
- JDK8 for Spark and JDBC
- Database JDBC driver

## File Structure
- `src/` - Contains the main Python script.
  - `transform.py` - The main Python script that parses and processes logs.
- `config.yml` - YAML config file for application settings.
- `.env` - Environment file storing sensitive information e.g., `DB_PASSWORD`.