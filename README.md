# Python ETL Pipeline

## Overview

This project is an ETL (Extract, Transform, Load) pipeline written in Python to fetch, process, and visualize intraday stock data for Tesla (TSLA) using the Alpha Vantage API. 
The pipeline extracts data from the API, transforms it into a structured format using Pandas, 
and loads it into a MySQL database. It also includes functionalities to visualize stock price data.

## Features

- **Data Extraction**: Fetches intraday stock data from the Alpha Vantage API.
- **Data Transformation**: Converts JSON data into a Pandas DataFrame and saves it as a CSV file.
- **Data Loading**: Saves the transformed DataFrame into a MySQL database.
- **Data Visualization**: Generates high-low stock price plots for a specified date using Matplotlib.
- **Logging**: Implements logging for tracking operations and debugging.

**Main Skills Used**: Object Oriented Programming (OOP), Pandas, Matplotlib, Decorators, Rest APIs

## Requirements

- Python 3.x
- `requests`
- `pandas`
- `matplotlib`
- `SQLAlchemy`
- `python-dotenv`
- `pymysql`
