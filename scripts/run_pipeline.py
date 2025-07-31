"""
Author: Thomas Kulch
DS5110 - Final Project -  Golf Course Manager
Pipeline runner module - Use for scheduling
"""
from src import data_processing

def main():
    # initialize ETL and dataframes
    process = data_processing.DataProcessor()
    df_golf_raw, df_weather_raw = process.extract_raw_data()

    # clean data
    df_golf_cleaned = process.clean_golf_data(df_golf_raw)
    df_weather_cleaned = process.clean_weather_data(df_weather_raw)

    # load data to db
    process.process_players(df_golf_cleaned)
    process.import_weather_to_database(df_weather_cleaned)
    process.import_rounds_to_database(df_golf_cleaned)

if __name__ == "__main__":
    main()