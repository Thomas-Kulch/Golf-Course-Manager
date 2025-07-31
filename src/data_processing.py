"""
Author: Thomas Kulch
DS5110 - Final Project - Golf Course Manager

Data Processing
PySpark ETL pipeline & advanced features

Requirements:
    -golf csv and weather csv
    -both need to be present in data/raw for ETL to work
    -golf_analytics db created as well as tables
Duplicate weather and player records are not loaded, but round record are
"""
import sys
import os
import random
import glob
import findspark
import platform

current_os = platform.system() # get current os

# change environment files based on os. need to update file locations with your personal paths
if current_os == "Windows": # windows
    # environment variables for Java and Spark manually in the script:
    os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17.0.15+6"  # your actual Java path for windows
    os.environ["SPARK_HOME"] = r"C:\spark\spark-4.0.0-bin-hadoop3"  # your Spark install path for windows
    os.environ["HADOOP_HOME"] = r"C:\hadoop-3.0.0"
    os.environ["PATH"] = (
        os.path.join(os.environ["JAVA_HOME"], "bin") + ";" +
        os.path.join(os.environ["SPARK_HOME"], "bin") + ";" +
        os.path.join(os.environ["HADOOP_HOME"], "bin") + ";" +
        os.environ.get("PATH", "")
    )
elif current_os == "Darwin": # macos
    # environment variables for Java and Spark manually in the script:
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home" # your actual Java path for macos
    os.environ["SPARK_HOME"] = "/Users/thomaskulch/spark/spark-4.0.0-bin-hadoop3" # your Spark install path for macos

    # macOS/Linux PATH separator is colon
    os.environ["PATH"] = (
            os.path.join(os.environ["JAVA_HOME"], "bin") + ":" +
            os.path.join(os.environ["SPARK_HOME"], "bin") + ":" +
            os.environ.get("PATH", "")
    )

# initialize findspark with your SPARK_HOME
findspark.init(os.environ["SPARK_HOME"])

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from scripts import conversions

class DataProcessor:
    def __init__(self):
        # absolute path to JDBC driver
        current_dir = os.path.dirname(os.path.abspath(__file__))
        jdbc_driver_path = os.path.join(current_dir, "..", "lib", "postgresql-42.3.1.jar")

        # initialize Spark with PostgreSQL driver
        self.spark = SparkSession.builder \
            .appName("Golf_Course_Manager") \
            .master("local[*]") \
            .config("spark.jars", jdbc_driver_path) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        # database connection properties
        self.jdbc_props = {
            "url": "jdbc:postgresql://localhost:5432/golf_analytics",
            "driver": "org.postgresql.Driver",
            "user": "golf_user",
            "password": "golf_password"
        }

        # test connection
        if not self.test_connection():
            raise Exception("Failed to connect to database")

        print("Database connection established")

    def test_connection(self):
        """test database connection using Spark"""
        try:
            print("Testing database connection...")

            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_props["url"]) \
                .option("dbtable", "(SELECT 'Connection successful!' as message, NOW() as timestamp) as test_query") \
                .option("user", self.jdbc_props["user"]) \
                .option("password", self.jdbc_props["password"]) \
                .option("driver", self.jdbc_props["driver"]) \
                .load()

            result = test_df.collect()[0]
            print(f"Database test result: {result['message']}")
            return True

        except Exception as e:
            print(f"Database connection failed: {e}")
            return False

    def extract_raw_data(self):
        """get raw data from files using glob"""
        # set paths
        raw_directory = "../data/raw/"
        golf_pattern = f"{raw_directory}golf*.csv"
        weather_pattern = f"{raw_directory}boston_weather_data.csv"

        # get files from paths
        golf_files = glob.glob(golf_pattern)
        weather_files = glob.glob(weather_pattern)

        # let user know how many files we found
        print(f"Found {len(golf_files)} golf files")
        print(f"Found {len(weather_files)} weather files")

        if golf_files and weather_files: # if both files found
            # only use the first file found for each type
            golf_file = golf_files[0]
            weather_file = weather_files[0]

            # let user know file being read
            print(f"Using golf file: {golf_file}")
            print(f"Using weather file: {weather_file}")

            # read data from raw files into spark dfs and return them
            df_golf_raw = self.spark.read.csv(golf_file, header=True, inferSchema=True)
            df_weather_raw = self.spark.read.csv(weather_file, header=True, inferSchema=True)

            return df_golf_raw, df_weather_raw

        # conditions for if one or no files are in the raw directory
        elif golf_files and not weather_files:
            print("Golf files found, but no weather files")
            return None, None
        elif not golf_files and weather_files:
            print("Weather files found, but no golf files")
            return None, None
        else:
            print("No files found")
            return None, None

    def clean_golf_data(self, df_golf_raw):
        """clean golf data - reshape data"""
        # get necessary columns from raw df and pivot the round data
        df_golf = df_golf_raw.select('Name', '`Open.R1`', '`Open.R2`', '`Open.R3`', '`Open.R4`') \
            .filter(col('Name').isNotNull()) \
            .unpivot(
            ids=['Name'],
            values=['`Open.R1`', '`Open.R2`', '`Open.R3`', '`Open.R4`'],
            variableColumnName='Round',
            valueColumnName='Score'
        ) \
            .withColumn("round_number", # this is a placeholder and rounds will be accumulated later by date
                        when(col("Round") == "Open.R1", 1)
                        .when(col("Round") == "Open.R2", 2)
                        .when(col("Round") == "Open.R3", 3)
                        .when(col("Round") == "Open.R4", 4)) \
            .withColumn("player_name", col("Name")) \
            .drop("Round", "Name")

        # add dates to records to be paired with weather records
        df_golf_with_dates = df_golf.withColumn("random_year", floor(rand() * 6) + 2017) \
            .withColumn("random_month", floor(rand() * 7) + 4) \
            .withColumn("random_day", floor(rand() * 28) + 1) \
            .withColumn("round_date",
                        to_date(concat(
                            col("random_year"),
                            lit("-"),
                            lpad(col("random_month"), 2, "0"),
                            lit("-"),
                            lpad(col("random_day"), 2, "0")
                        ), "yyyy-MM-dd")) \
            .drop("random_year", "random_month", "random_day")

        return df_golf_with_dates # return cleaned df

    def clean_weather_data(self, df_weather_raw):
        """clean raw weather data"""
        # get necessary columns from raw data and filter the dates
        df_weather = df_weather_raw.withColumn("date", to_date(col("time"))) \
            .withColumn("avg_temp", col("tavg")) \
            .withColumn("precipitation", col("prcp")) \
            .withColumn("wind_speed", col("wspd")) \
            .withColumn("day_of_week", date_format(col("date"), "EEEE")) \
            .withColumn("day_of_week_int", dayofweek(col("date"))) \
            .select("date", "avg_temp", "precipitation", "wind_speed", "day_of_week", "day_of_week_int") \
            .filter(col('date') >= '2017-04-01') \
            .filter(col('date').isNotNull()) \
            .dropDuplicates(["date"])

        return df_weather # return cleaned df

    def process_players(self, df_golf):
        """process players - insert new records only and avoid duplicates"""
        print("Processing players") # for user

        # Step 1: get unique players from golf data
        unique_players = df_golf.select("player_name").distinct()
        unique_count = unique_players.count()
        print(f"Found {unique_count} unique players in golf data")

        # Step 2: get existing players from database
        try:
            # connect to players table in db
            existing_players = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_props["url"]) \
                .option("dbtable", "players") \
                .option("user", self.jdbc_props["user"]) \
                .option("password", self.jdbc_props["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .load()

            existing_players_is_empty = False # variable keeping track if our table is empty
            # get count of players in players table
            existing_count = existing_players.count()
            print(f"Found {existing_count} existing players in database")

        except Exception as e:
            # if players table is empty, let user know
            print("No existing players found. Table empty.")
            existing_players = None
            existing_players_is_empty = True

        # Step 3: Find new players - those not in players table
        if not existing_players_is_empty:
            # left join to only get non existing players
            new_players = unique_players.join(
                existing_players.select("player_name"),
                on="player_name",
                how="left_anti"  # only players NOT in table
            )
        else:
            # if table is empty, all unique players are new
            new_players = unique_players

        # get count of new players
        new_player_count = new_players.count()

        if new_player_count > 0:
            # display new players count to be loaded to user
            print(f"Creating {new_player_count} new players")

            # Step 4: insert new players into database
            try:
                # write to db
                new_players.write \
                    .format("jdbc") \
                    .option("url", self.jdbc_props["url"]) \
                    .option("dbtable", "players") \
                    .option("user", self.jdbc_props["user"]) \
                    .option("password", self.jdbc_props["password"]) \
                    .mode("append") \
                    .option("driver", "org.postgresql.Driver") \
                    .save()

                print(f"Created {new_player_count} new players") # show inserted players to user

            except Exception as e:
                print(f"Error creating new players: {e}")
                raise
        else:
            # if all players already exist in db, do nothing
            print("All players already exist in database")

        # Step 5: return updated player mapping - all players with their IDs
        updated_players = self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_props["url"]) \
            .option("dbtable", "players") \
            .option("user", self.jdbc_props["user"]) \
            .option("password", self.jdbc_props["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        final_count = updated_players.count()
        print(f"Total players in database: {final_count}")

        return updated_players.select("player_id", "player_name")

    def import_weather_to_database(self, df_weather_cleaned):
        """import cleaned weather data to database"""
        print("Importing weather data to database")

        try:
            # Step 1: check for existing weather dates to avoid duplicates
            try:
                # read weather data from db
                existing_weather = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.jdbc_props["url"]) \
                    .option("dbtable", "weather") \
                    .option("user", self.jdbc_props["user"]) \
                    .option("password", self.jdbc_props["password"]) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()

                # get distinct existing dates (no dates should be duplicated)
                existing_dates = existing_weather.select("date").distinct()
                existing_count = existing_dates.count()
                print(f"Found {existing_count} existing weather dates in database")

            except Exception as e:
                # if table is empty, don't crash function
                print("No existing weather data found.")
                existing_dates = None

            # Step 2: filter out dates that already exist to avoid duplicates
            new_weather_data = df_weather_cleaned.join(
                existing_dates,
                on="date",
                how="left_anti"  # only dates NOT in existing table
            )

            # get counts of new records
            new_records_count = new_weather_data.count()

            # Step 3: import new weather data if any
            if new_records_count > 0:
                print(f"Importing dates")

                # insert new weather data
                new_weather_data.write \
                    .format("jdbc") \
                    .option("url", self.jdbc_props["url"]) \
                    .option("dbtable", "weather") \
                    .option("user", self.jdbc_props["user"]) \
                    .option("password", self.jdbc_props["password"]) \
                    .mode("append") \
                    .option("driver", "org.postgresql.Driver") \
                    .save()

                print(f"Weather data imported: {new_records_count} new records")
            else:
                print("All weather dates already exist in database")

            # Step 4: show final weather table summary to user
            final_weather_count = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_props["url"]) \
                .option("dbtable", "weather") \
                .option("user", self.jdbc_props["user"]) \
                .option("password", self.jdbc_props["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .load().count()

            print(f"Total weather records in database: {final_weather_count}")

        except Exception as e:
            print(f"Error importing weather data: {e}")
            raise

    def import_rounds_to_database(self, df_golf_cleaned):
        """import cleaned rounds data - database calculates handicaps automatically"""
        print("Processing and importing rounds data...")

        try:
            # Step 1: get player counts already existing in db
            players_mapping = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_props["url"]) \
                .option("dbtable", "players") \
                .option("user", self.jdbc_props["user"]) \
                .option("password", self.jdbc_props["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .load() \
                .select("player_id", "player_name")

            print(f"Found {players_mapping.count()} players in database")

            # Step 2: join rounds with player IDs
            df_rounds_with_players = df_golf_cleaned.join(
                players_mapping,
                on="player_name",
                how="inner"
            )

            # Step 3: verify weather data exists for all round dates
            print("Verifying weather data")
            weather_dates = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_props["url"]) \
                .option("dbtable", "weather") \
                .option("user", self.jdbc_props["user"]) \
                .option("password", self.jdbc_props["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .load() \
                .select("date")

            df_rounds_verified = df_rounds_with_players.join(
                weather_dates,
                df_rounds_with_players.round_date == weather_dates.date,
                "inner"
            ).drop("date")

            # Step 5: select final columns
            df_rounds_final = df_rounds_verified.select(
                "player_id",
                "player_name",
                "round_date",
                "score",
                "round_number"
            ).filter(col("score") > 0) # don't import scores of 0

            # get load count
            final_count = df_rounds_final.count()
            print(f"Rounds to import: {final_count}")

            # Step 6: insert rounds - triggers will automatically calculate handicaps and other stats
            if final_count > 0:
                print("Inserting rounds")

                # write to db
                df_rounds_final.write \
                    .format("jdbc") \
                    .option("url", self.jdbc_props["url"]) \
                    .option("dbtable", "rounds") \
                    .option("user", self.jdbc_props["user"]) \
                    .option("password", self.jdbc_props["password"]) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                print(f"Rounds imported: {final_count} records")

        except Exception as e:
            print(f"Error importing rounds data: {e}")
            raise

        return df_rounds_final


if __name__ == "__main__":
    # initialize processor, clean and load the data
    process = DataProcessor()
    df_golf_raw, df_weather_raw = process.extract_raw_data()
    df_golf_cleaned = process.clean_golf_data(df_golf_raw)
    df_weather_cleaned = process.clean_weather_data(df_weather_raw)
    process.process_players(df_golf_cleaned)
    process.import_weather_to_database(df_weather_cleaned)
    process.import_rounds_to_database(df_golf_cleaned)