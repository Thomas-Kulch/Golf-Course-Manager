# Golf Course Manager

Welcome to Boston Country Club! This project is a golf course management system that combines big data processing, machine learning, and analytics to provide score predictions, dynamic pricing, and performance insights for golfers.

## Features

- **Score Prediction**: ML-powered golf score predictions based on weather conditions and player statistics
- **Dynamic Pricing**: Dynamic pricing algorithm considering weather, player skill, and demand patterns
- **Player Analytics**: Performance tracking and visualization over time
- **Booking Management**: Complete tee time booking system built with Flask
- **Big Data Processing**: Apache Spark pipeline for processing large datasets
- **Relational Database**: PostgreSQL database with functions and triggers for simplified workflows

## Technology Stack

- **Backend**: Python, Flask
- **Big Data**: Apache Spark
- **Database**: PostgreSQL
- **Machine Learning**: Scikit-learn
- **Visualization**: Matplotlib
- **Frontend**: HTML

## Prerequisites

Before setting up the project, ensure you have the following installed:

- Python 3.8+
- Java 8 or 11 (required for Spark)
- Apache Spark 3.4+
- PostgreSQL 12+
- Git

## Installation & Setup

### 1. Clone the Repository

```bash
git clone https://github.com/Thomas-Kulch/Golf-Course-Manager.git
cd golf-analytics-platform
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Spark and Java Paths

Edit `src/data_processing.py` and update the following variables with your local paths:

```python
# Update these paths to match your local installation
SPARK_HOME = "/path/to/your/spark"  # ex "/usr/local/spark"
JAVA_HOME = "/path/to/your/java"    # ex "/usr/lib/jvm/java-11-openjdk"

# Example for macOS:
# SPARK_HOME = "/opt/homebrew/Cellar/apache-spark/3.4.0/libexec"
# JAVA_HOME = "/opt/homebrew/Cellar/openjdk@11/11.0.19/libexec/openjdk.jdk/Contents/Home"

# Example for Windows:
# SPARK_HOME = "C:\\spark\\spark-3.4.0-bin-hadoop3"
# JAVA_HOME = "C:\\Program Files\\Java\\jdk-11.0.19"
```

### 4. Set Up Database

#### Create PostgreSQL Database and Run Database Schema

In the database directory, execute the queries in create_database.sql, init.sql, and user_permissions.sql in order in PostgreSQL.

### 5. Load Data

Run the data processing pipeline to generate and load sample data:

```bash
# Process data with Spark pipeline. Make sure golf and weather data are in ../data/raw directory
python src/data_processing.py
```

### 6. Train Machine Learning Model

```bash
# Run the model training notebook
jupyter notebook notebooks/04_model_dev.ipynb
```

## Usage

### Start the Flask Application

```bash
python app.py
```

Click on the link in the terminal to access local site.

### Using the Application

1. **Enter Player Name**: Start by entering your name on the homepage
2. **New Player Setup**: If you're new, enter your handicap
3. **Book Tee Time**: Select date, time, and cart option
4. **View Predictions**: See your predicted score and price
5. **Analyze Performance**: View your historical performance charts (Only if you have 20 or more rounds in the system)

## Project Structure

```
golf-analytics-platform/
├── data/
│   ├── raw/                    # Original datasets
│   ├── processed/              # Cleaned data used for analysis
│   └── models/                 # Trained ML models
├── documents/                  # Project reports/presentation
├── database/
│   ├── create_database.sql     #  Database create script
│   ├── init.sql                # Database Schema and Functions
│   ├── queries.sql             # Queries for Database
│   ├── user_permissions.sql    # Database user permissions
├── notebooks/
│   ├── 01_eda.ipynb
│   ├── 02_sda.rmd
│   ├── 03_feature_eng.ipynb
│   └── 04_model_dev.ipynb
├── src/
│   ├── templates/
│       ├── index.html
│       └── dashboard.html
│   ├── app.py                 # Flask web application
│   ├── data_processing.py     # Spark ETL pipeline
│   ├── database.py            # Database operations
│   └── round_booking.py       # Booking system logic
├── scripts/
│   ├── conversions.py          # Simple conversions used in EDA
│   ├── feature_engineering.py # Feature engineering functions
│   └── run_pipeline.py         # Script that runs Spark pipeline
├── requirements.txt
└── README.md
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

**Thomas Kulch**  
*Data Scientist*

- Email: kulch.t@northeastern.edu

## Acknowledgments

- Kaggle for golf and weather data
- Apache Spark for big data processing framework
- Scikit-learn for machine learning
