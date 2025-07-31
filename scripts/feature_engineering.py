"""
Author: Thomas Kulch
DS5110 - Final Project - Golf Course Manager
Feautre Engineering module - Used in round_booking.py
"""
import pandas as pd
from sklearn.preprocessing import StandardScaler


def feature_engineering(df, scaler=None):
    """feature engineering for new records without scaling"""
    # create new features
    df['wind_precip'] = df['precipitation'] * df['wind_speed']
    df['wind_cold'] = df['wind_speed'] * (df['avg_temp'] < 15).astype(int)
    df['bad_weather_combo'] = ((df['wind_speed'] > 15) &
                           (df['precipitation'] > 0.5) &
                           (df['avg_temp'] < 15)).astype(int)
    df['weekend'] = (df['day_of_week_int'] >= 5)

    # get features, target, and scaler
    if 'score' in df.columns: # for reusability
        X = df.drop(['score'], axis=1)
        y = df['score']
        return X, y, scaler
    else:
        return df

def feature_engineering_with_scaling(df):
    """feature engineering for initial training with scaling included"""
    df['wind_precip'] = df['precipitation'] * df['wind_speed']
    df['wind_cold'] = df['wind_speed'] * (df['avg_temp'] < 15).astype(int)
    df['bad_weather_combo'] = ((df['wind_speed'] > 15) &
                           (df['precipitation'] > 0.5) &
                           (df['avg_temp'] < 15)).astype(int)
    df['weekend'] = (df['day_of_week_int'] >= 5)

    features_to_scale = ['round_number', 'handicap', 'avg_temp',
                   'precipitation', 'wind_speed']

    scaler = StandardScaler()
    df[features_to_scale] = scaler.fit_transform(df[features_to_scale])

    X = df.drop(['score'], axis=1)
    y = df['score']

    return X, y, scaler