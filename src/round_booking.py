"""
Author: Thomas Kulch
DS5110 - Final Project - Golf Course Manager

Round Booking module
    Used in Flask application

Requirements
    -ML model from 04 notebook
    -golf_analytics db created as well as tables
"""
import joblib
from datetime import datetime
import pandas as pd
from src.database import DatabaseManager
from scripts import feature_engineering as fe


class RoundBooking:
    def __init__(self):
        # load model and connect to db
        self.score_model = joblib.load('../data/models/model_GB.joblib')
        self.db = DatabaseManager()

    def predict_score(self, features):
        """use ML model to predict score"""
        # create df from input features
        feature_names = ['round_number', 'handicap', 'avg_temp', 'precipitation', 'wind_speed', 'day_of_week_int']
        df = pd.DataFrame([features], columns=feature_names)

        # use feature engineering function to setup features for ML
        input_df = fe.feature_engineering(df)

        # scale necessary features
        features_to_scale = ['round_number', 'handicap', 'avg_temp', 'precipitation', 'wind_speed']
        input_df[features_to_scale] = self.score_model['scaler'].transform(input_df[features_to_scale])

        # ensure columns match training data
        expected_columns = self.score_model['feature_names']

        missing_columns = [] # fill in missing columns if needed
        for col in expected_columns:
            if col not in input_df.columns:
                input_df[col] = 0
                missing_columns.append(col)

        # reorder columns to match training
        input_df = input_df[expected_columns]

        # make score prediction for user
        prediction = self.score_model['model'].predict(input_df)

        return round(prediction[0]) # return predicted score

    def calculate_price(self, tee_time_hour, cart, features):
        """dynamic pricing function"""
        base_price = 75
        price = base_price

        if features[5] >= 5:  # day of week, weekend surcharge
            price += 20

        if cart: # golf cart surcharge
            price += 20

        if tee_time_hour > 13 and features[5] < 5: # if tee time after 1pm or weekday
            price -= 15 # make it cheaper

        if features[3] > 0.5 or features[4] > 15 or features[2] < 15:
            price -= 5  # 3 is rain, 4 is wind, 2 is avg_temp

        if features[1] < 3: # handicap
            price -= 5 # better playesr pay a bit less

        if features[1] > 15: # handicap
            price += 5 # worse players pay a bit more

        if features[0] > 15: # round number, loyalty bonus for members
            price -= 5

        return price # return price for user


    def create_booking(self, name, date, tee_time_hour, cart, features):
        """booking function - this is used by Flask application"""
        try:
            # get predictions and price
            predicted_score = self.predict_score(features)
            price = self.calculate_price(tee_time_hour, cart, features)

            # check if player exists
            player_query = "SELECT player_id FROM players WHERE player_name = %s"
            player_result = self.db.execute_query(player_query, (name,))

            if not player_result: # if player doesn't exist
                # name should be added to db in flask now, this is for testing here
                raise ValueError(f"Player '{name}' not found in database")

            # get player id
            player_id = player_result[0]['player_id']

            # create booking record in SQL
            insert_query = """
                        INSERT INTO bookings (player_id, tee_time, price_paid, booking_status, round_date, score_prediction, booking_time) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
            # generate tee time based on input hour and date
            tee_time = datetime.strptime(f"{date} {tee_time_hour}:00", "%Y-%m-%d %H:%M")

            # run query to insert record
            self.db.execute_query(
                insert_query,
                (player_id, tee_time, price, 'confirmed', date, predicted_score, datetime.now())
            )

            # get booking id
            get_booking_id = 'SELECT MAX(booking_id) FROM bookings WHERE player_id = %s'

            booking_id = self.db.execute_query(
                get_booking_id,
                (player_id,)
            )[0]['max']

            return booking_id, predicted_score, price

        except Exception as e:
            raise Exception(f"Error creating booking: {e}")

if __name__ == "__main__":
    # test it works
    try:
        booking_system = RoundBooking()

        # test features: [round_number, handicap, avg_temp, precipitation, wind_speed, day_of_week_int]
        test_features = [40, 30, 80, 0.0, 12, 6]  # Monday, good weather, mid-handicap

        test_booking = booking_system.create_booking(
            name="Adam Long",
            date="2022-06-15",
            tee_time_hour=10,
            cart=True,
            features=test_features
        )

        print("Test Booking Success:", test_booking)

    except Exception as e:
        print(f"Test failed: {e}")