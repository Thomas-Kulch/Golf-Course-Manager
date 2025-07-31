"""
Author: Thomas Kulch
DS5110 - Final Project - Golf Course Manager

Frontend Flask application for Round Booking

Requirements
    -templates directory with index.html and dashboard.html
    -golf_analytics database setup
"""
from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # For server-side plotting
import matplotlib.pyplot as plt
import io
import base64
from datetime import datetime

from database import DatabaseManager
from round_booking import RoundBooking

app = Flask(__name__)
app.secret_key = 'test' # need secret key for this to work

# connect to db and initialize round booking system
db = DatabaseManager()
booking_system = RoundBooking()

@app.route('/', methods=['GET', 'POST'])
def index():
    """homepage"""
    if request.method == 'POST':
        # get player name on home page
        player_name = request.form['player_name']
        player_name = player_name.strip().title() # capitalize first letters of name
        return redirect(url_for('player_dashboard', name=player_name))

    return render_template('index.html')

@app.route('/player/<name>')
def player_dashboard(name):
    """dashboard for existing players"""
    # check if player exists
    player_query = "SELECT player_id, handicap FROM players WHERE player_name = %s"
    player_result = db.execute_query(player_query, (name,))

    if player_result:
        # existing player
        player_id = player_result[0]['player_id']
        handicap = player_result[0]['handicap']
        is_new_player = False

        # get performance charts for existing player
        charts = generate_player_charts(player_id)
    else:
        # new player
        player_id = None
        handicap = None
        is_new_player = True
        charts = None

    return render_template('dashboard.html',
                           player_name=name,
                           player_id=player_id,
                           handicap=handicap,
                           is_new_player=is_new_player,
                           charts=charts)

@app.route('/book', methods=['POST'])
def make_booking():
    """booking system"""
    # get players name from home page
    player_name = request.form['player_name']
    player_name = player_name.strip().title() # homogenize

    # get date from user
    date = request.form['booking_date']
    # get tee time from user
    time_hour = int(request.form['booking_time'])
    # if player selects the cart checkbox
    cart = 'cart' in request.form

    # handle new player
    if request.form.get('is_new_player') == 'true':
        # ask player for their estimated handicap
        handicap = float(request.form['handicap'])
        # add new player to database
        insert_player_query = "INSERT INTO players (player_name, handicap) VALUES (%s, %s) RETURNING player_id"
        db.execute_query(insert_player_query, (player_name, handicap))
    else:
        # get existing player's handicap
        player_query = "SELECT handicap FROM players WHERE player_name = %s"
        result = db.execute_query(player_query, (player_name,))
        handicap = result[0]['handicap']

    # get weather data for the date
    weather_features = get_weather_for_date(date)

    # create features for prediction
    day_of_week = datetime.strptime(date, '%Y-%m-%d').weekday()
    round_number = get_player_round_count(player_name) + 1

    # features array for score predictor
    features = [
        round_number,
        handicap,
        weather_features['avg_temp'],
        weather_features['precipitation'],
        weather_features['wind_speed'],
        day_of_week
    ]

    try:
        # create booking, predict score and get price
        booking_id, predicted_score, price = booking_system.create_booking(
            name=player_name,
            date=date,
            tee_time_hour=time_hour,
            cart=cart,
            features=features
        )

        # show user success flash - gives them the price they'll pay and their predicted score for the round
        flash(f"Booking confirmed! Cost: ${price}. Predicted score: {predicted_score}", 'success')
        return redirect(url_for('player_dashboard', name=player_name))

    except Exception as e:
        flash(f"Booking failed: {str(e)}", 'error')
        return redirect(url_for('player_dashboard', name=player_name))


def generate_player_charts(player_id):
    """generate performance charts for existing players"""
    # get player's round history from SQL
    rounds_query = """
        SELECT r.score, r.round_date, w.avg_temp, w.wind_speed, w.precipitation
        FROM rounds r
        LEFT JOIN weather w ON r.round_date = w.date
        WHERE r.player_id = %s
        ORDER BY r.round_date
    """

    rounds_data = db.execute_query(rounds_query, (player_id,))

    # early exit if playerh as no round data - will not show dashboard
    if not rounds_data:
        return None

    # convert to df
    df = pd.DataFrame(rounds_data)

    charts = {} # initialize charts dict

    # Score trend over time plot
    plt.figure(figsize=(10, 6))
    plt.plot(pd.to_datetime(df['round_date']), df['score'], marker='o')
    plt.title('Score Trend Over Time')
    plt.xlabel('Date')
    plt.ylabel('Score')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # convert to image
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png')
    img_buffer.seek(0)
    charts['score_trend'] = base64.b64encode(img_buffer.getvalue()).decode()
    plt.close()

    # Weather vs Score analysis subplots
    plt.figure(figsize=(12, 4))

    # temp vs score
    plt.subplot(1, 3, 1)
    plt.scatter(df['avg_temp'], df['score'])
    plt.xlabel('Temperature')
    plt.ylabel('Score')
    plt.title('Temperature vs Score')

    # wind vs score
    plt.subplot(1, 3, 2)
    plt.scatter(df['wind_speed'], df['score'])
    plt.xlabel('Wind Speed')
    plt.ylabel('Score')
    plt.title('Wind vs Score')

    # rain vs score
    plt.subplot(1, 3, 3)
    plt.scatter(df['precipitation'], df['score'])
    plt.xlabel('Precipitation')
    plt.ylabel('Score')
    plt.title('Rain vs Score')

    plt.tight_layout()

    # convert to image
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png')
    img_buffer.seek(0)
    charts['weather_analysis'] = base64.b64encode(img_buffer.getvalue()).decode()
    plt.close()

    return charts

def get_weather_for_date(date):
    """get weather data for the tee time date"""
    # query weather data from SQL
    weather_query = "SELECT avg_temp, precipitation, wind_speed FROM weather WHERE date = %s"
    result = db.execute_query(weather_query, (date,))

    # output weather features
    return {
            'avg_temp': result[0]['avg_temp'],
            'precipitation': result[0]['precipitation'],
            'wind_speed': result[0]['wind_speed']
        }

def get_player_round_count(player_name):
    """get number of rounds played by player"""
    # query round count from db for player
    count_query = """
        SELECT COUNT(*) as round_count 
        FROM rounds r 
        JOIN players p ON r.player_id = p.player_id 
        WHERE p.player_name = %s
    """
    result = db.execute_query(count_query, (player_name,))

    return result[0]['round_count'] if result else 0

if __name__ == '__main__':
    app.run()