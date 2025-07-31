/*
 Author: Thomas Kulch
 DS5110 - Final Project - Golf Course Manager

 Golf Course Manager Database Schema
 Create tables, indexes, functions and triggers
 */

-- Players table
CREATE TABLE IF NOT EXISTS players (
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(100) UNIQUE NOT NULL,
    member_status VARCHAR(20) DEFAULT 'guest',
    handicap FLOAT,
    rounds_played INTEGER DEFAULT 0
);

-- Weather table
CREATE TABLE IF NOT EXISTS weather (
    date DATE PRIMARY KEY,
    avg_temp FLOAT,
    precipitation FLOAT,
    wind_speed FLOAT,
    day_of_week VARCHAR(20),
    day_of_week_int INTEGER
);

-- Rounds table
CREATE TABLE IF NOT EXISTS rounds (
    round_id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES players(player_id),
    player_name VARCHAR(100) NOT NULL,
    round_date DATE REFERENCES weather(date),
    score INTEGER NOT NULL,
    round_number INTEGER
);

-- Bookings table
CREATE TABLE IF NOT EXISTS bookings (
    booking_id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES players(player_id),
    tee_time TIMESTAMP NOT NULL,
    price_paid FLOAT NOT NULL DEFAULT 75,
    booking_status VARCHAR(20) DEFAULT 'confirmed',
    round_date DATE NOT NULL,
    score_prediction FLOAT DEFAULT NULL,
    booking_time TIMESTAMP NOT NULL
);

-- indexes for performance
CREATE INDEX IF NOT EXISTS idx_players_name ON players(player_name);
CREATE INDEX IF NOT EXISTS idx_weather_date ON weather(date);
CREATE INDEX IF NOT EXISTS idx_rounds_player_round ON rounds(player_id, round_number);

-- Triggers and Functions
CREATE OR REPLACE FUNCTION update_player_stats()
RETURNS TRIGGER AS $$
BEGIN
    -- update rounds_played count for each player
    UPDATE players
    SET rounds_played = (
        SELECT COUNT(*)
        FROM rounds
        WHERE player_id = NEW.player_id
    )
    WHERE player_id = NEW.player_id;

    -- member status update if they've played 30+ rounds
    UPDATE players
    SET member_status = 'member'
    WHERE player_id = NEW.player_id
    AND (SELECT COUNT(*) FROM rounds WHERE player_id = NEW.player_id) >= 30
    AND member_status = 'guest';

    -- update handicap (calculate from past 20 rounds)
	UPDATE players
    SET handicap = calculate_handicap_for_player(NEW.player_id)
    WHERE player_id = NEW.player_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- trigger to update player stats when rounds are inserted
CREATE TRIGGER update_player_stats_trigger
    AFTER INSERT ON rounds
    FOR EACH ROW
    EXECUTE FUNCTION update_player_stats();

-- function to calculate handicap for a specific player
CREATE OR REPLACE FUNCTION calculate_handicap_for_player(p_player_id INTEGER)
RETURNS FLOAT AS $$
DECLARE
    average_differential FLOAT;
    rounds_count INTEGER;
BEGIN
    -- Count how many rounds this player has
    SELECT COUNT(*) INTO rounds_count
    FROM rounds
    WHERE player_id = p_player_id;

    -- if no rounds, return NULL
    IF rounds_count = 0 THEN
        RETURN NULL;
    END IF;

    -- calculate average differential from most recent rounds
    SELECT AVG(ROUND(((score - 67.3) * 113.0 / 119.0)::NUMERIC, 1)) INTO average_differential
    FROM (
        SELECT score
        FROM rounds
        WHERE player_id = p_player_id
        ORDER BY round_date DESC
        LIMIT 20
    ) recent_scores;

    -- Apply your 0.96 factor and round
    RETURN ROUND((average_differential::NUMERIC * 0.96), 1);

END;
$$ LANGUAGE plpgsql;

-- Function to assign round numbers based on dates of rounds
CREATE OR REPLACE FUNCTION assign_round_numbers()
RETURNS TRIGGER AS $$
BEGIN
    -- update round_number for all rounds of this player ordered by round_date
    WITH ordered_rounds AS (
        SELECT round_id,
               ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY round_date ASC) AS rn
        FROM rounds
        WHERE player_id = NEW.player_id
    )
    UPDATE rounds
    SET round_number = ordered_rounds.rn
    FROM ordered_rounds
    WHERE rounds.round_id = ordered_rounds.round_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger to assign round numbers on insert
CREATE TRIGGER trigger_assign_round_numbers
AFTER INSERT ON rounds
FOR EACH ROW
EXECUTE FUNCTION assign_round_numbers();

-- Function for adjusting scores
-- this is done because the scores from the raw data do not take any of this into account
-- this could help with LR model accuracy
CREATE OR REPLACE FUNCTION adjust_score_for_round()
RETURNS TRIGGER AS $$
DECLARE
    weather_data RECORD;
    adjusted_score INTEGER;
BEGIN
    -- start with the base score
    adjusted_score := NEW.score;

    -- get weather data for the round date
    SELECT avg_temp, precipitation, wind_speed, day_of_week_int
    INTO weather_data
    FROM weather
    WHERE date = NEW.round_date;

    -- temperature adjustments
    IF weather_data.avg_temp < 50 THEN
        adjusted_score := adjusted_score + 4;
    ELSIF weather_data.avg_temp > 95 THEN
        adjusted_score := adjusted_score + 4;
    ELSIF weather_data.avg_temp > 80 THEN
        adjusted_score := adjusted_score - 1;
    END IF;

    -- wind adjustments
    IF weather_data.wind_speed > 15 THEN
        adjusted_score := adjusted_score + 4;
    ELSIF weather_data.wind_speed > 10 THEN
        adjusted_score := adjusted_score + 3;
    ELSIF weather_data.wind_speed < 3 THEN
        adjusted_score := adjusted_score - 1;
    END IF;

    -- precipitation adjustments
    IF weather_data.precipitation > 10 THEN
        adjusted_score := adjusted_score + 5;
    ELSIF weather_data.precipitation > 0 THEN
        adjusted_score := adjusted_score + 3;
    ELSIF weather_data.precipitation = 0 THEN
        adjusted_score := adjusted_score - 1;
    END IF;

    -- day of week adjustments
    IF weather_data.day_of_week_int IN (5, 6) THEN
        adjusted_score := adjusted_score - 1;
    END IF;
    IF weather_data.day_of_week_int IN (0, 1) THEN
        adjusted_score := adjusted_score + 2;
    END IF;

    NEW.score := adjusted_score;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- trigger to activate adjust score function on record insert
CREATE TRIGGER trigger_adjust_score
    BEFORE INSERT OR UPDATE ON rounds
    FOR EACH ROW
    EXECUTE FUNCTION adjust_score_for_round();