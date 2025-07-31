/*
 Author: Thomas Kulch
 DS5110 - Final Project - Golf Course Manager

 Golf Course Manager Queries for Database
 */

-- Player performance summary
SELECT
    p.player_name,
    p.member_status,
    p.handicap,
    COUNT(r.round_id) as total_rounds,
    AVG(r.score) as avg_score,
    MIN(r.score) as best_score,
    MAX(r.score) as worst_score
FROM players p
LEFT JOIN rounds r ON p.player_id = r.player_id
GROUP BY p.player_id, p.player_name, p.member_status, p.handicap
ORDER BY avg_score;

-- Weather impact on scores
SELECT
    CASE
        WHEN w.precipitation > 0.5 THEN 'Rainy'
        WHEN w.wind_speed > 15 THEN 'Windy'
        WHEN w.avg_temp < 45 OR w.avg_temp > 95 THEN 'Extreme Weather'
        ELSE 'Good Conditions'
    END as weather_category,
    COUNT(*) as rounds_played,
    AVG(r.score) as avg_score,
    MIN(r.score) as best_score,
    MAX(r.score) as worst_score
FROM rounds r
JOIN weather w ON r.round_date = w.date
GROUP BY weather_category
ORDER BY avg_score;

-- Monthly trends
SELECT
    EXTRACT(YEAR FROM round_date) as year,
    EXTRACT(MONTH FROM round_date) as month,
    COUNT(*) as rounds_played,
    AVG(score) as avg_score
FROM rounds
GROUP BY year, month
ORDER BY year, month;


-- Top performers by conditions
SELECT
    p.player_name,
    CASE
        WHEN w.precipitation > 0.5 THEN 'Rainy'
        WHEN w.wind_speed > 15 THEN 'Windy'
        ELSE 'Good Conditions'
    END as conditions,
    COUNT(*) as rounds_played,
    AVG(r.score) as avg_score
FROM players p
JOIN rounds r ON p.player_id = r.player_id
JOIN weather w ON r.round_date = w.date
GROUP BY p.player_name, conditions
HAVING COUNT(*) >= 3
ORDER BY conditions, avg_score;