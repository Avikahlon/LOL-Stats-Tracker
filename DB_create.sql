-- 1. Teams
CREATE TABLE teams (
	team_id SERIAL PRIMARY KEY,
	team_name VARCHAR(255) NOT NULL,
	region VARCHAR(255) NOT NULL
);

-- 2. Players
CREATE TABLE players (
	player_id SERIAL PRIMARY KEY,
	player_name VARCHAR(255) NOT NULL,
	player_nationality VARCHAR(255),
	player_role VARCHAR(100) NOT NULL CHECK (player_role IN ('Top', 'Jungle', 'Mid', 'ADC', 'Support')),
	team_id INT REFERENCES teams(team_id)
);

-- 3. Tournaments
CREATE TABLE tournaments (
	tournament_id SERIAL PRIMARY KEY,
	tournament_name VARCHAR(255) NOT NULL,
	region VARCHAR(255) NOT NULL,
	start_date DATE NOT NULL,
	end_date DATE NOT NULL,
	patch VARCHAR(100),  -- FIXED typo: VACRHAR -> VARCHAR
	number_of_games INT,
	season VARCHAR(20)
);

-- 4. Matches
CREATE TABLE matches (
	match_id SERIAL PRIMARY KEY,
	match_name VARCHAR(255),
	tournament_id INT REFERENCES tournaments(tournament_id),
	match_date DATE NOT NULL,
	team1_id INT REFERENCES teams(team_id),
	team2_id INT REFERENCES teams(team_id),
	winner_id INT REFERENCES teams(team_id),
	best_of INT NOT NULL
);

-- 5. Games
CREATE TABLE games (
	game_id SERIAL PRIMARY KEY,
	match_id INT REFERENCES matches(match_id),
	game_number INT NOT NULL,
	blue_team_id INT REFERENCES teams(team_id),
	red_team_id INT REFERENCES teams(team_id),
	winner_team_id INT REFERENCES teams(team_id),
	duration_seconds INT NOT NULL
);

-- 6. Champions
CREATE TABLE champions (
	champion_id SERIAL PRIMARY KEY,
	champion_name VARCHAR(255) NOT NULL
);

-- 7. Player Game Stats
CREATE TABLE playerGameStats (
	player_game_id SERIAL PRIMARY KEY,
	game_id INT REFERENCES games(game_id),
	player_id INT REFERENCES players(player_id),
	champion_id INT REFERENCES champions(champion_id),
	team_id INT REFERENCES teams(team_id),
	kills INT,
	deaths INT,
	assists INT, 
	cs INT,
	gold_earned INT,
	damage_dealt INT,
	wards_placed INT, 
	wards_killed INT,
	vision_score INT,
	result VARCHAR(10) NOT NULL
);

-- 8. Bans
CREATE TABLE bans (
	ban_id SERIAL PRIMARY KEY,
	game_id INT REFERENCES games(game_id),
	team_id INT REFERENCES teams(team_id),
	champion_id INT REFERENCES champions(champion_id),
	ban_order INT CHECK (ban_order BETWEEN 1 AND 5)
);

--Checking if tables created
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE';
  