import re
from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
import boto3
import psycopg2
import json
from dotenv import load_dotenv

load_dotenv()

ENDPOINT = os.getenv('endpoint')
PORT = int(os.getenv('port', 5432))
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DBNAME = os.getenv('dbname')
REGION = os.getenv('region')


def data_cleaner(value, dtype=str):
    if value is None:
        return None

    value = value.strip()  # remove spaces
    if value in ("-", ""):
        return None

    if ":" in value and dtype in (int, float):
        try:
            minutes, seconds = value.split(":")
            value = f"{minutes}{seconds}"
        except ValueError:
            return None

    try:
        if dtype == int:
            return int(value)
        elif dtype == float:
            return float(value)
        else:
            return value
    except ValueError:
        return None

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def format_time(seconds_str):
    try:
        seconds = int(seconds_str)
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}:{secs:02d}"
    except (ValueError, TypeError):
        return None

def clean_text(text):
    return re.sub(r"[^\w.\s]", "", text)

def get_season_split_role_data():
    url = "https://gol.gg/players/list/season-S15/split-Spring/tournament-ALL/"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://gol.gg/",
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Failed to load page")
        return
    
    soup = BeautifulSoup(response.text, 'html.parser')
    region_tables = soup.find_all("table", class_="region_filter")

    role_soup = soup.find_all(attrs={"name": "role"})
    print(role_soup)

    if len(region_tables) < 3:
        print("Expected season, split, and role filters not found.")
        return

    # --- Extract Seasons ---
    season_table = region_tables[0]
    season_links = season_table.find_all("a")
    seasons = [re.search(r'season-(S\d+)', a['href']).group(1) for a in season_links if 'season-' in a['href']]

    # --- Extract Splits ---
    split_table = region_tables[1]
    split_links = split_table.find_all("a")
    splits = [re.search(r'split-([A-Za-z\-]+)', a['href']).group(1) for a in split_links if 'split-' in a['href']]

    # --- Extract Roles ---
    role_table = region_tables[2]
    role_links = role_table.find_all("a", attrs={"role-val": True})
    roles = [a['role-val'] for a in role_links if a['role-val'] != "ALL"]

    return {
        "seasons": seasons,
        "splits": splits,
        "roles": roles
    }

#helper func
def get_seasons():
    season_page = requests.get("https://gol.gg/tournament/list/")
    soup = BeautifulSoup(season_page.text, "html.parser")

    season_elements = soup.select("li.season-link a")
    seasons = []

    for elem in season_elements:
        onclick = elem.get("onclick", "")
        if "reloadTR" in onclick:
            season = onclick.split('"')[1]
            seasons.append(season)

    return seasons

#helper func
def load_tournament_names():

    try:
        conn = psycopg2.connect(
                                host=ENDPOINT, 
                                port=PORT, 
                                database=DBNAME, 
                                user=USER, 
                                password=PASSWORD
                            )
        cur = conn.cursor()
        cur.execute("SELECT tournament_name FROM tournament_staging;")
        rows = cur.fetchall()
        tournaments = [row[0] for row in rows]
        return tournaments

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return []
    
#helper func
def get_urls_from_db():
    try:
        conn = psycopg2.connect(
                                host=ENDPOINT, 
                                port=PORT, 
                                database=DBNAME, 
                                user=USER, 
                                password=PASSWORD
                            )
        cur = conn.cursor()
        cur.execute("SELECT game_url FROM match_staging;")
        rows = cur.fetchall()
        urls = [row[0] for row in rows]
        return urls

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return []

#Gets all tournaments and their basic details
#TODO: add Split
def get_tournaments():

    url = "https://gol.gg/tournament/ajax.trlist.php"

    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest",
        "referer": "https://gol.gg/tournament/list/"
    }

    seasons = get_seasons()
    raw_data = {}

    for season in seasons:
        response = requests.post(url, headers=headers, data={"season": season})
        if response.status_code != 200:
            print(f"Failed for {season}: {response.status_code}")
            continue
        raw_data[season] = response.json()

    return raw_data


#this is stats for each split/season TODO: for each individual split in a season(DONE)
# TODO: role base player data(rn no way of finding player role)
def get_players():
    data = get_season_split_role_data()

    seasons, splits, roles = data["seasons"], data["splits"], data["roles"]

    all_players = []

    headers_list = ["name", "link", "country", "games", "winrate", "kda", "avg_kills", "avg_deaths", "avg_assists", 
                "csm", "gpm", "kp", "dmg_pct", "dpm", "vspm", "wpm", "wcpm", "vwpm", "gd15", "csd15", "xpd15", "fb_pct", 
                "fb_victim_pct", "penta_kills", "solo_kills", "season", "split"]

    for i, season in enumerate(seasons):
        for split in splits:

            print(f"Fetching player stats for {season},{split} ...")

            url = f"https://gol.gg/players/list/season-{season}/split-{split}/tournament-ALL/"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Referer": "https://gol.gg/players/list/",
                "Accept-Language": "en-US,en;q=0.9",
            }

            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed for {season}: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find("table", class_="table_list playerslist tablesaw trhover")

            if not table:
                print(f"No player table found for {season}")
                continue

            rows = table.find_all("tr")[1:]
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 24:
                    continue

                name_tag = cols[0].find('a')
                name = name_tag.text.strip()
                link = name_tag['href']

                country = cols[1].find("span").text.strip() if cols[1].find("span") else cols[1].get_text(strip=True)

                values = [name, link, country] + [c.get_text(strip=True) for c in cols[2:24]] + [season, split]

                player_dict = dict(zip(headers_list, values))
                all_players.append(player_dict)
        
    all_players = json.dumps(all_players, indent=4)        

    return all_players


def get_teams():
    seasons = get_seasons()
    splits = ["All", "Pre-Season","Winter", "Spring"]

    all_teams = []

    headers_list = ["name", "region", "games", "winrate", "k:d", "GPM", "GDM", "gameDuration", "killsPerGame", 
                "deathsPerGame", "towersKilled", "towersLost", "FBpercent", "FTpercent", "FOSpercent", 
                "dragsPerGame", "dragPercent", "vgPerGame", "heraldPercent", "atakPercent", "avgDrags15", "TDat15", 
                "GDat15", "platesPerGame", "baronPergame", "baronPercent", "cspm", "dpm",
                "wpm", "visionWardsPM", "wardsClearedPM", "season", "split"] 

    for split in splits:
        for i, season in enumerate(seasons):
            if i <= 2:
                continue

            # print(f"Fetching team stats for {season},{split} ...")

            url = f"https://gol.gg/teams/list/season-{season}/split-{split}/tournament-ALL/"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Referer": "https://gol.gg/players/list/",
                "Accept-Language": "en-US,en;q=0.9",
            }

            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed for {season}: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find("table", class_="table_list playerslist tablesaw trhover")

            if not table:
                print(f"No team table found for {season}")
                continue

            rows = table.find_all("tr")[1:]

            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 24:
                    continue

                name_tag = cols[0].find('a')
                name = name_tag.text.strip()

                values = [name] + [c.get_text(strip=True) for c in cols[2:32]] + [season, split]

                team_dict = dict(zip(headers_list, values))
                all_teams.append(team_dict)
        
    all_teams = json.dumps(all_teams, indent=4)

    return all_teams

# gets the Best of info about a match series
def get_bo_(url):
    url = url.strip(".")
    url = f"https://gol.gg{url}"
    headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://gol.gg/game/stats/",
            "Accept-Language": "en-US,en;q=0.9",
        }

    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            print(f"Failed to fetch match summary: {url}")
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        bo_div = soup.find("div", class_="col-4 col-sm-2 text-center")
        if bo_div:
            h1 = bo_div.find("h1")
            if h1:
                bo_text = h1.text.strip()
                if bo_text.startswith("BO"):
                    return int(bo_text[2:])

        return None
    except Exception as e:
        print(f"Error fetching BO from summary: {e}")
        return None

# TODO: change output to json
def get_matches():

    tournaments = load_tournament_names()
    tournaments = tournaments[:2]
    matches = []

    for tournament in tournaments:

        url = f"https://gol.gg/tournament/tournament-matchlist/{tournament}/"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://gol.gg/tournament-matchlist/",
            "Accept-Language": "en-US,en;q=0.9",
        }    

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Failed for {tournament}: {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find("table", class_="table_list")

        if not table:
            print(f"No matches table found for {tournament}")
            continue

        rows = table.find_all("tr")[1:]

        for row in rows:
            cols = row.find_all('td')
            if not cols or len(cols) < 7:
                continue

            link_tag = cols[0].find('a')
            match_url = link_tag['href'].strip().replace("page-game", "page-summary") \
                if "page-game" in link_tag['href'] else link_tag['href'].strip()
            match_name = link_tag.text.strip()
            bo = get_bo_(match_url)

            team1_name = cols[1].text.strip()
            score = cols[2].text.strip()
            team2_name = cols[3].text.strip()
            match_type = cols[4].text.strip()
            patch = cols[5].text.strip()
            match_date = cols[6].text.strip()

            team1_class = cols[1].get('class', [])
            team2_class = cols[3].get('class', [])

            if 'text_victory' in team1_class:
                winner = team1_name
                loser = team2_name
            elif 'text_victory' in team2_class:
                winner = team2_name
                loser = team1_name
            else:
                winner = loser = None

            game_urls = get_game_url(match_url, score)

            match = {
                'match_name': match_name,
                'tournament': tournament,
                'match_url': match_url,
                'team1': team1_name,
                'team2': team2_name,
                'winner': winner,
                'loser': loser,
                'score': score,
                'match_type': match_type,
                'patch': patch,
                'date': match_date,
                'BO': bo,
                'game_urls': game_urls
            }

            matches.append(match)
    return matches

#gets urls for each game in a match series
def get_game_url(match_url=None, score=None):

    match = re.search(r'/game/stats/(\d+)/page-summary/', match_url)
    if not match:
        print("Invalid match URL format.")
    else:
        base_game_id = int(match.group(1))

        # Calculate number of games from score
        try:
            score_parts = [int(s.strip()) for s in score.split('-')]
            total_games = sum(score_parts)
        except:
            total_games = 1

        game_urls = [
            f"https://gol.gg/game/stats/{base_game_id + i}/page-game/"
            for i in range(total_games)
        ]

        return game_urls

#TODO: fetch game_urls from db
def get_game(game_url):

    headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://gol.gg/game/stats/",
            "Accept-Language": "en-US,en;q=0.9",
    }

    for index, game in enumerate(game_url):

        match_id = index+10

    for game in game_url:

        response = requests.get(game, headers=headers)

        if response.status_code != 200:
            print(f"Failed for {game}: {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        
        game_data = extract_team_game_data(soup, game)
        player_data = extract_player_game_data(soup, game, index)

        return (game_data , player_data)

#gets data for 1 game from a series of matches
def extract_team_game_data(soup, game_url):
    game_data = []

    teams = soup.select("div.row.rowbreak > div.col-12.col-sm-6")  # One for each side

    for side in teams:
        header = side.find("div", class_="blue-line-header") or side.find("div", class_="red-line-header")
        if not header:
            continue

        team_name = header.find("a").text.strip()
        result = "WIN" if "WIN" in header.text else "LOSS"

        # Basic stats: kills, towers, dragons, barons, gold
        stats_row = side.select_one("div.row[style*='min-height']")
        stats = stats_row.find_all("span", class_="score-box")
        
        kills = towers = dragons = barons = gold = None
        for span in stats:
            alt_text = span.find("img")["alt"]
            val = span.text.strip().split()[-1]

            if "Kills" in alt_text:
                kills = int(val)
            elif "Towers" in alt_text:
                towers = int(val)
            elif "Dragons" in alt_text:
                dragons = int(val)
            elif "Nashor" in alt_text:
                barons = int(val)
            elif "Gold" in alt_text:
                gold = val

        # First Blood / First Tower (check <img alt="First Blood"> or <img alt="First Tower">)
        firsts = stats_row.find_all("img", class_="champion_icon_Xlight")
        first_blood = any("Blood" in img["alt"] for img in firsts)
        first_tower = any("Tower" in img["alt"] for img in firsts)

        # Dragons types
        dragon_imgs = stats_row.find_all("img", class_="champion_icon_XS")
        dragon_types = [img["alt"] for img in dragon_imgs]

        rows = side.find_all("div", class_="row")
        # Bans
        bans = []
        # Picks
        picks = []

        for row in rows:
            label = row.find("div", class_="col-2")
            if not label:
                continue

            label_text = label.text.strip()

            if label_text == "Bans":
                ban_imgs = row.find("div", class_="col-10").find_all("img")
                bans = [img["alt"].strip() for img in ban_imgs if "alt" in img.attrs]

            elif label_text == "Picks":
                pick_imgs = row.find("div", class_="col-10").find_all("img")
                picks = [img["alt"].strip() for img in pick_imgs if "alt" in img.attrs]

        game_data.append({
            "team": team_name,
            "result": result,
            "kills": kills,
            "towers": towers,
            "dragons": dragons,
            "barons": barons,
            "gold": gold,
            "first_blood": first_blood,
            "first_tower": first_tower,
            "dragon_types": dragon_types,
            "bans": bans,
            "picks": picks,
            "game_url": game_url
        })

    return game_data

#gets players data from 1 game from a series
def extract_player_game_data(soup, game_url, game_number):
    players_data = []

    tables = soup.find_all("table", class_="playersInfosLine")
    
    if not tables or len(tables) < 2:
        return players_data

    for side, table in zip(['blue', 'red'], tables):  # assumes 2 tables: one for each team
        rows = table.find_all("tr")
        
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 4:
                continue
            
            table = soup.find("div", class_="row break")

            # First column: champion and player
            champ_tag = cols[0].select_one("a[href*='champion-stats']")
            player_tag = cols[0].select_one("a[href*='player-stats']")
            champion = champ_tag["title"].replace(" stats", "") if champ_tag else None
            player_name = player_tag.text.strip() if player_tag else None
            player_url = player_tag["href"] if player_tag else ""
            player_id = player_url.strip("/").split("/")[2] if player_url else None

            # KDA is in the second-last column
            kda_text = cols[-2].text.strip()
            try:
                kills, deaths, assists = map(int, kda_text.split('/'))
            except ValueError:
                kills = deaths = assists = 0  # fallback

            # CS is in the last column
            cs_text = cols[-1].text.strip()
            try:
                cs = int(cs_text.split()[0])
            except (ValueError, IndexError):
                cs = 0

            players_data.append({
                "game_url": game_url,
                "game_number": game_number,
                "player_name": player_name,
                "team_side": side,
                "champion": champion,
                "kills": kills,
                "deaths": deaths,
                "assists": assists,
                "cs": cs
            })

    return players_data  

if __name__ == "__main__":
    get_matches()