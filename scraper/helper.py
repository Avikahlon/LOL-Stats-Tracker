import re
from bs4 import BeautifulSoup
import requests
import pandas as pd
from pprint import pprint

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def clean_text(text):
    return re.sub(r"[^\w.\s]", "", text)

def load_tournaments():
    return

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

#Gets all tournaments and their basic details
def get_tournaments():

    url = "https://gol.gg/tournament/ajax.trlist.php"

    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest",
        "referer": "https://gol.gg/tournament/list/"
    }

    seasons = get_seasons()

    tournaments_list = []

    for season in seasons:
        data = {
            "season": season
        }

        response = requests.post(url, headers=headers, data=data)

        if response.status_code != 200:
            print(f"Failed for {season}: {response.status_code}")
            continue
        
        tournaments = response.json()

        for t in tournaments:
            tournament = {
                'name': t.get('trname', '').strip(),
                'region': t.get('region', '').strip(),
                'number_of_games': int(t.get('nbgames', 0)),
                'game_duration': t.get('avgtime', '').strip(),
                'first_game': t.get('firstgame', '').strip(),
                'last_game': t.get('lastgame', '').strip(),
                'season': season
            }
            tournaments_list.append(tournament)

    return tournaments_list

#this is stats for each split/season TODO: for each individual split in a season(DONE)
# TODO: role base player data(rn no way of finding player role)
def get_players():
    data = get_season_split_role_data()

    seasons, splits, roles = data["seasons"], data["splits"], data["roles"]

    all_players = []

    for split in splits:
        for i, season in enumerate(seasons):
            if i <= 2:
                continue

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
                player = {
                    'name': name_tag.text.strip() if name_tag else '',
                    'link': name_tag['href'].strip() if name_tag else '',
                    'country': cols[1].text.strip(),
                    'games': int(cols[2].text.strip()),
                    'winrate': cols[3].text.strip(),
                    'kda': cols[4].text.strip(),
                    'avg_kills': cols[5].text.strip(),
                    'avg_deaths': cols[6].text.strip(),
                    'avg_assists': cols[7].text.strip(),
                    'csm': cols[8].text.strip(),
                    'gpm': cols[9].text.strip(),
                    'kp': cols[10].text.strip(),
                    'dmg_pct': cols[11].text.strip(),
                    'dpm': cols[12].text.strip(),
                    'vspm': cols[13].text.strip(),
                    'wpm': cols[14].text.strip(),
                    'wcpm': cols[15].text.strip(),
                    'vwpm': cols[16].text.strip(),
                    'gd15': cols[17].text.strip(),
                    'csd15': cols[18].text.strip(),
                    'xpd15': cols[19].text.strip(),
                    'fb_pct': cols[20].text.strip(),
                    'fb_victim_pct': cols[21].text.strip(),
                    'penta_kills': cols[22].text.strip(),
                    'solo_kills': cols[23].text.strip(),
                    'season': season,
                    'split': split
                }

                all_players.append(player)

    return all_players

def get_teams():
    seasons = get_seasons()
    splits = ["All", "Pre-Season","Winter", "Spring"]

    all_teams = []

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
                team = {
                    'name': name_tag.text.strip() if name_tag else '',
                    'season': cols[1].text.strip(),
                    'region': cols[2].text.strip(),
                    'games': cols[3].text.strip(),
                    'win-rate': cols[4].text.strip(),
                    'k:d': cols[5].text.strip(),
                    'GPM': cols[6].text.strip(),
                    'GDM': cols[7].text.strip(),
                    'gameDuration': cols[8].text.strip(),
                    'killsPerGame': cols[9].text.strip(),
                    'deathsPerGame': cols[10].text.strip(),
                    'towersKilled': cols[11].text.strip(),
                    'towersLost': cols[12].text.strip(),
                    'FBpercent': cols[13].text.strip(),
                    'FTpercent': cols[14].text.strip(),
                    'FOSpercent': cols[15].text.strip(),
                    'dragsPerGame': cols[16].text.strip(),
                    'dragPercent': cols[17].text.strip(),
                    'vgPerGame': cols[18].text.strip(),
                    'heraldPercent': cols[19].text.strip(),
                    'atakPercent': cols[20].text.strip(),
                    'avgDrags15': cols[21].text.strip(),
                    'TDat15': cols[22].text.strip(),
                    'GDat15': cols[23].text.strip(),
                    'platesPerGame': cols[24].text.strip(),
                    'baronPergame': cols[25].text.strip(),
                    'baronPercent': cols[26].text.strip(),
                    'cspm': cols[27].text.strip(),
                    'dpm': cols[28].text.strip(),
                    'wpm': cols[29].text.strip(),
                    'visionWardsPM': cols[30].text.strip(),
                    'wardsClearedPM': cols[31].text.strip(),
                    'season': season,
                    'split': split
                }

                all_teams.append(team)

    return all_teams

# gets the Best of info about a match series
def get_bo_(url):
    url = url.strip(".")
    url = f"https://gol.gg{url}"
    print(url)
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

def get_matches():

    # tournaments = load_tournament_names()
    tournaments = ["EMEA Masters 2025 Winter"]

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
            match_url = link_tag['href'].strip()
            match_name = link_tag.text.strip()
            bo = get_bo_(match_url)

            team1_name = cols[1].text.strip()
            score = cols[2].text.strip()
            team2_name = cols[3].text.strip()
            match_type = cols[4].text.strip()
            match_duration = cols[5].text.strip()
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
                winner = loser = None  # Draw or unknown

            match = {
                'match_name': match_name,
                'match_url': match_url,
                'team1': team1_name,
                'team2': team2_name,
                'winner': winner,
                'loser': loser,
                'score': score,
                'match_type': match_type,
                'patch': match_duration,
                'date': match_date,
                'BO': bo
            }

            matches.append(match)
    return matches

def get_game_url(match_url=None, score=None):

    match_url = ""

    match_url = "/game/stats/65055/page-summary/"
    score = "3 - 0"

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

def get_game(games):

    headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://gol.gg/game/stats/",
            "Accept-Language": "en-US,en;q=0.9",
    }

    for index, game in enumerate(games):

        match_id= index+10
    for game in games:

        response = requests.get(game, headers=headers)

        if response.status_code != 200:
            print(f"Failed for {game}: {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        
        game_data = extract_team_game_data(soup)
        player_data = extract_player_game_data(soup, match_id, index)

        return (game_data , player_data)

#gets data for 1 game froma series of matches
def extract_team_game_data(soup):
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
            "picks": picks
        })

    return game_data

#gets players data from 1 game from a series
def extract_player_game_data(soup, match_id, game_number):
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
                "match_id": match_id,
                "game_number": game_number,
                "player_id": player_id,
                "player_name": player_name,
                "team_side": side,
                "champion": champion,
                "kills": kills,
                "deaths": deaths,
                "assists": assists,
                "cs": cs
            })

    return players_data




if __name__=="__main__":

    base_url = "https://gol.gg/players/list/season-S15/split-ALL/" 

    print(get_season_split_role_data())

    # games=get_game_url(match_url = "/game/stats/65055/page-summary/",
    #     score = "3 - 0")

    # player, game = get_game(games)

    # df =  pd.DataFrame.from_dict(game)
    # print(df)

    # print("\n=== Seasons ===")
    # seasons = get_seasons()
    # pprint(seasons)

    # print("\n=== Tournaments ===")
    # tournaments = get_tournaments()
    # pprint(tournaments)

    # print("\n=== Players ===")
    # players = get_players()
    # pprint(players[:3])  # Print just first 3 for brevity

    # print("\n=== Teams ===")
    # teams = get_teams()
    # pprint(teams[:3])  # First 3

    # print("\n=== Matches ===")
    # matches = get_matches()
    # pprint(matches)

    # print("\n=== Game URLs ===")
    # if matches:
    #     game_urls = get_game_url(match_url=matches[0]['match_url'], score=matches[0]['score'])
    #     pprint(game_urls)

    #     print("\n=== Game Data and Player Data ===")
    #     game_data, player_data = get_game(game_urls)
    #     pprint(game_data)
    #     pprint(player_data)