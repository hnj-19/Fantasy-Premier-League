import pandas as pd
import numpy as np
import requests
import lxml.html as lh
import math
import json
from bs4 import BeautifulSoup
import re
import backoff

def get_fbref_data(season="2022-23"):
    features = ['squad', 'comp', 'round', 'shots_total', 'shots_on_target', 'touches', 'pressures',
                'tackles', 'interceptions', 'blocks', 'xg', 'npxg', 'xa', 'sca', 
                'gca', 'passes_completed', 'passes', 'passes_pct', 'progressive_passes', 
                'carries', 'progressive_carries',
                'dribbles_completed', 'dribbles']
    
    # url for the premier league
    url_base = 'https://fbref.com'
    pl_history_page = '/en/comps/9/history/Premier-League-Seasons'
    url = url_base + pl_history_page

    # scrape the history table to get latest season
    pl_history_table = get_table(url)
    pl_history_urls = get_urls(['season'], 'season', pl_history_table, url_base)

    season_urls = pl_history_urls[0:1]
    df = get_stats(features, season_urls, url_base)
    df.to_csv(f'../fpl_data_fetch/data/{season}/fbref/fbref_data.csv')

## functions to scrape fbref data
# get stats (given by features) for all players/teams
# for season pages given by season_urls
def get_stats(features, season_urls, url_base):
    seasons_list = []
    
    for url, season_name in zip(season_urls['url'], season_urls['season']):
        print(season_name)
        print(url)
        season_table = get_table(url)
        team_urls = get_urls(['squad'], 'squad', season_table, url_base)
        season_df = get_season_stats(features, team_urls, url_base)
        season_df.insert(0, 'season', season_name)
        seasons_list.append(season_df)
    
    df = pd.concat(seasons_list, ignore_index=True)
    return df

# get stats (given by features) for all players 
# for team pages given by team_urls
def get_season_stats(features, team_urls, url_base):
    teams_list = []
    
    for url, team_name in zip(team_urls['url'], team_urls['squad']):
        print(team_name)
        print(url)
        team_table = get_table(url)
        player_urls = get_urls(['player', 'games'], 'matches', team_table, url_base)
        team_df = get_team_stats(features, player_urls)
        teams_list.append(team_df)
        
    df = pd.concat(teams_list, ignore_index=True)
    return df

# get stats (given by features)
# for all players given by player_urls
def get_team_stats(features, player_urls):
    players_list = []
    player_urls_played = player_urls[player_urls['games'] != '0']

    for url, player_name in zip(player_urls_played['url'], player_urls_played['player']):
        print(player_name)
        player_table = get_table(url)
        player_df = get_player_stats(features, player_table)
        player_df.insert(0, 'player', player_name)
        if len(player_df) > 0:
            players_list.append(player_df[player_df['comp'] == 'Premier League'])

    df = pd.concat(players_list, ignore_index=True)
    return df

# get game by game player stats for one season
# date always included by default
def get_player_stats(features, player_table):
    pre_dict = dict()    
    table_rows = player_table.find_all('tr')    
    for row in table_rows:
        if(row.find('td',{"data-stat":'xg'}) != None):
            date = row.find('th',{"data-stat":'date'}).text.strip().encode().decode("utf-8")
            if date != '':
                if 'date' in pre_dict:
                    pre_dict['date'].append(date)
                else:
                    pre_dict['date'] = [date]
                for f in features:
                    text = row.find('td',{"data-stat":f}).text.strip().encode().decode("utf-8")
                    if f in pre_dict:
                        pre_dict[f].append(text)
                    else:
                        pre_dict[f] = [text]
    df = pd.DataFrame.from_dict(pre_dict)
    return df

def backoff_hdlr(details):
    print ("Backing off {wait:0.1f} seconds afters {tries} tries "
           "calling function {target} with args {args} and kwargs "
           "{kwargs}".format(**details))

# get a table on any page, defaults to first one
@backoff.on_exception(
    backoff.expo,
    (IndexError, requests.exceptions.RequestException),
    max_tries=10,
    on_backoff=backoff_hdlr
)
def get_table(url, table_no=0):
    res = requests.get(url)
    ## The next two lines get around the issue with comments breaking the parsing.
    comm = re.compile("<!--|-->")
    soup = BeautifulSoup(comm.sub("",res.text),'lxml')
    all_tables = soup.findAll("tbody")    
    table = all_tables[table_no]
    return table

# get text and urls for a given field
def get_urls(text_fields, url_field, table, url_base):
    pre_dict = dict()    
    table_rows = table.find_all('tr')
    for row in table_rows:
        if(row.find('th',{"scope":"row"}) != None):
            for f in text_fields:
                if(row.find('th',{"data-stat":f}) != None):
                    text = row.find('th',{"data-stat":f}).text.strip().encode().decode("utf-8")
                else:
                    text = row.find('td',{"data-stat":f}).text.strip().encode().decode("utf-8")
                if f in pre_dict:
                    pre_dict[f].append(text)
                else: 
                    pre_dict[f] = [text]
                
            if row.find('th',{"data-stat":url_field}) != None:
                url = url_base + row.find('th',{"data-stat":url_field}).find('a').get('href')
#                 url = url_base + row.find('a')['href']
            else:
                url = url_base + row.find('td',{"data-stat":url_field}).find('a').get('href')

            if 'url' in pre_dict:
                pre_dict['url'].append(url)
            else: 
                pre_dict['url'] = [url]
                
    df = pd.DataFrame.from_dict(pre_dict)
    return df

def fbref_merge():
    seasons = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22']
    df = pd.DataFrame()
    for season in seasons:
        _df = pd.read_csv(f'data/{season}/fbref/fbref_data.csv')
        df = pd.concat([df,_df], ignore_index=False,sort=False)
    df.to_csv('data/fbref_merged.csv', index=False, encoding = 'utf-8')

def main():
    # get_fbref_data()
    fbref_merge()

if __name__ == "__main__":
    main()