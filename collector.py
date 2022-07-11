from asyncio import SubprocessTransport
from distutils.command.build import build
from operator import index
import os
import sys
import csv
from xml.dom.minidom import Element
import pandas as pd
import numpy as np

def get_teams(directory):
    teams = {}
    fin = open(directory + "/teams.csv", 'rU')
    reader = csv.DictReader(fin)
    for row in reader:
        teams[int(row['id'])] = row['name']
    return teams


def get_fixtures(directory):
    fixtures_home = {}
    fixtures_away = {}
    fin = open(directory + "/fixtures.csv", 'rU')
    reader = csv.DictReader(fin)
    for row in reader:
        fixtures_home[int(row['id'])] = int(row['team_h'])
        fixtures_away[int(row['id'])] = int(row['team_a'])
    return fixtures_home, fixtures_away


def get_positions(directory):
    positions = {}
    names = {}
    pos_dict = {'1': "GK", '2': "DEF", '3': "MID", '4': "FWD"}
    fin = open(directory + "/players_raw.csv", 'rU',encoding="utf-8")
    reader = csv.DictReader(fin)
    for row in reader:
        positions[int(row['id'])] = pos_dict[row['element_type']] 
        names[int(row['id'])] = row['first_name'] + ' ' + row['second_name']
    return names, positions

def get_expected_points(gw, directory):
    xPoints = {}
    fin = open(os.path.join(directory, 'xP' + str(gw) + '.csv'), 'rU')
    reader = csv.DictReader(fin)
    for row in reader:
        xPoints[int(row['id'])] = row['xP']
    return xPoints

def merge_gw(gw, gw_directory, encoding = 'utf-8'):
    df_merged = pd.DataFrame()
    merged_gw_filename = "merged_gw.csv"
    out_path = os.path.join(gw_directory, merged_gw_filename)
    for i in range(1,gw + 1):

    
        gw_filename = "gw" + str(i) + ".csv"
        gw_path = os.path.join(gw_directory, gw_filename)
        df = pd.read_csv(gw_path, encoding=encoding)
        df['GW'] = i
        pd.concat([df_merged,df],ignore_index=True, sort=False)
    
    df_merged.to_csv(out_path, index=False)
    # fin = open(gw_path, 'rU', encoding=encoding)
    # reader = csv.DictReader(fin)
    # fieldnames = reader.fieldnames
    # fieldnames += ["GW"]
    # rows = []
    # for row in reader:
    #     row["GW"] = gw
    #     rows += [row]
    # out_path = os.path.join(gw_directory, merged_gw_filename)
    # fout = open(out_path,'a', encoding=encoding) #change back to a
    # writer = csv.DictWriter(fout, fieldnames=fieldnames, lineterminator='\n')
    # print(gw)
    # if gw == 1:
    #     writer.writeheader()
    # for row in rows:
    #     writer.writerow(row)

    return(df)


def collect_gw(gw, directory_name, output_dir, root_directory_name="data/2021-22"):
    rows = []
    fieldnames = []
    fixtures_home, fixtures_away = get_fixtures(root_directory_name)
    teams = get_teams(root_directory_name)
    names, positions = get_positions(root_directory_name)
    xPoints = get_expected_points(gw, output_dir)
    for root, dirs, files in os.walk(u"./" + directory_name):
        for fname in files:
            if fname == 'gw.csv':
                fpath = os.path.join(root, fname)
                fin = open(fpath, 'rU')
                reader = csv.DictReader(fin)
                fieldnames = reader.fieldnames
                for row in reader:
                    if int(row['round']) == gw:
                        id = int(os.path.basename(root).split('_')[-1])
                        name = names[id]
                        position = positions[id]
                        fixture = int(row['fixture'])
                        if row['was_home'] == True or row['was_home'] == "True":
                            row['team'] = teams[fixtures_home[fixture]]
                        else:
                            row['team'] = teams[fixtures_away[fixture]]
                        row['name'] = name
                        row['position'] = position
                        if id in xPoints:
                            row['xP'] = xPoints[id]
                        else:
                            row['xP'] = 0.0
                        rows += [row]

    fieldnames = ['name', 'position', 'team', 'xP'] + fieldnames
    outf = open(os.path.join(output_dir, "gw" + str(gw) + ".csv"), 'w', encoding="utf-8")
    writer = csv.DictWriter(outf, fieldnames=fieldnames, lineterminator='\n')
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

def collect_all_gws(directory_name, output_dir):
    for i in range(1,5):
        collect_gw(i, directory_name, output_dir)

def merge_all_gws(num_gws, gw_directory, encoding = 'utf-8'):
    merged_gw_filename = "merged_gw.csv"
    out_path = os.path.join(gw_directory, merged_gw_filename)
    df = pd.DataFrame()
    
    for i in range(1, num_gws):
        df_tmp = merge_gw(i, gw_directory, encoding)
        df = pd.concat([df, df_tmp])
    
    df.to_csv(out_path, encoding = encoding, index = False)

def main():
    #collect_all_gws(sys.argv[1], sys.argv[2])
    # merge_all_gws(int(sys.argv[1]), sys.argv[2])
    # collect_gw(37, sys.argv[1], sys.argv[2])
    merge_all_gws(39, 'data/2021-22/gws')

def build_players(season):
    # read in player information for each season and add to list
    season_players = []

    players = pd.read_csv(f'data/{season}/players_raw.csv', 
                            usecols=['first_name', 'second_name', 'web_name', 'id', 
                                    'element_type', 'now_cost', 'team',
                                    'chance_of_playing_this_round', 'chance_of_playing_next_round', 'status'])
    season_players.append(players)

    # create full name field for each player
    for players in season_players:
        players['full_name'] = players['first_name'] + ' ' + players['second_name']
        players.drop(['first_name', 'second_name'], axis=1, inplace=True)

    # create series of all unique player names
    all_players = pd.concat(season_players, axis=0, ignore_index=True, sort=False)
    all_players = pd.DataFrame(all_players['full_name'].drop_duplicates())
    all_players['season'] = season

    # create player dataset with their id, team code and position id for each season
    for players in season_players:
        all_players = all_players.merge(players, on='full_name', how='left')

    element_type_dict = {
            1: 'GK',
            2: 'DEF',
            3: 'MID',
            4: 'FWD'
        }
    all_players['position'] = all_players['element_type'].replace(element_type_dict) 
    all_players = all_players.drop(columns=['element_type'])
    all_players = all_players.rename(columns={'id': 'element'})
    return all_players

def collect_fixtures(season, gw):
    # get fixtures
    fixtures = pd.read_csv(f'data/{season}/fixtures.csv')
    teams = pd.read_csv(f'data/{season}/teams.csv')
    team_dict = teams[['id', 'name']].dropna().set_index('id').to_dict()['name']

    fixtures['home_team'] = fixtures['team_h'].replace(team_dict)
    fixtures['away_team'] = fixtures['team_a'].replace(team_dict)
    fixtures = fixtures[['home_team', 'away_team', 'team_h','team_a', 'kickoff_time', 'event']]
    fixtures = fixtures.rename(columns={'event':'gw'})


    # use these two lines if updating play_proba later in week
    # current_gw = last_gw
    # df_train_new = pd.read_csv(path/'train_v7.csv', index_col=0, dtype={'season':str,
    #                                                                     'comp':str,
    #                                                                     'squad':str})
 
    # set starting gameweek (where are we right now in the season)
    current_gw = gw + 1
    fixtures = fixtures[fixtures['gw'] >= current_gw].sort_values(by=['gw'])
    fixtures['match_no'] = np.arange(fixtures.shape[0])
    
    # add universal team codes for home and away teams
    fixtures = fixtures.merge(teams, left_on='home_team', right_on='name', how='left')
    fixtures = fixtures.merge(teams, left_on='away_team', right_on='name', how='left')
    fixtures = fixtures[['gw', 'match_no', 'home_team', 'away_team', 'team_h','team_a', 'kickoff_time']]
    
    return fixtures

def collect_remaining_season(season, gw):
    # join home team to all players for current season
    fixtures = collect_fixtures(season, gw)
    all_players = build_players(season)
    home_df = fixtures.merge(all_players, 
                left_on='team_h', 
                right_on='team', 
                how='left')
    home_df['team'] = home_df['home_team']

    # pull out the required fields and rename columns
    home_df = home_df[['season', 'gw', 'match_no', 'home_team', 'away_team', 'kickoff_time', 
                    'full_name', 'element', 'team', 'position', 'now_cost', 'chance_of_playing_this_round', 
                    'chance_of_playing_next_round', 'status', 'web_name']]
    

    # add home flag
    home_df['was_home'] = True

    # join away team to all players for current season
    away_df = fixtures.merge(all_players, 
                left_on='team_a', 
                right_on='team', 
                how='left')
    away_df['team'] = away_df['away_team']

    # pull out the required fields and rename columns
    away_df = away_df[['season', 'gw',  'match_no', 'home_team', 'away_team', 'kickoff_time', 
                    'full_name', 'element', 'team', 'position', 'now_cost', 'chance_of_playing_this_round', 
                    'chance_of_playing_next_round','status', 'web_name']]

    # add home flag
    away_df['was_home'] = False

    # concatenate home and away players
    remaining_season_df = pd.concat([home_df,away_df], ignore_index=True).reset_index(drop=True)

    # divide cost by 10 for actual cost
    remaining_season_df['price'] = remaining_season_df['now_cost']/10

    # set availability probability
    # 0 = 0% chance, 25 = 25% chance, etc
    # 'None' or '100' = 100% chance
    remaining_season_df.loc[remaining_season_df['chance_of_playing_this_round'] == 'None', 'chance_of_playing_this_round'] = 100
    remaining_season_df['chance_of_playing_this_round'] = remaining_season_df['chance_of_playing_this_round'].astype('float')
    remaining_season_df.loc[remaining_season_df['chance_of_playing_next_round'] == 'None', 'chance_of_playing_next_round'] = 100
    remaining_season_df['chance_of_playing_next_round'] = remaining_season_df['chance_of_playing_next_round'].astype('float')
    
    remaining_season_df.to_csv(f'data/{season}/remaining_season.csv', index=False)
    
    return remaining_season_df

def collect_play_probabilities(season, gw):
    df_play_probs_merged = pd.read_csv('data/play_probabilities.csv')
    df_play_probs_current = pd.read_csv(f'data/{season}/remaining_season.csv')
    df_play_probs_current['date'] = pd.to_datetime(df_play_probs_current['kickoff_time']).dt.date
    df_play_probs_current = df_play_probs_current[df_play_probs_current['gw'] == gw]

    df_play_probs_current = df_play_probs_current[['full_name', 'season', 'chance_of_playing_this_round','date', 'element']]
    df_play_probs_current = df_play_probs_current.rename(columns={'full_name':'player'})
    df_play_probs_current = df_play_probs_current.drop_duplicates(subset=['element'])
    
    df = pd.concat([df_play_probs_merged, df_play_probs_current], ignore_index= True)
    df.to_csv('data/play_probabilities.csv', index=False)

def main():
    # collect_remaining_season('2022-23', 0)
    collect_play_probabilities('2022-23', 0)

if __name__ == '__main__':
    main()
