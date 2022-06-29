import requests
import json
from bs4 import BeautifulSoup
import re
import codecs
import pandas as pd
import os
import csv

def get_data(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Response was code " + str(response.status_code))
    html = response.text
    parsed_html = BeautifulSoup(html, 'html.parser')
    scripts = parsed_html.findAll('script')
    filtered_scripts = []
    for script in scripts:
        if len(script.contents) > 0:
            filtered_scripts += [script]
    return scripts

def get_epl_data(season=2021):
    scripts = get_data(f"https://understat.com/league/EPL/{season}")
    teamData = {}
    playerData = {}
    datesData = {}
    for script in scripts:
        for c in script.contents:
            split_data = c.split('=')
            data = split_data[0].strip()
            if data == 'var teamsData':
                content = re.findall(r'JSON\.parse\(\'(.*)\'\)',split_data[1])
                decoded_content = codecs.escape_decode(content[0], "hex")[0].decode('utf-8')
                teamData = json.loads(decoded_content)
            elif data == 'var playersData':
                content = re.findall(r'JSON\.parse\(\'(.*)\'\)',split_data[1])
                decoded_content = codecs.escape_decode(content[0], "hex")[0].decode('utf-8')
                playerData = json.loads(decoded_content)
            elif data == 'var datesData':
                content = re.findall(r'JSON\.parse\(\'(.*)\'\)',split_data[1])
                decoded_content = codecs.escape_decode(content[0], "hex")[0].decode('utf-8')
                datesData = json.loads(decoded_content)
    datesData = pd.json_normalize(datesData, sep = '_')
    return teamData, playerData, datesData

def get_player_data(id):
    scripts = get_data("https://understat.com/player/" + str(id))
    groupsData = {}
    matchesData = {}
    shotsData = {}
    for script in scripts:
        for c in script.contents:
            split_data = c.split('=')
            data = split_data[0].strip()
            if data == 'var matchesData':
                content = re.findall(r'JSON\.parse\(\'(.*)\'\)',split_data[1])
                decoded_content = codecs.escape_decode(content[0], "hex")[0].decode('utf-8')
                matchesData = json.loads(decoded_content)
            elif data == 'var shotsData':
                content = re.findall(r'JSON\.parse\(\'(.*)\'\)',split_data[1])
                decoded_content = codecs.escape_decode(content[0], "hex")[0].decode('utf-8')
                shotsData = json.loads(decoded_content)
            elif data == 'var groupsData':
                content = re.findall(r'JSON\.parse\(\'(.*)\'\)',split_data[1])
                decoded_content = codecs.escape_decode(content[0], "hex")[0].decode('utf-8')
                groupsData = json.loads(decoded_content)
    return matchesData, shotsData, groupsData

def get_matches_data(season=2021):
    scripts = get_data(f"https://understat.com/league/EPL/{season}")
    datesData = {}
    for script in scripts:
        for c in script.contents:
            split_data = c.split('=')
            data = split_data[0].strip()
            if data == 'var datesData':
                content = re.findall(r'JSON\.parse\(\'(.*)\'\)',split_data[1])
                decoded_content = codecs.escape_decode(content[0], "hex")[0].decode('utf-8')
                datesData = json.loads(decoded_content)
    matchesData = pd.json_normalize(datesData, sep = '_')
    return matchesData

def get_xCS(match_id):
    scripts = get_data(f"https://understat.com/match/{match_id}")
    shotsData = {}
    for script in scripts:
        for c in script.contents:
            split_data = c.split('=')
            data = split_data[0].strip()
            if data == 'var shotsData':
                content = re.findall(r'JSON\.parse\(\'(.*)\'\)',split_data[1])
                decoded_content = codecs.escape_decode(content[0], "hex")[0].decode('utf-8')
                shotsData = json.loads(decoded_content)
    shotsData_h = pd.json_normalize(shotsData.get('h'), sep = '_')
    shotsData_a = pd.json_normalize(shotsData.get('a'), sep = '_')
    shotsData = pd.concat([shotsData_h, shotsData_a], ignore_index = True)
    shotsData['xCS'] = 1-shotsData.xG.astype('float64')
    
    xCS_h = shotsData[shotsData['h_a'] == 'a'].xCS.product()
    xCS_a = shotsData[shotsData['h_a'] == 'h'].xCS.product()
    
    xCS = pd.DataFrame({'match_id': match_id,
                        'xCS_h': xCS_h,
                        'xCS_a': xCS_a}, index=[0])
    
    return xCS

def parse_epl_data(outfile_base, season):
    season_short = season[0:4]
    teamData,playerData, datesData = get_epl_data(season_short)
    new_team_data = []
    for t,v in teamData.items():
        new_team_data += [v]
    for data in new_team_data:
        team_frame = pd.DataFrame.from_records(data["history"])
        team = data["title"].replace(' ', '_')
        team_frame['team'] = team
        team_frame.to_csv(os.path.join(outfile_base, 'teams/understat_' + team + '.csv'), index=False)
        print(team)
    player_frame = pd.DataFrame.from_records(playerData)
    player_frame.to_csv(os.path.join(outfile_base, 'understat_player.csv'), index=False)
    for d in playerData:
        matches, shots, groups = get_player_data(int(d['id']))
        indi_player_frame = pd.DataFrame.from_records(matches)
        indi_player_frame['Understat_ID'] = d['id']
        indi_player_frame['player_name'] = d['player_name']
        player_name = d['player_name']
        player_name = player_name.replace(' ', '_')
        indi_player_frame.to_csv(os.path.join(outfile_base, 'players/', player_name + '_' + d['id'] + '.csv'), index=False)
        print(player_name)
    understat_players_merge(season)
    
    print("xCS data")
    match_ids = get_matches_data(season_short)
    match_ids = match_ids[match_ids['isResult']==True]
    match_ids = match_ids[['id']]
    xCS = pd.DataFrame()
    for match_id in get_matches_data()['id']:
        df_tmp = get_xCS(match_id)
        xCS = pd.concat([xCS, df_tmp], ignore_index = True)
    xCS.to_csv(os.path.join(outfile_base, 'understat_xCS.csv'), index=False)
    
    datesData = pd.merge(datesData, xCS, how = 'left', on = 'match_id')
    datesData.to_csv(os.path.join(outfile_base, 'understat_fixtures.csv'), index=False)

class PlayerID:
    def __init__(self, us_id, fpl_id, us_name, fpl_name):
        self.us_id = str(us_id)
        self.fpl_id = str(fpl_id)
        self.us_name = us_name
        self.fpl_name = fpl_name
        

def match_ids(understat_dir, data_dir):
    with open(os.path.join(understat_dir, 'understat_player.csv')) as understat_file:
        understat_inf = csv.DictReader(understat_file)
        ustat_players = {}
        for row in understat_inf:
            ustat_players[row['player_name']] = row['id']

    with open(os.path.join(data_dir, 'player_idlist.csv')) as fpl_file:
        fpl_players = {}
        fpl_inf = csv.DictReader(fpl_file)
        for row in fpl_inf:
            fpl_players[row['first_name'] + ' ' + row['second_name']] = row['id']
    players = []
    found = {}
    for k, v in ustat_players.items():
        if k in fpl_players:
            player = PlayerID(v, fpl_players[k], k, k)
            players += [player]
            found[k] = True
        else:
            player = PlayerID(v, -1, k, "")
            players += [player]

    for k, v in fpl_players.items():
        if k not in found:
            player = PlayerID(-1, v, "", k)
            players += [player]

    with open(os.path.join(data_dir, 'id_dict.csv'), 'w+') as outf:
        outf.write('Understat_ID,FPL_ID,Understat_Name,FPL_Name\n')
        for p in players:
            outf.write(p.us_id + "," + p.fpl_id + "," + p.us_name + "," + p.fpl_name + "\n")

def understat_players_merge(season='2022-23'):
    df = pd.DataFrame()
    directory = os.path.join('data', season, 'understat/players')
    for root,dirs,files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                tmp = pd.read_csv(os.path.join(directory,file))
                df = pd.concat([df,tmp],axis=0)
    outfile = os.path.join('data', season, 'understat')
    df.to_csv(outfile + '/understat_players_merged.csv', index = False)
    return df

def main():
    #parse_epl_data('data/2022-23/understat/')
    #md, sd, gd = get_player_data(318)
    #match_frame = pd.DataFrame.from_records(md)
    #match_frame.to_csv('auba.csv', index=False)
    match_ids('data/2022-23/understat', 'data/2022-23')

if __name__ == '__main__':
    main()
