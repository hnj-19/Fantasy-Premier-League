from numpy.lib.index_tricks import _diag_indices_from
import pandas as pd 
import numpy as np
from os.path import dirname, join
import os

def import_merged_gw(season='2020-21'):
    """ Function to call merged_gw.csv file in every data/season folder
    Args:
        season (str): Name of the folder season that contains the merged_gw.csv file
    """

    path = os.getcwd()
    filename = 'merged_gw.csv'
    season_path = join(dirname(dirname("__file__")), path, 'data', season, 'gws', filename)
    return season_path

def clean_players_name_string(df, col='name'):
    """ Clean the imported file 'name' column because it has different patterns between seasons

    Args:
        df: merged df for all the seasons that have been imported
        col: name of the column for cleanup
    """
    #replace _ with space in name column
    df[col] = df[col].str.replace('_', ' ')
    #remove number in name column
    df[col] = df[col].str.replace('\d+', '')
    #trim name column
    df[col] = df[col].str.strip()
    return df

def filter_players_exist_latest(df, col='position'):
    """ Fill in null 'position' (data that only available in 20-21 season) into previous seasons. 
        Null meaning that player doesnt exist in latest season hence can exclude.
    """

    df[col] = df.groupby('name')[col].apply(lambda x: x.ffill().bfill())
    # df = df[df[col].notnull()]
    return df

def get_opponent_team_name(df):
    """ Find team name from master_team_list file and match with the merged df 
    """

    path = os.getcwd()
    filename = 'master_team_list.csv'
    team_path = join(dirname(dirname("__file__")), path, 'data', filename)
    df_team = pd.read_csv(team_path)

    #create id column for both df_team and df
    df['id'] = df['season'].astype(str) + '_' + df['opponent_team'].astype(str)
    df_team['id'] = df_team['season'].astype(str) + '_' + df_team['team'].astype(str)

    #merge two dfs
    df = pd.merge(df, df_team, on = 'id', how = 'left')

    #rename column
    df = df.rename(columns={"team_name": "opp_team_name"})
    return df

def export_cleaned_data(df):
    """ Function to export merged df into specified folder
    Args:
        path (str): Path of the folder
        filename(str): Name of the file
    """

    path = os.getcwd()
    filename = 'cleaned_merged_seasons.csv'
    filepath = join(dirname(dirname("__file__")), path, 'data', filename)
    df.to_csv(filepath, encoding = 'utf-8', index = False)
    return df 

def merge_position_data():
    """
    Get position/team data for older data
    """

    season_latin = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22']
    encoding_latin = ['latin', 'latin', 'latin', 'utf-8', 'utf-8', 'utf-8'] 
    for season,encoding in zip(season_latin, encoding_latin):
        df_gw = pd.read_csv(f'data/{season}/gws/merged_gw.csv', encoding = encoding)
        df_players = pd.read_csv(f'data/{season}/players_raw.csv')
        df_teams = pd.read_csv('data/master_team_list.csv')
        df_elements = pd.read_csv('data/element_types.csv')

        df_players = df_players[['element_type','id','team', 'web_name']]
        df_teams = df_teams[df_teams['season']==season]


        df_tmp = df_players.merge(df_teams, how = 'left', on = 'team')
        df_tmp = df_tmp.merge(df_elements, how = 'left', on = 'element_type')
        if season in ['2016-17', '2017-18', '2018-19', '2019-20']:
            df_tmp = df_tmp[['element_type','id','team_name','position', 'web_name']]
            df_tmp = df_tmp.rename(columns={'id': 'element', 'team_name': 'team'})
            df_tmp = df_tmp[['element','team','position', 'web_name']]
        else:
            df_tmp = df_tmp[['id','web_name']]
            df_tmp = df_tmp.rename(columns={'id': 'element'})
            df_tmp = df_tmp[['element','web_name']]
        
        df_gw = df_gw.merge(df_tmp, how = 'left', on = 'element')
        
        df_gw.to_csv(f'data/{season}/gws/merged_gw.csv', encoding = encoding, index = False)    

def main():
    merge_position_data()

if __name__ == '__main__':
    main()
