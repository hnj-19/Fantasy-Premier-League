from mergers import *
import pandas as pd

def merge_data():
    """ Merge all the data and export to a new file
    """
    season_latin = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22'] 
    encoding_latin = ['latin-1', 'latin-1', 'latin-1', 'utf-8', 'utf-8', 'utf-8']

    df = pd.DataFrame()
    for i,j in zip(season_latin, encoding_latin):
        data = pd.read_csv(import_merged_gw(season=i), encoding=j)
        data['season'] = i
        df = pd.concat([df,data], ignore_index=True, sort=False)

    
    df = df[['season','name', 'web_name', 'position', 'team', 'assists','bonus','bps','clean_sheets',
            'creativity','element','fixture','goals_conceded','goals_scored','ict_index',
            'influence','kickoff_time','minutes','opponent_team','own_goals','penalties_missed',
            'penalties_saved','red_cards','round','saves','selected','team_a_score','team_h_score',
            'threat','total_points','transfers_balance','transfers_in','transfers_out','value','was_home',
            'yellow_cards','GW', 'chance_of_playing_this_round']]

    df = clean_players_name_string(df, col='name')
    df = filter_players_exist_latest(df, col='position')
    df = get_opponent_team_name(df)


    df = df[['season_x', 'name', 'web_name', 'position', 'team_x', 'assists', 'bonus', 'bps',
       'clean_sheets', 'creativity', 'element', 'fixture', 'goals_conceded',
       'goals_scored', 'ict_index', 'influence', 'kickoff_time', 'minutes',
       'opponent_team', 'opp_team_name', 'own_goals', 'penalties_missed', 'penalties_saved',
       'red_cards', 'round', 'saves', 'selected', 'team_a_score',
       'team_h_score', 'threat', 'total_points', 'transfers_balance',
       'transfers_in', 'transfers_out', 'value', 'was_home', 'yellow_cards',
       'GW', 'chance_of_playing_this_round']]

    df = df.rename(columns={'season_x': 'season', 'team_x': 'team'})
    
    export_cleaned_data(df)

def understat_fixtures_merge():
    """ 
    Merge all the understat fixtures data and export to a new file
    """
    season_latin = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22'] 

    df = pd.DataFrame()
    for i in season_latin:
        data = pd.read_csv(f'data/{i}/understat/understat_fixtures.csv')
        df = pd.concat([df,data])
    df.to_csv('data/understat_fixtures_merged.csv', index=False)
    return df

def understat_players_cleaned():
    """
    Merge all the understat players merged data
    """

    season_latin = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22'] 
    df = pd.DataFrame()
    for i in season_latin:
        data = pd.read_csv(f'data/{i}/understat/understat_players_merged.csv')
        df = pd.concat([df,data], ignore_index=True, sort=False)
    df = df.drop_duplicates()
    df.to_csv('data/understat_cleaned_merged_players.csv', index=False)

def understat_stats_merge():
    fixtures = pd.read_csv('data/understat_fixtures_merged.csv')
    players = pd.read_csv('data/understat_cleaned_merged_players.csv')
    merge_df = players.merge(fixtures, how = "left", on = ['id'])
    merge_df.to_csv('data/understat_cleaned_merged_seasons.csv', index = False)
    return merge_df

def id_dict_merge():
    season_latin = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22', '2022-23']
    encoding_latin = ['latin-1', 'latin-1', 'latin-1', 'utf-8', 'utf-8', 'utf-8', 'utf-8'] 
    df = pd.DataFrame()
    for i,j in zip(season_latin, encoding_latin):
        data = pd.read_csv(f'data/{i}/id_dict.csv', encoding = j)
        df = pd.concat([df,data], ignore_index=True, sort=False)
    df = df.drop_duplicates()
    df.to_csv('data/id_dict.csv', index=False, encoding='utf-8')

def fpl_other_data_merge():  
    """
    Merge understat and fpl data
    """

    df_gws = pd.read_csv('data/cleaned_merged_seasons.csv')
    df_gws['date'] = pd.to_datetime(df_gws['kickoff_time']).dt.date
    df_ids = pd.read_csv('data/id_dict.csv')
    df_teams = pd.read_csv('data/master_team_list.csv', usecols=['team_name','spi_name','transfermarkt_name'])
    
    df_understat = pd.read_csv('data/understat_cleaned_merged_seasons.csv')
    df_understat['date'] = pd.to_datetime(df_understat['date']).dt.date
    df_understat= df_understat.drop_duplicates(subset=['date', 'Understat_ID'])
    
    df_fbref = pd.read_csv('data/fbref_merged.csv')
    df_fbref['date'] = pd.to_datetime(df_fbref['date'], infer_datetime_format=True).dt.date
    
    df_transfermarkt = pd.read_csv('data/transfermarkt_merged.csv')

    df_spi = pd.read_csv('data/spi_data.csv')
    df_spi['date'] = pd.to_datetime(df_spi['date'], infer_datetime_format=True).dt.date

    df = df_gws.merge(
        df_ids, 
        how = "left", 
        left_on = ['season', 'element'], 
        right_on = ['season', 'FPL_ID'])
    
    df = df.merge(
        df_understat, 
        how = "left", 
        on =['date','Understat_ID'], 
        suffixes=('','_ustat'))

    df = df.merge(
        df_fbref, 
        how = 'left', 
        left_on = ['date','fbref_name'], 
        right_on=['date', 'player'], 
        suffixes=('','_fbref'))
    
    df = df.merge(
        df_teams, 
        how = 'left', 
        left_on = 'team', 
        right_on= 'team_name')
    
    df = df.merge(
        df_transfermarkt, 
        how = 'left', 
        left_on=['season','transfermarkt_name'], 
        right_on = ['season','name'],
        suffixes=('','_tm'))
    
    df = df.merge(
        df_spi, 
        how = 'left', 
        left_on = ['date','spi_name'], 
        right_on = ['date','team'],
        suffixes=('','_spi'))

    df = df.drop_duplicates(subset=['Understat_ID','date'])

    df.to_csv('data/all_merged.csv', index = False)

def main():
    merge_position_data()
    merge_play_prob_data()
    merge_data()
    understat_players_cleaned()
    understat_fixtures_merge()
    understat_stats_merge()
    # #id_dict_merge() DO NOT RUN
    fpl_other_data_merge()


if __name__ == "__main__":
    main()