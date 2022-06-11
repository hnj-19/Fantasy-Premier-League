from mergers import *

def merge_data():
    """ Merge all the data and export to a new file
    """
    season_latin = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22'] 
    encoding_latin = ['latin-1', 'latin-1', 'latin-1', 'utf-8', 'utf-8', 'utf-8']

    df = pd.DataFrame()
    for i,j in zip(season_latin, encoding_latin):
        data = pd.read_csv(import_merged_gw(season=i), encoding=j)
        data['season'] = i
        df = df.append(data, ignore_index=True, sort=False)

    df = df[['season','name', 'position', 'team', 'assists','bonus','bps','clean_sheets','creativity','element','fixture','goals_conceded','goals_scored','ict_index','influence','kickoff_time','minutes','opponent_team','own_goals','penalties_missed','penalties_saved','red_cards','round','saves','selected','team_a_score','team_h_score','threat','total_points','transfers_balance','transfers_in','transfers_out','value','was_home','yellow_cards','GW']]

    df = clean_players_name_string(df, col='name')
    df = filter_players_exist_latest(df, col='position')
    df = get_opponent_team_name(df)

    df = df[['season_x', 'name', 'position', 'team_x', 'assists', 'bonus', 'bps',
       'clean_sheets', 'creativity', 'element', 'fixture', 'goals_conceded',
       'goals_scored', 'ict_index', 'influence', 'kickoff_time', 'minutes',
       'opponent_team', 'opp_team_name', 'own_goals', 'penalties_missed', 'penalties_saved',
       'red_cards', 'round', 'saves', 'selected', 'team_a_score',
       'team_h_score', 'threat', 'total_points', 'transfers_balance',
       'transfers_in', 'transfers_out', 'value', 'was_home', 'yellow_cards',
       'GW']]
    
    export_cleaned_data(df)

def understat_fixtures_merge():
    """ 
    Merge all the understat fixtures data and export to a new file
    """
    season_latin = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22'] 

    df = pd.DataFrame()
    for i in season_latin:
        data = pd.read_csv(f'data/{i}/understat/understat_fixtures.csv')
        df = df.append(data, ignore_index=True, sort=False)
    df.to_csv('data/understat_fixtures_merged.csv', index=False)

def understat_players_cleaned():
    """
    Merge all the understat players merged data
    """

    season_latin = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22'] 
    df = pd.DataFrame()
    for i in season_latin:
        data = pd.read_csv(f'data/{i}/understat/understat_players_merged.csv')
        df = df.append(data, ignore_index=True, sort=False)
    df = df.drop_duplicates()
    df.to_csv('data/understat_cleaned_merged_players.csv', index=False)

def understat_stats_merge():
    fixtures = pd.read_csv('data/understat_fixtures_merged.csv')
    players = pd.read_csv('data/understat_cleaned_merged_players.csv')
    merge_df = players.merge(fixtures, how = "left", on = ['id'])
    merge_df.to_csv('data/understat_cleaned_merged_seasons.csv', index = False)
    return merge_df


def main():
    # understat_players_cleaned()
    # understat_fixtures_merge()
    understat_stats_merge()
    # merge_data()


if __name__ == "__main__":
    main()