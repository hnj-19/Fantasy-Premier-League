import pandas as pd


def get_spi_data():
    df = pd.read_csv('https://projects.fivethirtyeight.com/soccer-api/club/spi_matches.csv')
    teams = pd.read_csv('data/master_team_list.csv')['spi_name'].unique()
    _df1 = df[df['team1'].isin(teams)]
    _df2 = df[df['team2'].isin(teams)]
    _df = pd.DataFrame()
    df = pd.concat([_df,_df1,_df2], ignore_index=True).reset_index(drop=True)
    df.to_csv('data/spi_data.csv', index = False)
    return df

def main():
    get_spi_data()

if __name__ == "__main__":
    main()