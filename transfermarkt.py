
import pandas as pd
import numpy as np
import requests
import lxml.html as lh

# function to scrape market value of premier league teams at the start of each season
# from www.transfermarkt.com
def get_transfermarkt_data(season = '2022-23', header_row=11, team_rows=range(13,33)):
    season_short = season[2:4] + season[5:7]
    # url for page with team market value at start of season
    url=r'https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1/plus/?saison_id=' + '20' + season_short[0:2]
    
    #Create a handle, page, to handle the contents of the website
    page = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    
    #Store the contents of the website under doc
    doc = lh.fromstring(page.content)
    
    #Parse data that are stored between <tr>..</tr> of HTML
    tr_elements = doc.xpath('//tr')
    
    #Create empty list
    col=[]
    i=0

    #For each row, store each first element (header) and an empty list
    for t in tr_elements[header_row]:
        i+=1
        name=t.text_content()
        col.append((name,[]))
        
    #data is stored on the second row onwards
    for j in team_rows:
        #T is our j'th row
        T=tr_elements[j]

        #If row is not of size 10, the //tr data is not from our table 
        if len(T)!=7:
            break

        #i is the index of our column
        i=0

        #Iterate through each element of the row
        for t in T.iterchildren():
            data=t.text_content() 
            #Check if row is empty
            if i>0:
            #Convert any numerical value to integers
                try:
                    data=int(data)
                except:
                    pass
            #Append the data to the empty list of the i'th column
            col[i][1].append(data)
            #Increment i for the next column
            i+=1
        
    # create market values dataframe
    Dict={title:column for (title,column) in col}
    df=pd.DataFrame(Dict)
        
    # convert market value string to float for millions of euros
    values = [float(item[0].replace(',', '.').replace('€', '').replace('bn', '').replace('m', '')) 
              for item in df['Total market value'].astype(str).str.split(" ", 1)]
    values = [item*10**3 if item < 3 else item for item in values]
    
    # to remove effect of inflation, take relative market value for each season
    values_normalised = values/np.mean(values)
    
    # market value website has shortened team names
    # lookup dictionary of full names
    team_names = {'Man City': 'Manchester City',
                  'Spurs': 'Tottenham Hotspur',
                  'Man Utd': 'Manchester United',
                  'Leicester': 'Leicester City',
                  'West Ham': 'West Ham United',
                  'Wolves': 'Wolverhampton Wanderers',
                  'Brighton': 'Brighton and Hove Albion',
                  'Newcastle': 'Newcastle United',
                  'Sheff Utd': 'Sheffield United',
                  'West Brom': 'West Bromwich Albion',
                  'Swansea': 'Swansea City',
                  'Huddersfield': 'Huddersfield Town',
                  'Cardiff': 'Cardiff City'}
    
    # create smaller dataframe with team names, market value and the season
    df = df[['name']]
    df.replace(team_names, inplace=True)
    df['value'] = values
    df['relative_market_value'] = values_normalised
    df['season'] = season
    
    df.to_csv(f'data/{season}/transfermarkt/transfermarkt_data.csv', index=False)
    return df

def transfermarkt_merge():
    seasons = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22']
    df = pd.DataFrame()
    for season in seasons:
        _df = pd.read_csv(f'data/{season}/transfermarkt/transfermarkt_data.csv')
        df = pd.concat([df,_df], ignore_index=False,sort=False)
    df.name = df.name.replace('[^a-zA-Z0-9 ]', '', regex=True).str.strip()
    df.to_csv('data/transfermarkt_merged.csv', index=False, encoding = 'utf-8')

def main():
    # get_transfermarkt_data()
    transfermarkt_merge()

if __name__ == "__main__":
    main()