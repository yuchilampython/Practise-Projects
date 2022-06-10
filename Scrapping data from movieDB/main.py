import requests
from requests_html import HTMLSession
import pandas as pd

year = input('Input year: ')
assert len(year) == 4

def scraping_movie_stat(year=year):
    session = HTMLSession()
    url = f'https://www.boxofficemojo.com/year/world/{year}/'
    r = session.get(url)

    r_table = r.html.find('.a-section.imdb-scroll-table-inner', first=True)
    rows = r_table.find('tr')
    # header name in list
    header = [h.text for h in rows[0].find('th')]
    detail = []

    # details in list
    for row in rows[1:]:
        row_item = row.find('td')
        row_item_list = []
        for each in row_item:
            row_item_list.append(each.text)
        detail.append(row_item_list)

    df = pd.DataFrame(detail, columns=header)

    return df

# put the movie stat to DataFrame
#WorldwideBoxOffice = scraping_movie_stat(year='2021')
#print(WorldwideBoxOffice.head())

# get IMDB_id with movie_name
def get_IMDB_id(movie_name=None):
    IMDB_id = None

    if movie_name != None:
        api_key = '9d07bb1'
        endpoint = f'http://www.omdbapi.com/?apikey={api_key}&t={movie_name}'

        r = requests.get(endpoint)
        #print(r.status_code)
        if r.status_code in range(200,299):
            data = r.json()
            if 'imdbID' in data:
                IMDB_id = data['imdbID']
                return IMDB_id

# get IMDB_id of Water Gate Bridge
#IMDB_id = get_IMDB_id(movie_name='Water Gate Bridge')
#print(IMDB_id)

# get movieDB_ID with IMDB_id
def get_movieDB_id(external_id=None):
    if external_id != None:
        api_key = 'e0ce409ef50f64dfe38f633488a495b9'
        api_base_url = f'https://api.themoviedb.org/3'
        endpoint_path = f'/find/{external_id}'
        external_source = 'imdb_id'
        endpoint = f'{api_base_url}{endpoint_path}?api_key={api_key}&external_source={external_source}'

        r = requests.get(endpoint)
        if r.status_code == 200:
            data = r.json()
            if data['movie_results']:
                result = data['movie_results'][0]
                if 'id' in result:
                    movie_DB_id = result['id']
                    return movie_DB_id

# get movieDB_id of Water Gate Bridge with IMDB_id
#movieDB_id = get_movieDB_id(IMDB_id)
#print(movieDB_id)

# get the a list of cast and character with movieDB_ID
def get_cast_details(movieDB_id=None):
    if movieDB_id != None:
        api_key = 'e0ce409ef50f64dfe38f633488a495b9'
        api_base_url = f'https://api.themoviedb.org/3'
        endpoint_path = f'/movie/{movieDB_id}/credits'
        endpoint = f'{api_base_url}{endpoint_path}?api_key={api_key}'

        cast_list = ''

        r = requests.get(endpoint)
        data = r.json()
        casts = data['cast']
        for cast in casts:
            cast_string = cast['name'] + ': ' + cast['character']
            cast_list = cast_list + cast_string + '\n'
        return cast_list

# get cast details of Water Gate Bridge with movieDB_id
#cast_details = get_cast_details(movieDB_id)
#print(cast_details)

# add IMDB_id and movieDB_id and cast to DataFrame (all rows)
def add_to_df(table=pd.DataFrame()):
    all_IMDB_id = []
    all_movieDB_id = []
    all_cast_details = []
    # iterate through the web scraping list
    for movie_name in table.loc[:, 'Release Group']:
        IMDB_id = get_IMDB_id(movie_name=movie_name)
        movieDB_id = get_movieDB_id(IMDB_id)
        cast_details = get_cast_details(movieDB_id)

        all_IMDB_id.append(IMDB_id)
        all_movieDB_id.append(movieDB_id)
        all_cast_details.append(cast_details)

    table['imdb_id'] = all_IMDB_id
    table['movieDB_id'] = all_movieDB_id
    table['cast name and character'] = all_cast_details


WorldwideBoxOffice = scraping_movie_stat(year=year)
add_all = add_to_df(table=WorldwideBoxOffice)
WorldwideBoxOffice.to_csv(f'movie {year}.csv')
print(WorldwideBoxOffice.head())

