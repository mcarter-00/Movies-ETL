#Import dependencies
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2
from config import db_password
import time

#Set file directory
file_dir = '/Users/mariacarter/Desktop/Berkeley-Bootcamp/Analysis-Projects/Movies-ETL/Resources/'

def process_ETL(wiki_movies, kaggle_metadata, ratings):
    
    with open (f'{file_dir}/'+wiki_movies, mode='r') as file:
        wiki_movies_raw = json.load(file)
    
    kaggle_metadata = pd.read_csv(f'{file_dir}/'+kaggle_metadata, low_memory=False)   
    ratings = pd.read_csv(f'{file_dir}/'+ratings)

    #Use a list comprehension to filter data
    wiki_movies = [movie for movie in wiki_movies_raw 
                   if ('Director' in movie or 'Directed by' in movie) 
                       and 'imdb_link' in movie
                       and 'No. of episodes' not in movie]

    
    #Loop through every key, add the alt_titles dict to the movie object
    def clean_movie(movie):
        movie = dict(movie) # create a non-destructive copy
        alt_titles = {}
    
        #Combine alternate titles into one list
        for key in ['Also known as', 'Arabic', 'Cantonese', 'Chinese', 'French',
                   'Hangul', 'Hebrew', 'Hepburn', 'Japanese', 'Literally',
                   'Mandarin', 'McCune–Reischauer', 'Original title', 'Polish',
                   'Revised Romanization', 'Romanized', 'Russian',
                   'Simplified', 'Traditional', 'Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
    
    #Merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)        
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies', 'Production company(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release date')
        change_column_name('Released Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')
    
        return movie

    #Use a list comprehension to make a list of clean movies
    clean_movies = [clean_movie(movie) for movie in wiki_movies]

    #Create a Wiki Movies DF from the clean movies dataset
    wiki_movies_df = pd.DataFrame(clean_movies)

    #Extract IMDb ID
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')

    #Drop duplicate IMDb IDs
    wiki_movies_df.drop_duplicates(subset= 'imdb_id', inplace=True)

    #Use a list comprehension to remove mostly null columns from the Wiki Movies DF
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns 
                            if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]

    #Create a revised Wiki Movies DF from the updated data
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    #Drop 'Box Office' from dataset, converting lists to strings
    box_office = wiki_movies_df['Box office'].dropna().apply(lambda x: ''.join(x) if type(x) == list else x)

    #Create forms in the 'Box Office' data and use regular expressions to parse the data
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'

    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    #Extract & convert the 'Box Office' values
    box_office.str.extract(f'({form_one}|{form_two})')

    def parse_dollars(s):
    
        #If s is not a string, return NaN
        if type(s) != str:
            return np.nan
    
        #If input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
        
            #Remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]', '', s)
        
            #Convert to float and multiply by a million
            value = float(s) * 10**6
        
            #Return value
            return value
    
        #If input is of the form $###.# billion
        elif re.match('\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
        
            #Remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]', '', s)
        
            #Convert to float and multiply by a billion
            value = float(s) * 10**9
        
            #Return value
            return value
    
        #If input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
    
            #Remove dollar sign and commas
            s = re.sub('\$|,','', s)
        
            #Convert to float
            value = float(s)
        
            #Return value
            return value
    
        #Otherwise, return NaN
        else:
            return np.nan

    #Extract the values from 'Box Office' using str.extract & apply parse_dollars to the 1st column
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    #Drop the 'Box Office' column
    wiki_movies_df.drop('Box office', axis=1, inplace=True)

    #Drop 'Budget' from dataset, converting lists to strings:
    budget = wiki_movies_df['Budget'].dropna().apply(lambda x: ''.join(x) if type(x) == list else x)

    #Remove any values betwen a dollar sign & a hyphen in 'Budget'
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    #Remove any values betwen a dollar sign & a hyphen in 'Budget'
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    #Use same pattern matches to parse 'Budget'
    matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
    budget[~matches_form_one & ~matches_form_two]

    #Remove citation references
    budget = budget.str.replace(r'\[\d+\]s*','')
    budget[~matches_form_one & ~matches_form_two]

    #Parse the 'Budget' values
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    #Drop the 'Budget' column 
    wiki_movies_df.drop('Budget', axis=1, inplace=True)

    #Drop 'Release date' from dataset, converting lists to strings:
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ''.join(x) if type(x)== list else x)

    #Parse the forms
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'

    #Extract the dates
    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)

    #Use built-in to_datetime() to parse the dates, and set the infer_datetime_format option to 'True' because there are different date formats.
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

    #Drop 'Running time' from dataset, converting lists to strings:
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    #Extract digits, and allow for both possible patterns by adding capture groups around the \d instances and add an alternating character
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

    #Convert from string to numeric
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

    #Apply a function that converts the 'hour' and 'minute' capture groups to 'minutes' if the pure minutes capture group is zero, and save the output to wiki_movies_df
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

    #Drop 'running time'
    wiki_movies_df.drop('Running time', axis=1, inplace=True)

    #Remove bad data from Kaggle Metadata DF
    kaggle_metadata[~kaggle_metadata['adult'].isin(['True','False'])]

    #Keep rows where adult=False, then drop the adult column
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult', axis='columns')

    #Convert data to since 'video' are T/F values
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'

    #For numeric columns, use to_numeric() method. 
    #Make sure errors= argument is set to 'raise' so that we know if theres data that can't be converted to numbers
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')

    #Convert 'release_date' to datetime using to_datetime()
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

    #Since there's so many rows that are null in the Ratings DF, set the null_counts = True
    ratings.info(null_counts=True)

    #Specify in to_datetime() that the origin is 'unix' and the time unit is seconds, and assign it to the 'timestamp; column
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
 
    #Merge Wikipedia & Kaggle Metadata
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki', '_kaggle'])
 
    #Drop the wild outlier (aka 'The Holiday') from Wikipedia data
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)

    #Convert the 'Languge' list to a tuple so that .value_counts() can work
    movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)
    movies_df['original_language'].value_counts(dropna=False)
    
    #Drop the title_wiki, release_date_wiki, Language, and Production company(s) columns
    movies_df.drop(columns=['title_wiki', 'release_date_wiki', 'Language', 'Production company(s)'], inplace=True)
    
    #Make a function that fills in missing data for a column pair and then drops the redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)
        
    #Run the function for the three column pairs that were decided to be filled with zeros
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    
    #Check that there aren’t any columns with only one value, and convert lists to tuples for value_counts() to work.
    for col in movies_df.columns:
        lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
        value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
        num_values = len(value_counts)
        if num_values == 1:
            print(col)
    
    movies_df['video'].value_counts(dropna=False)

    #Reorder the columns
    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                          ]]

    #Rename the columns
    movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                     }, axis='columns', inplace=True)

    #Count how many times a movie received a given rating 
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')

    #Rename the columns... prepend rating_ to each column with a list comprehension:
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    #Connect Pandas to SQL
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    
    engine = create_engine(db_string)
    
    #Import the movie data 
    movies_df.to_sql(name='movies', con=engine)

    #Create a variable for the number of rows imported
    rows_imported = 0

    #Get the start_time from time.time()
    start_time = time.time()

    for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
    
        #Print out the range of rows that are being imported
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
    
        data.to_sql(name='ratings', con=engine, index=False, if_exists='replace')
    
        #Increment the number of rows imported by the size of 'data'
        rows_imported += len(data)
    
        #Add elapsed time to final printout
        print(f'Done. {time.time() - start_time} total seconds elapsed') 

process_ETL("wikipedia.movies.json", "movies_metadata.csv", "ratings.csv")        