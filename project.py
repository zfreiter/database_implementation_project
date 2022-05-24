import datetime
import pandas as pd
import numpy as np
import psycopg2
from db import insert_stars, insert_companies, insert_genres, insert_ratings, insert_film_genre
from db import insert_film, connect, insert_film_company
from helpers import convert_date_to_postgres

# Get our csv as a dataframe and select the columns we want
df = pd.read_csv('movies.csv')
films = df[["name","score","released","budget", "gross", "votes", "rating"]]

# Get the distinct ratings in the CSV and add them to the ratings table
ratings_series = df[["rating"]]
ratings = ratings_series.rating.unique()
list_of_ratings = list(ratings) # Covert the numpy array to a Python List


def populate_films(films_df):
    """ Takes a dataframe of films and stores each row in the database by 
        passing each row into the insert_film() function. Pandas gives an empty
        column in a row the nan (float type). If an empty value is present,
        None keyword should be used to represent null in postgres.
        The film, and ratings table must be created prior to running this 
        function and the ratings table must be populated.
    """
    conn = connect()
    cur = conn.cursor()
    for row in films_df.itertuples():
        score = None
        date = None
        budget = None
        gross = None
        votes = None
        rating = None

        # There is a rating
        if (type(row[7]) != float):
            rating = row[7]

        # there is a score
        if (np.isnan(row[2]) == False):

            score = row[2]

        # there is a release date.
        if type(row[3]) != float:
            date = convert_date_to_postgres(row[3])

        # there is a budget
        if (np.isnan(row[4]) == False):
            budget = row[4]

        # there is a gross
        if (np.isnan(row[5]) == False):
            gross = row[5]


        # There are votes
        if (np.isnan(row[6]) == False):
            votes = row[6]

        # insert the row
        insert_film(row[1], score, date, budget, gross, votes, rating, cur)

    conn.commit()
    conn.close()


# Uncomment these to insert the ratings and films. Make sure all tables have
# been created first
# insert_ratings(list_of_ratings)
# populate_films(films)


# df = df[["name", "score", "star"]]
# Get the names of the stars in the CSV and add them to the database
#stars = df.star.unique()
#list_of_stars = list(stars) # Covert the list numpy array to a Python List
#insert_stars(list_of_stars)

df2 = pd.read_csv('movies.csv')

company = df2[["company"]]
genre = df2[["genre"]]

# list_remove_na = company.dropna() # Drop all NaN(empty cells)
# list_of_companies = list_company_na.drop_duplicates() # Drop all the duplicate rows in the dataframe
# list_of_companies = list_of_companies.values.tolist() # Convert the list numpy array to a Python List
# insert_companies(list_of_companies)

# list_of_genres = genre.drop_duplicates() # Drop all the duplicate rows in the dataframe
# list_of_genres = list_of_genres.values.tolist() # Convert the list numpy array to a Python List
# insert_genres(list_of_genres)

# film_company = df2["company"].values.tolist() # Create a list of all the companies in the movies table
# insert_film_company(film_company) # Insert all the film company relations

# film_genre = df2["genre"].values.tolist() # Create a list of all the genre's in the movies table
# insert_film_genre(film_genre) # Insert all the film genre relations
