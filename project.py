""" project.py: This is where Pandas functions go. All psycopg2 functions 
    should be written in the db module and imported here when needed.
"""
import datetime
import pandas as pd
import numpy as np
import psycopg2
from db import insert_companies, insert_genres, insert_ratings, insert_film_genre
from db import insert_film, connect, insert_film_company, do_query
from helpers import convert_date_to_postgres

# Get our csv as a dataframe and select the columns we want
df = pd.read_csv('movies.csv')
films = df[["name","score","released","budget", "gross", "votes", "rating", "runtime"]]


# Get the distinct ratings in the CSV and add them to the ratings table
ratings_series = df[["rating"]]
ratings = ratings_series.rating.unique()
list_of_ratings = list(ratings) # Covert the numpy array to a Python List


# Get the writers, directors, and stars into one list
all_people = df[["writer", "director", "star"]]
all_people = all_people.stack()
all_people = pd.unique(all_people)
all_people = list(set(all_people))


# Get films, dates and people in a list by writer, direcotr, star
film_writers = df[["name", "released", "writer"]]
stars = df[["name", "released", "star"]]
directors = df[["name", "released", "director"]]

film_writers = film_writers.values.tolist()
directors = directors.values.tolist()
stars = stars.values.tolist()


df2 = pd.read_csv('movies.csv')
company = df2[["company"]]
genre = df2[["genre"]]


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
        runtime = None

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

        # There is a runtime
        if (np.isnan(row[8]) == False):
            runtime = row[8]

        # insert the row
        insert_film(row[1], score, date, budget, gross, votes, rating, runtime, cur)

    conn.commit()
    conn.close()

"""
# Populate film_persons table
film_writers = df[["name", "released", "writer", "director", "star"]] 
all_films = all_films.drop_duplicates()
print (all_films.head())
"""
