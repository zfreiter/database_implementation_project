import datetime
import pandas as pd
import numpy as np
import psycopg2
from db import *
from db import *
from project import *
from helpers import convert_date_to_postgres

# Make sure all tables have been created first

"""
# Insert films
insert_ratings(list_of_ratings)
populate_films(films)
print ("films have been inserted.")


# Insert companies
list_company_na = company.dropna() # Drop all NaN(empty cells)
list_of_companies = list_company_na.drop_duplicates() # Drop all the duplicate rows in the dataframe
list_of_companies = list_of_companies.values.tolist() # Convert the list numpy array to a Python List
insert_companies(list_of_companies)
print ("companies have been inserted.")


# Insert Genres
list_of_genres = genre.drop_duplicates() # Drop all the duplicate rows in the dataframe
list_of_genres = list_of_genres.values.tolist() # Convert the list numpy array to a Python List
insert_genres(list_of_genres)
print ("Genres have been inserted.")


# Insert film Company relations
film_company = df2["company"].values.tolist() # Create a list of all the companies in the movies table
insert_film_company(film_company) # Insert all the film company relations
print ("film-company has been inserted.")


# Insert film Genre relations
film_genre = df2["genre"].values.tolist() # Create a list of all the genre's in the movies table
insert_film_genre(film_genre) # Insert all the film genre relations
print ("film-genre has been inserted.")

"""
# Insert Persons and their film relationships
insert_persons(all_people)
print ("people have been inserted")

# Insert the relationships between films and people
insert_film_persons(film_writers, "writer");
insert_film_persons(directors, "director")
insert_film_persons(stars, "star")
print ("relationships have been inserted")




