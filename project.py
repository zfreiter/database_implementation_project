import pandas as pd
import psycopg2
from db import insert_stars

# Get our csv as a dataframe and select the columns we want
df = pd.read_csv('movies.csv')
df = df[["name", "score", "star"]]

# Get the names of the stars in the CSV and add them to the database
stars = df.star.unique()
list_of_stars = list(stars) # Covert the list numpy array to a Python List
insert_stars(list_of_stars)
