import pandas as pd
import psycopg2
from db import insert_stars, insert_companies, insert_genres

# Get our csv as a dataframe and select the columns we want
df = pd.read_csv('movies.csv')
df = df[["name", "score", "star"]]

# Get the names of the stars in the CSV and add them to the database
#stars = df.star.unique()
#list_of_stars = list(stars) # Covert the list numpy array to a Python List
#insert_stars(list_of_stars)

df2 = pd.read_csv('movies.csv')

company = df2[["company"]]
genre = df2[["genre"]]

list_of_companies = company.drop_duplicates() # Drop all the duplicate rows in the dataframe
list_of_companies = list_of_companies.values.tolist() # Convert the list numpy array to a Python List

list_of_genres = genre.drop_duplicates() # Drop all the duplicate rows in the dataframe
list_of_genres = list_of_genres.values.tolist() # Convert the list numpy array to a Python List

insert_companies(list_of_companies)
insert_genres(list_of_genres)
