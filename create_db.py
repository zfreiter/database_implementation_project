""" create_db.py: This module creates all of the tables in the 
    database by calling the functions that execute the PostgreSQL statements.
"""
import psycopg2
import os
from db import *

create_role_type()
create_persons_table()
create_company_table()
create_genre_table()
create_ratings_table()
create_film_table()
create_film_persons_table()
create_film_genre_table()
create_film_company_table()
