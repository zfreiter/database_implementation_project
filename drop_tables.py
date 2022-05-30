import psycopg2
import os
from db import *

def drop_tables():
    drop_table("company")
    drop_table("film")
    drop_table("genre")
    drop_table("persons")
    drop_table("film_company")
    drop_table("film_genre")
    drop_table("film_persons")
    drop_table("rating")

def drop_table(table):
    conn = connect()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS %s CASCADE;",(table,))
    conn.commit()
    conn.close()
