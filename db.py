import psycopg2
import os


def connect():
    """ Connects to the server using psycopg2
        and returns a connection object when called.
    """
    try:
        connection = psycopg2.connect(
            host=os.environ['CLASS_DB_HOST'],
            database=os.environ['CLASS_DB_USERNAME'],
            user=os.environ['CLASS_DB_USERNAME'],
            password=os.environ['CLASS_DB_PASSWORD']
        )
        return connection
    except:
        print ("Failed to connect to the database.")


def create_persons_table():
    """ Creates a table for persons in the database 
    """
    conn = connect()
    cur = conn.cursor()
    create_stmt = "CREATE TABLE persons(" \
                  "id SERIAL PRIMARY KEY," \
                  "first_name   varchar(32)," \
                  "last_name varchar(32));"
    cur.execute(create_stmt)
    conn.commit()
    conn.close()

def create_company_table():
    """ Creates a table for company in the database 
    """
    conn = connect()
    cur = conn.cursor()
    create_stmt = "CREATE TABLE company(" \
                  "id SERIAL PRIMARY KEY," \
                  "company varchar(64));" 
    cur.execute(create_stmt)
    conn.commit()
    conn.close()

def create_film_table():
    """ Creates a table for film in the database
    """
    conn = connect()
    cur = conn.cursor()
    create_stmt = "CREATE TABLE film(" \
                  "id SERIAL PRIMARY KEY," \
                  "title varchar(128)," \
                  "score DECIMAL(2, 1)," \
                  "release DATE," \
                  "budget INT," \
                  "gross INT," \
                  "votes INT," \
                  "rating varchar(15));"
    cur.execute(create_stmt)
    conn.commit()
    conn.close()

def create_genre_table():
    """ Creates a table for genre in the database 
    """
    conn = connect()
    cur = conn.cursor()
    create_stmt = "CREATE TABLE genre(" \
                  "id SERIAL PRIMARY KEY," \
                  "genre_type varchar(32));"
    cur.execute(create_stmt)
    conn.commit()
    conn.close()


def create_ratings_table():
    """ Creates a table for ratings in the database 
    """
    conn = connect()
    cur = conn.cursor()
    create_stmt = "CREATE TABLE rating(" \
                  "id SERIAL PRIMARY KEY," \
                  "rating_type varchar(10));"
    cur.execute(create_stmt)
    conn.commit()
    conn.close()


def insert_ratings(ratings_array):
    conn = connect()
    cur = conn.cursor()
    for i in ratings_array:
        cur.execute("INSERT INTO rating (rating_type) VALUES (%s)", (i,))
    conn.commit()
    conn.close()


def insert_film(name, score, date, budget, gross, votes, rating):
    print (name, score, date, budget, gross, votes, rating)


def insert_stars(stars):
    """ Takes a Python list of stars and adds them to the database. Checks if
        name is singular or has both a first and last name.
    """
    conn = connect()
    cur = conn.cursor()
    for i in stars:
        if type(i) == str and " " in i:
            first_name = i.split()[0]
            last_name = i.split()[1]
            cur.execute("INSERT INTO persons (first_name, last_name) VALUES (%s, %s)", (first_name,last_name))
        elif (type(i) == str and " " not in i):
            name = i
            cur.execute("INSERT INTO persons (first_name) VALUES (%s)", (name,))
        else:
            pass
    conn.commit()
    conn.close()


def insert_companies(company):
    """ Takes a Python list of companies and adds them to the database.
    """
    conn = connect()
    cur = conn.cursor()
    for i in company:
      cur.execute("INSERT INTO company (company) VALUES (%s)", i)
    conn.commit()
    conn.close()

def insert_genres(genre):
    """ Takes a Python list of genres and adds them to the genre databese
    """
    conn = connect()
    cur = conn.cursor()
    for i in genre:
      cur.execute("INSERT INTO genre (genre_type) VALUES (%s)", i)
    conn.commit()
    conn.close()

# Uncomment to Create Persons table
# create_persons_table()
# create_company_table()
# create_genre_table()
# create_film_table()
# create_ratings_table()
