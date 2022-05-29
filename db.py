""" db.py: This is where psycopg2 functions are defined. Most functions are
    used to create the tables that make up the database and insert data.
"""
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


def create_role_type():
    """ Creates the person_role type. People can be writers, directors, or stars.
    """
    conn = connect()
    cur = conn.cursor()
    create_stmt = "DROP TYPE IF EXISTS person_role; CREATE TYPE person_role AS ENUM ('writer', 'director', 'star');" 
    cur.execute(create_stmt)
    conn.commit()
    conn.close()


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


def create_film_persons_table():
    """ Creates a table for the ways people are associcated with films
    """
    conn = connect()
    cur = conn.cursor()
    create_stmt = "CREATE TABLE film_persons(" \
                  "person_id INTEGER," \
                  "film_id INTEGER," \
                  "role_status person_role," \
                  "FOREIGN KEY (film_id) REFERENCES film (id)," \
                  "FOREIGN KEY (person_id) REFERENCES persons (id)," \
                  "PRIMARY KEY (person_id, film_id, role_status));" 
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


def create_film_company_table():
    """ Creates a table for film company relation in the database
    """
    conn = connect()
    cur = conn.cursor()
    create_stmt = "CREATE TABLE film_company(" \
                  "film_id INTEGER NOT NULL," \
                  "company_id INTEGER NOT NULL," \
                  "PRIMARY KEY (film_id)," \
                  "FOREIGN KEY (film_id) REFERENCES film (id)," \
                  "FOREIGN KEY (company_id) REFERENCES company (id));"
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
                  "budget float(1)," \
                  "gross float(1)," \
                  "votes INT," \
                  "rating INTEGER," \
                  "FOREIGN KEY (rating) REFERENCES rating (id));"
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


def create_film_genre_table():
    """ Creates a table for film-genre relation in the database
    """
    conn = connect()
    cur = conn.cursor()
    create_stmt = "CREATE TABLE film_genre(" \
                  "film_id INTEGER NOT NULL," \
                  "genre_id INTEGER NOT NULL," \
                  "PRIMARY KEY (film_id, genre_id)," \
                  "FOREIGN KEY (film_id) REFERENCES film (id)," \
                  "FOREIGN KEY (genre_id) REFERENCES genre (id));"
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


def insert_film(name, score, date, budget, gross, votes, rating, cur):
    if (rating != None):
        cur.execute("SELECT id FROM rating WHERE rating_type = %s", (rating,))
        rating_id = cur.fetchone()[0]
        cur.execute("INSERT INTO film (title, score, release, budget, gross, votes, rating) VALUES (%s, %s, %s, %s, %s, %s, %s)", 
                    (name, score, date, budget, gross, votes, rating_id))
    else:
        cur.execute("INSERT INTO film (title, score, release, budget, gross, votes, rating) VALUES (%s, %s, %s, %s, %s, %s, %s)", 
                    (name, score, date, budget, gross, votes, None))


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
      if(i != ""):
        cur.execute("INSERT INTO company (company) VALUES (%s)", i)
    conn.commit()
    conn.close()

def insert_film_persons(director, writer, star):
    """ Takes a Python list of directors, writers, and stars and searches for their matching
    films, then inserts them into the relational table
    """
    insert_director(director)
    insert_writer(writer)
    insert_star(star)

def insert_director(director):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM film")
    film_id = cur.fetchall()
    spot = 0
    for i in director:
        if(i != i):
            spot = spot + 1
        else:
            if type(i) == str and " " in i:
                first_name = i.split()[0]
                last_name = i.split()[1]
                cur.execute("SELECT id FROM persons WHERE first_name = %s AND last_name = %s",(first_name, last_name))
            else:
                name = i
                cur.execute("SELECT id FROM persons WHERE first_name = %s",(name))
            director_id = cur.fetchone()[0]
            cur.execute("INSERT INTO film_persons (film_id, person_id, role_status) VALUES (%s, %s, %s)", (film_id[spot], director_id, "director",))
            spot = spot + 1
    conn.commit()
    conn.close()

def insert_writer(writer):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM film")
    film_id = cur.fetchall()
    spot = 0
    for i in writer:
        if(i != i):
            spot = spot + 1
        else:
            if type(i) == str and " " in i:
                first_name = i.split()[0]
                last_name = i.split()[1]
                cur.execute("SELECT id FROM persons WHERE first_name = %s AND last_name = %s",(first_name, last_name))
            else:
                name = i
                cur.execute("SELECT id FROM persons WHERE first_name = %s",(name))
            writer_id = cur.fetchone()[0]
            cur.execute("INSERT INTO film_persons (film_id, person_id, role_status) VALUES (%s, %s, %s)", (film_id[spot], writer_id, "writer",))
            spot = spot + 1
    conn.commit()
    conn.close()

def insert_star(star):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM film")
    film_id = cur.fetchall()
    spot = 0
    for i in star:
        if(i != i):
            spot = spot + 1
        else:
            if type(i) == str and " " in i:
                first_name = i.split()[0]
                last_name = i.split()[1]
                cur.execute("SELECT id FROM persons WHERE first_name = %s AND last_name = %s",(first_name, last_name))
            else:
                name = i
                cur.execute("SELECT id FROM persons WHERE first_name = %s",(name))
            star_id = cur.fetchone()[0]
            cur.execute("INSERT INTO film_persons (film_id, person_id, role_status) VALUES (%s, %s, %s)", (film_id[spot], star_id, "star",))
            spot = spot + 1
    conn.commit()
    conn.close()

def insert_film_company(company):
    """ Takes a Python list of companies and searches for their ids
    and adds them to the film ids to create an insert.
    """
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM film")
    film_id = cur.fetchall()
    spot = 0
    for i in company:
      if(i != i):
        spot = spot + 1
      else:
        cur.execute("SELECT id FROM company WHERE company = %s", (i,))
        company_id = cur.fetchone()[0]
        cur.execute("INSERT INTO film_company (film_id, company_id) VALUES (%s, %s)", (film_id[spot], company_id,))
        spot = spot + 1
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


def insert_film_genre(genre):
    """ Takes a Python list of genre and seaches for their ids 
    and inserts them into the file_genre table.
    """
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM film")
    film_id = cur.fetchall()
    spot = 0
    for i in genre:
      cur.execute("SELECT id FROM genre WHERE genre_type = %s", (i,))
      genre_id = cur.fetchone()[0]
      cur.execute("INSERT INTO film_genre (film_id, genre_id) VALUES (%s, %s)", (film_id[spot],genre_id,))
      spot = spot + 1
    conn.commit()
    conn.close()


def do_query():
    conn = connect()
    cur = conn.cursor()
    con_query = True
    while con_query:
      my_query = input("Enter your query: ")
      try:
        cur.execute(my_query)       
        #result = cur.fetchall()
        result = cur.fetchmany(10)
        for row in result:
          print(row)
      except (Exception, psycopg2.Error) as error:
        print("Error fetching data from PostgreSQL table", error)
        conn.commit()
        conn.close()
        conn = connect()
        cur = conn.cursor()
      another_query = input("\nWould you like to do another query (y/n): ")
      if (another_query == "n"):
        con_query = False
        print("\nGood Bye")
    conn.commit()
    conn.close()

