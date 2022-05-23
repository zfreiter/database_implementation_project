import datetime
import pandas as pd
import numpy as np
import psycopg2

def convert_date_to_postgres(df_date):
    """ Takes in a date from the movies df and converts it to a postgresql
        date
    """
    date = df_date.split(' (')[0]   # now date is 'month day, year'
    date = date.split(" ")          # now date is a list with ['month', 'day,', 'year']

    if (len(date)) != 3:            # date must have month day and year
        date = None
    else:
        day = date[1].replace(',', '')
        date = date[2] + '-' + date[0] + '-' + day
    return date
