from .config import *

import pymysql

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

#TODO: Rename 

def load_file(conn,table,file):
    """
    Lode local data file into table
    :param conn: the Connection object
    :param table: the table to query
    :param file: the local file to load from
    """
    command = (f"LOAD DATA LOCAL INFILE '{file}' INTO TABLE {table} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES STARTING BY '' TERMINATED BY '\\n'")
    cur = conn.cursor()
    cur.execute(command)
    return cur.fetchall()
