from .config import *

import pymysql
import urllib.request
from os.path import exists
import ipywidgets as widgets

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def execute(conn, commands, output_commands=False):
    cur = conn.cursor()
    if type(commands)==str:
        commands=[commands]
    for command in commands:
        if output_commands:
            print('$'+command)
        cur.execute(command)
    cur.close()
    conn.commit()
    return cur.fetchall()

def create_connection_and_maybe_create_database_if_missing(user, password, host, database, port=3306,create_database_if_missing = None):
    """ Create a database connection to the MariaDB database
        specified by the host url and database name.
        If this fails because the database does not exist, create it (after confirmation)
    :param user: username
    :param password: password
    :param host: host url
    :param database: database
    :param port: port number
    :return: Connection object or None
    """
    try:
        conn = pymysql.connect(user=user,
                               passwd=password,
                               host=host,
                               port=port,
                               local_infile=1,
                               db=database)

    except pymysql.OperationalError as e:
        print(f"Error connecting to the MariaDB Server: {e}")
        ER_BAD_DB_ERROR = 1049 #FROM https://mariadb.com/kb/en/mariadb-error-codes/

        if e.args[0] == ER_BAD_DB_ERROR:
            def create_database():
                print("  creating database")
                conn = pymysql.connect(user=user,
                                        passwd=password,
                                        host=host,
                                        port=port,
                                        local_infile=1)
                create_database_ifne(conn)
                print("  created, connection established")
                return conn

            if create_database_if_missing == None:
                result = input(f"Do you want to create the database {database}? y/(n)")
                if result.strip().lower() in ["y","yes"]:
                    return create_database()
            elif create_database_if_missing:
                return create_database()

    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")

    else:
        return conn


def create_database_ifne(conn):
    return execute(conn,"CREATE DATABASE IF NOT EXISTS `property_prices` DEFAULT CHARACTER SET utf8 COLLATE utf8_bin", output_commands=True)

def create_pricepaid_table(conn):
    return execute(conn,["DROP TABLE IF EXISTS `pp_data`","""CREATE TABLE IF NOT EXISTS `pp_data` (
    `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
    `price` int(10) unsigned NOT NULL,
    `date_of_transfer` date NOT NULL,
    `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
    `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
    `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
    `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
    `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
    `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
    `street` tinytext COLLATE utf8_bin NOT NULL,
    `locality` tinytext COLLATE utf8_bin NOT NULL,
    `town_city` tinytext COLLATE utf8_bin NOT NULL,
    `district` tinytext COLLATE utf8_bin NOT NULL,
    `county` tinytext COLLATE utf8_bin NOT NULL,
    `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
    `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
    `db_id` bigint(20) unsigned NOT NULL
    ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1""",
    "ALTER TABLE `pp_data` ADD PRIMARY KEY (`db_id`), MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1"])

def load_pricepaid_data(conn, dest_dir, years=range(1995,2023), source_base="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"):
    for year in years:
        filename = f"pp-{year}.csv"
        source = f"{source_base}/{filename}"
        destination = f"{dest_dir}/{filename}"
        if not exists(destination):
            print(f"downloading {source} to {destination}")
            urllib.request.urlretrieve(source, destination)
        load_file(conn,"pp_data",destination,display=True,enclosed_by_double_quote=True)


def add_pricepaid_indicies(conn):
    return execute(conn,["CREATE INDEX `pp.postcode` USING HASH ON `pp_data` (postcode)",
    "CREATE INDEX `pp.date` USING HASH ON `pp_data` (date_of_transfer)"])

def create_postcode_table(conn):
    execute(conn,["DROP TABLE IF EXISTS `postcode_data`",
    """CREATE TABLE IF NOT EXISTS `postcode_data` (
    `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
    `status` enum('live','terminated') NOT NULL,
    `usertype` enum('small', 'large') NOT NULL,
    `easting` int unsigned,
    `northing` int unsigned,
    `positional_quality_indicator` int NOT NULL,
    `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
    `lattitude` decimal(11,8) NOT NULL,
    `longitude` decimal(10,8) NOT NULL,
    `postcode_no_space` tinytext COLLATE utf8_bin NOT NULL,
    `postcode_fixed_width_seven` varchar(7) COLLATE utf8_bin NOT NULL,
    `postcode_fixed_width_eight` varchar(8) COLLATE utf8_bin NOT NULL,
    `postcode_area` varchar(2) COLLATE utf8_bin NOT NULL,
    `postcode_district` varchar(4) COLLATE utf8_bin NOT NULL,
    `postcode_sector` varchar(6) COLLATE utf8_bin NOT NULL,
    `outcode` varchar(4) COLLATE utf8_bin NOT NULL,
    `incode` varchar(3)  COLLATE utf8_bin NOT NULL,
    `db_id` bigint(20) unsigned NOT NULL
    ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin""",
    "ALTER TABLE `postcode_data` ADD PRIMARY KEY (`db_id`), MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1",
    "CREATE INDEX `po.postcode` USING HASH ON `postcode_data` (postcode)"
    ])


def select_top(conn, table,  n):
    """
    Query n first rows of the table
    :param conn: the Connection object
    :param table: The table to query
    :param n: Number of rows to query
    """
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table} LIMIT {n}')
    rows = cur.fetchall()
    return rows

def head(conn, table, n=5):
  rows = select_top(conn, table, n)
  for r in rows:
      print(r)


#TODO: Rename
def load_file(conn,table,file,display=False,enclosed_by_double_quote=False):
    """
    Lode local data file into table
    :param conn: the Connection object
    :param table: the table to query
    :param file: the local file to load from
    """
    if display:
        print(f"Loading {file} into {table}")
    enclosed_specifier = "ENCLOSED BY '\"'" if enclosed_by_double_quote else ""
    command = (f"LOAD DATA LOCAL INFILE '{file}' INTO TABLE {table} FIELDS TERMINATED BY ',' {enclosed_specifier} LINES STARTING BY '' TERMINATED BY '\\n'")
    return execute(conn,command)

#======= Bounding box stuff (todo rename the word stuff) ======
selwyn_coords = (52.2011,0.1056)
mainland_bbox = (49.9591, 58.66667, -8.1775, 1.766667)

def bbox(centre,height,width=None):
    if width == None:
        width = height
    lat,long = centre
    return (lat-height/2,lat+height/2,long-width/2,long+width/2)

#======= Open street maps stuff ======
