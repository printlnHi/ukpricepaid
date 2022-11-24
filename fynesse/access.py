from .config import *

import pymysql
import urllib.request
from os.path import exists

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def execute(conn, commands):
    cur = conn.cursor()
    if type(commands)==str:
        commands=[commands]
    for command in commands:
        cur.execute(command)
    cur.close()
    conn.commit()
    return cur.fetchall()

def create_connection(user, password, host, database, port=3306):
    """ Create a database connection to the MariaDB database
        specified by the host url and database name.
    :param user: username
    :param password: password
    :param host: host url
    :param database: database
    :param port: port number
    :return: Connection object or None
    """
    conn = None
    try:
        conn = pymysql.connect(user=user,
                               passwd=password,
                               host=host,
                               port=port,
                               local_infile=1,
                               db=database
                               )
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn


def create_database_ifne(conn):
    return execute(conn,"CREATE DATABASE IF NOT EXISTS `property_prices` DEFAULT CHARACTER SET utf8 COLLATE utf8_bin")

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

def load_pricepaid_data(conn, dest_dir, source_base="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"):
    for year in range(1995,2023):
        filename = f"pp-{year}.csv"
        source = f"{source_base}/{filename}"
        destination = f"{dest_dir}/{filename}"
        if not exists(destination):
            print(f"downloading {source} to {destination}")
            urllib.request.urlretrieve(source, destination)
        load_file(conn,"pp_data",destination)


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
def load_file(conn,table,file,enclosed_by_double_quote=False):
    """
    Lode local data file into table
    :param conn: the Connection object
    :param table: the table to query
    :param file: the local file to load from
    """
    enclosed_specifier = "ENCLOSED BY '\"'" if enclosed_by_double_quote else ""
    command = (f"LOAD DATA LOCAL INFILE '{file}' INTO TABLE {table} FIELDS TERMINATED BY ',' {enclosed_specifier} LINES STARTING BY '' TERMINATED BY '\\n'")
    return execute(conn,command)
