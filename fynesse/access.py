from .config import *

import pymysql
import urllib.request
from os.path import exists
import geopandas as gpd
import osmnx as ox

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

def inner_join(conn, bbox = None, date_bound = None, limit=None, sample_every = None, output_commands=False):
  conditions = []
  if sample_every != None:
    conditions.append("pp_data.db_id mod sample_every = 0")
  if bbox != None:
    conditions.append(f"lattitude between {bbox[0]} AND {bbox[1]} AND longitude between {bbox[2]} and {bbox[3]}")
  if date_bound != None:
    from_date,to_date = date_bound
    conditions.append(f"DATE(date_of_transfer) between '{from_date}' and '{to_date}'")
  conditions = " AND ".join(conditions)

  query = f"""
SELECT price, date_of_transfer, `pp_data`.postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county, country, lattitude, longitude
FROM
    `pp_data`
INNER JOIN 
    `postcode_data`
ON
    `pp_data`.postcode = `postcode_data`.postcode
{"WHERE "+conditions if len(conditions)>0 else ""}
{f"LIMIT {limit}" if limit != None else ""}
"""
  results=execute(conn, query, output_commands=output_commands)
  gdf = gpd.GeoDataFrame(results, columns=["price", "date_of_transfer", "postcode", "property_type", "new_build_flag", "tenure_type", "locality", "town_city", "district", "county", "country", "latitude", "longitude"])
  gdf.geometry = gpd.points_from_xy(gdf.longitude, gdf.latitude, crs= "EPSG:4326")
  return gdf

#===== Bounding boxes and example coordinates =====
"""
The sane bounding box (bbox) format is
  (minlat,maxlat,minlong,maxlong)
however ox's bbox functions use
  (maxlat,minlat,maxlong,minlong)
All sample bboxs and return types are of the sane format, unless otherwise specified
"""
example_coords = {"selwyn":(52.2011,0.1056), "leman_locke_aldagte": (51.5145,-0.0708), "london":(51.5072, 0.1276)}

mainland_bbox = (49.9591, 58.66667, -8.1775, 1.766667)

def bbox(centre,width,height):
    lat,long = centre
    return (lat-height/2,lat+height/2,long-width/2,long+width/2)

"""
bbox comparison to use for documentation
bbox(access.selwyn_coords,0.1,0.1) -> (52.1511, 52.251099999999994, 0.0556, 0.15560000000000002)
km_bbox(access.selwyn_coords,11.119,11.119) -> (52.15110228594827, 52.251097714051724, 0.02402262622942146, 0.18717737377057853)
"""
def km_bbox(centre,kmwidth,kmheight):
    """Creates a geographic bounding box s.t. the east-west line through the centre is the required width and the north-south line through the centre is the required height
    :param centre: the geographic lat,long centre
    :param kmwidth: the width of the E-W line in kilometers
    :param kmheight: the height of the N-S line in kilometers
    :return a bbox
    """
    lat,long = centre
    kms_in_1_degree_of_lat = ox.distance.great_circle_vec(0,long,1,long) / 1000
    kms_in_1_degree_of_long = ox.distance.great_circle_vec(lat,0,lat,1) / 1000
    height = kmheight / kms_in_1_degree_of_lat
    width = kmwidth / kms_in_1_degree_of_long
    return bbox(centre,height,width)


def toggle_format(bbox):
    """Toggle between (minlat,maxlat,minlong,maxlong) and (maxlat,minlat,maxlong,minlong)"""
    return (bbox[1],bbox[0],bbox[3],bbox[2])

def in_bbox(point, bbox):
    """
    :param point: a (lat,long) point
    :param bbox: a bbox
    :return a bolean indicating whether  point is inside or omn bbox"""
    return bbox[0] <= point[0] <= bbox[1] and bbox[2] <= point[1] <= bbox[3]


# ===== Open street maps =====
tagsets = {
"any_amenity":{"amenity":True},
"positive_transport_amenities":
  {"amenity": ["bicycle_station", "bicycle_rental", "boat_rental", "bus_station"]},
"very_negative_amenities":
  {"amenity":["brothel","casino","gambling","love_hotel","swingerclub","prison"]},
"mid_distance_education_amenities":
  {"amenity": ["college","kindergarten","library","toy_library","training","school","university"]}
}

def collect_pois(bbox, poi_collection_specs):
    """
    collect all pois in a bounding box for every collection spec
    :param bbox: the bounding box in which to do the collection
    :param poi_collection_specs: a list/iterable of dictionaries, each with a value for 'tagset'
    :return the same list of dictionaries each with a new entry 'pois' containing the collected pois"""
    poi_specs = []
    for poi_collection_spec in poi_collection_specs:
        pois = ox.geometries_from_bbox(* toggle_format(bbox), tagsets[poi_collection_spec["tagset"]])
        print(f'{len(pois)} pois from {poi_collection_spec["tagset"]}')
        poi_spec = dict(poi_collection_spec)
        poi_spec["pois"]=pois
        poi_specs.append(poi_spec)
    return poi_specs
