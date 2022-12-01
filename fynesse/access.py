from .config import *

import pymysql
import urllib.request
from os.path import exists
import geopandas as gpd
import pandas as pd
import osmnx as ox

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

# ==== Database setup and access ====


def execute(conn, *queries, output_queries=False):
    """
    Execute queries over connection before committing
    :param conn: database connection
    :param *args: the query or queries to be executed
    :param output_queries: whether each query should be printed before it is executed
    :return the result of all commands
    """
    cur = conn.cursor()
    for query in queries:
        print(query)
        cur.execute(query)
    cur.close()
    conn.commit()
    return cur.fetchall()


def create_database_ifne(conn):
    """
    Create the property_prices database if it doesn't exist
    :param conn: database connection"""
    return execute(
        conn,
        "CREATE DATABASE IF NOT EXISTS `property_prices` DEFAULT CHARACTER SET utf8 COLLATE utf8_bin",
        output_queries=True)


def create_connection_and_maybe_create_database_if_missing(
        user, password, host, database, port=3306, create_database_if_missing=None):
    """
    Create a database connection to the MariaDB database specified by the host url and database name. If this fails because the database does not exist, possibly create it
    :param user: username
    :param password: password
    :param host: host url
    :param database: database
    :param port: port number
    :param create_database_if_missing: either a boolean or None in which case the user is asked
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
        ER_BAD_DB_ERROR = 1049  # FROM https://mariadb.com/kb/en/mariadb-error-codes/

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

            if create_database_if_missing is None:
                result = input(
                    f"Do you want to create the database {database}? y/(n)")
                if result.strip().lower() in ["y", "yes"]:
                    return create_database()
            elif create_database_if_missing:
                return create_database()

    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")

    else:
        return conn


def create_pricepaid_table(conn):
    """
    Create the pp_data table according to the schema outlined in the notebook with an autoincrementing db_id primary key
    :param conn: database connection
    """
    return execute(
        conn,
        "DROP TABLE IF EXISTS `pp_data`",
        """CREATE TABLE IF NOT EXISTS `pp_data` (
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
        "ALTER TABLE `pp_data` ADD PRIMARY KEY (`db_id`), MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1")


def load_pricepaid_data(
        conn,
        dest_dir,
        years=range(
            1995,
            2023),
        source_base="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"):
    """
    Load whole-of-year HM Land Registry Price Paid Data by downloading annual datafiles unless already present and loading them into the pp_data table.
    :param conn: database connection
    :param dest_dir: the directory that the datafiles will be looked for in and downloaded into if absent
    :param years: an iterable of the years to load into the database
    :param source_base: the source URL to retrieve the annual datafile's from
    """
    for year in years:
        assert (1995 <= year and year <= 2022)
        filename = f"pp-{year}.csv"
        source = f"{source_base}/{filename}"
        destination = f"{dest_dir}/{filename}"
        if not exists(destination):
            print(f"downloading {source} to {destination}")
            urllib.request.urlretrieve(source, destination)
        load_file(
            conn,
            "pp_data",
            destination,
            display=True,
            enclosed_by_double_quote=True)


def create_pricepaid_indicies(conn):
    """
    Create pp_data indicies on postcode and date_of_transfer
    :param conn: database connection
    """
    return execute(
        conn,
        "CREATE INDEX `pp.postcode` USING HASH ON `pp_data` (postcode)",
        "CREATE INDEX `pp.date` USING HASH ON `pp_data` (date_of_transfer)")


def create_postcode_table(conn):
    """
    Create the postcode_data table according to the schema outlined in the notebook with an autoincrementing db_id primary key and an index on postcode
    :param conn: database connection
    """
    execute(
        conn,
        "DROP TABLE IF EXISTS `postcode_data`",
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
        "CREATE INDEX `po.postcode` USING HASH ON `postcode_data` (postcode)")


def clean_postcode_data(conn, backup_table=None):
    """
    Remove entries from postcode_data where the country is not England or Wales or the longitude is 0
    :param conn: database connection
    :param backup_table: None or str, if str the name of a new table postcode_data will be copied tobefore cleaning
    """
    inclusion_criteria = "((country = 'England' OR country = 'Wales') AND longitude != 0)"
    if backup_table is None:
        execute(
            conn,
            f"DELETE FROM postcode_data WHERE NOT {inclusion_criteria}")
    else:
        execute(conn, f"RENAME TABLE postcode_data TO `{backup_table}`")
        create_postcode_table(conn)
        execute(
            conn,
            f"INSERT INTO postcode_data SELECT * FROM `{backup_table}` WHERE {inclusion_criteria}")


def select_top(conn, table, n):
    """
    Select the top rows of a table
    :param conn: database connection
    :param table: the table to query
    :param n: number of rows to select
    """
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM `{table}` LIMIT {n}')
    rows = cur.fetchall()
    return rows


def head(conn, table, n=5):
    """
    Print the top rows of a table
    :param conn: database connection
    :param table: the table to query
    :param n: number of rows to print
    """
    rows = select_top(conn, table, n)
    for r in rows:
        print(r)


def load_file(conn, table, file, display=False,
              enclosed_by_double_quote=False):
    """
    Lode local data file into table
    :param conn: database connection
    :param table: the table to query
    :param file: the local file to load from
    """
    if display:
        print(f"Loading {file} into `{table}`")
    enclosed_specifier = "ENCLOSED BY '\"'" if enclosed_by_double_quote else ""
    command = (
        f"LOAD DATA LOCAL INFILE '{file}' INTO TABLE `{table}` FIELDS TERMINATED BY ',' {enclosed_specifier} LINES STARTING BY '' TERMINATED BY '\\n'")
    return execute(conn, command)


def inner_join(
        conn,
        bbox=None,
        invert_bbox=False,
        date_bound=None,
        limit=None,
        one_in=None,
        output_query=False):
    """
    Perform a join on postcode_data and pp_data on the postcode column
    :param conn: database connection
    :param bbox: bbox to constrain coordinates
    :param invert_bbox: if False, coordinates must be within bbox, if True, coordinates must be outside
    :param date_bound: a tuple of dates that date_of_transfer must be within
    :param limit: the maximum number of rows that may be returned
    :param one_in: the reciprocal of the probability that a row is selected
    :param output_query: whether the SQL query should be printed
    """
    conditions = []
    if one_in is not None:
        conditions.append(f"RAND(pp_data.db_id)<{1.0/one_in}")
    if bbox is not None:
        if invert_bbox:
            conditions.append(
                f"(lattitude < {bbox[0]} OR {bbox[1]} < lattitude OR longitude < {bbox[2]} OR {bbox[3]} < longitude)")
        else:
            conditions.append(
                f"lattitude between {bbox[0]} AND {bbox[1]} AND longitude between {bbox[2]} and {bbox[3]}")
    if date_bound is not None:
        from_date, to_date = date_bound
        conditions.append(
            f"DATE(date_of_transfer) between '{from_date}' and '{to_date}'")
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
    results = execute(conn, query, output_queries=output_query)
    gdf = gpd.GeoDataFrame(
        results,
        columns=[
            "price",
            "date_of_transfer",
            "postcode",
            "property_type",
            "new_build_flag",
            "tenure_type",
            "locality",
            "town_city",
            "district",
            "county",
            "country",
            "latitude",
            "longitude"])
    gdf.geometry = gpd.points_from_xy(
        gdf.longitude, gdf.latitude, crs="EPSG:4326")
    gdf.date_of_transfer = pd.to_datetime(gdf.date_of_transfer)
    return gdf


# ===== Bounding boxes and example coordinates =====
"""
The sane bounding box (bbox) format is
  (minlat,maxlat,minlong,maxlong)
  however osmnx's bbox functions use
  (maxlat,minlat,maxlong,minlong)
All sample bboxs and return types are of the sane format, unless otherwise specified
"""

example_coords = {"selwyn": (52.2011, 0.1056), "leman_locke_aldagte": (
    51.5145, -0.0708), "london": (51.5072, 0.1276)}
mainland_bbox = (49.9591, 55.8111, -5.716667, 1.766667)
property_types = ["D", "S", "T", "F", "O"]


def bbox(centre, width, height):
    """
    Creates a geographic bounding box
    :param centre: the centre of the bounding box
    :param width: the angle of lattitude of the box
    :param height: the angle of longitude of the box
    :return the bbox
    """
    lat, long = centre
    return (
        lat - height / 2,
        lat + height / 2,
        long - width / 2,
        long + width / 2)


"""
bbox comparison to use for documentation
bbox(access.selwyn_coords,0.1,0.1) -> (52.1511, 52.251099999999994, 0.0556, 0.15560000000000002)
km_bbox(access.selwyn_coords,11.119,11.119) -> (52.15110228594827, 52.251097714051724, 0.02402262622942146, 0.18717737377057853)
"""


def km_bbox(centre, kmwidth, kmheight):
    """Creates a geographic bounding box s.t. the east-west line through the centre is the required width and the north-south line through the centre is the required height
    :param centre: the geographic lat,long centre
    :param kmwidth: the width of the E-W line in kilometers
    :param kmheight: the height of the N-S line in kilometers
    :return a bbox
    """
    lat, long = centre
    kms_in_1_degree_of_lat = ox.distance.great_circle_vec(
        0, long, 1, long) / 1000
    kms_in_1_degree_of_long = ox.distance.great_circle_vec(
        lat, 0, lat, 1) / 1000
    height = kmheight / kms_in_1_degree_of_lat
    width = kmwidth / kms_in_1_degree_of_long
    return bbox(centre, height, width)


def toggle_format(bbox):
    """Toggle between (minlat,maxlat,minlong,maxlong) and (maxlat,minlat,maxlong,minlong)
    :param bbox: a bbox in one format
    :return the bbox is the other format"""
    return (bbox[1], bbox[0], bbox[3], bbox[2])


def in_bbox(point, bbox):
    """
    :param point: a (lat,long) point
    :param bbox: a bbox
    :return a bolean indicating whether the point is inside bbox"""
    return bbox[0] <= point[0] <= bbox[1] and bbox[2] <= point[1] <= bbox[3]


# ===== Open street maps =====
tagsets = {
    "any_amenity": {
        "amenity": True},
    "positive_transport_amenities": {
        "amenity": [
            "bicycle_station",
            "bicycle_rental",
            "boat_rental",
            "bus_station"]},
    "very_negative_amenities": {
        "amenity": [
            "brothel",
            "casino",
            "gambling",
            "love_hotel",
            "swingerclub",
            "prison",
            "stripclub"]},
    "mid_distance_education_amenities": {
        "amenity": [
            "college",
            "kindergarten",
            "library",
            "toy_library",
            "training",
            "school",
            "university"],
        "place_of_worship": {
            "amenity": "place_of_worship"}}}


def collect_pois(bbox, tagset):
    """
    collect all pois in a bounding for a tagset
    :param bbox: the bbox
    :param tagset: the open street maps tagset"""
    return ox.geometries_from_bbox(* toggle_format(bbox), tagset)


def collect_pois_from_collection_spec(bbox, poi_collection_specs):
    """
    collect all pois in a bounding box for every collection spec
    :param bbox: the bounding box in which to do the collection
    :param poi_collection_specs: a list/iterable of dictionaries, each with a value for 'tagset'
    :return the same list of dictionaries each with a new entry 'pois' containing the collected pois"""
    poi_specs = []
    for poi_collection_spec in poi_collection_specs:
        pois = collect_pois(bbox, tagsets[poi_collection_spec["tagset"]])
        print(f'{len(pois)} pois from {poi_collection_spec["tagset"]}')
        poi_spec = dict(poi_collection_spec)
        poi_spec["pois"] = pois
        poi_specs.append(poi_spec)
    return poi_specs
