from .config import *

from . import access

import osmnx as ox
import matplotlib.pyplot as plt

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""

# ===== Inspecting database tables and calculating summary stats =====
def numcol_summary(conn,table,col):
  results = access.execute(conn, f"SELECT max({col}), min({col}), avg({col}), stddev({col}) FROM {table}")[0]
  return  {"max":results[0],"min":results[1],"avg":results[2],"stddev":results[3]}

def group_count(conn,table,group_by):
  return access.execute(conn, f"SELECT {group_by}, COUNT(*) FROM {table} GROUP BY {group_by}")

def summarise_table(conn,table,numerical_cols,groupings,display=True):
  total_rows = access.execute(conn, "SELECT count(*) FROM pp_data")
  if display:
    print(f"total_rows: {total_rows}")

  numerical_cols_results = {}
  for col in numerical_cols:
    stat = numcol_summary(conn,table,col)
    numerical_cols_results[col] = stat
    if display:
      print(f'{col} summary statistics:\n max/avg/min:{stat["max"]:.3g}/{stat["avg"]:.3g}/{stat["min"]:.3g} stddev:{stat["stddev"]:.3g}')

  grouped_results = {}
  for group_by in groupings:
    group_counts = group_count(conn,table,group_by)
    grouped_results = {group_by:group_counts}
    if display:
      print(f"{group_by} group counts:\n {group_counts}")

  return {"total_rows":total_rows, "numerical_cols":numerical_cols_results, "groupings": grouped_results}

# ===== Open street maps =====
def plot_transactions_and_pois(bbox,transactions,poi_specs):
    """
    TODO: DOCUMENT
    """
    fig, ax = plt.subplots(figsize=(8,8))

    # Plot street edges
    graph = ox.graph_from_bbox(* access.toggle_format(bbox))
    nodes, edges = ox.graph_to_gdfs(graph)
    edges.plot(ax=ax, linewidth=1, edgecolor="dimgray")

    # Plot transactions
    transactions.plot(ax=ax, alpha=0.5, c=transactions["price"])

    # Plot pois
    for poi_spec in poi_specs:
        pois = poi_spec["pois"]
        if len(pois)>0:
            pois.plot(ax=ax, color=poi_spec.get("color"), markersize=poi_spec.get("markersize"), marker=poi_spec.get("marker"))

    plt.tight_layout()
    return (fig,ax)

def get_smallest_distances_2D(gdf1, gdf2, k=3):
    """
    for every entry in gdf1, calculate the k smallest distances from it to entries in gdf2
    :param gdf1: a GeoDataFrame
    :param gdf2: a GeoDataFrame
    :return a series of float64 series, containing k smallest distances (or less depending on length of gdf2)
    """
    #TODO-someday: use ox.distance.shortest path as alternative weighting
    ys = gdf2.geometry.to_crs(epsg=3310)
    return gdf1.geometry.to_crs(epsg=3310).map(lambda x: ys.distance(x).nsmallest(k))

def data():
    """Load the data from access and ensure missing values are correctly encoded as well as indices correct, column names informative, date and times correctly formatted. Return a structured data structure such as a data frame."""
    df = access.data()
    raise NotImplementedError

def query(data):
    """Request user input for some aspect of the data."""
    raise NotImplementedError

def view(data):
    """Provide a view of the data that allows the user to verify some aspect of its quality."""
    raise NotImplementedError

def labelled(data):
    """Provide a labelled set of data ready for supervised learning."""
    raise NotImplementedError
