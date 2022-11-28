from .config import *

from . import access

"""These are the types of import we might expect in this file
import pandas
import bokeh
import seaborn
import matplotlib.pyplot as plt
import sklearn.decomposition as decomposition
import sklearn.feature_extraction"""

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""

# ==== Inspecting database tables and calculating summary stats ====
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
mainland_bbox = {"maxlat":58.66667, "minlat":49.9591, "maxlong":1.766667, "minlong":-8.1775}

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
