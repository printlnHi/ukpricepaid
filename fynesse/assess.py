from .config import *

from . import access

import osmnx as ox
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from scipy import stats

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""

# ===== Inspecting database tables and calculating summary stats =====


def numcol_summary(conn, table, col):
    """
    Compute summary statics for a numerical column in a table
    :param conn: the database connection
    :param table: the table
    :param col: the column
    :return a dictionary with four keys - "min", "max", "avg", "stddev" and 3 float values
    """
    results = access.execute(
        conn,
        f"SELECT min({col}), max({col}), avg({col}), stddev({col}) FROM `{table}`")[0]
    return {
        "min": results[0],
        "max": results[1],
        "avg": results[2],
        "stddev": results[3]}


def group_count(conn, table, group_by):
    return access.execute(
        conn, f"SELECT {group_by}, COUNT(*) FROM `{table}` GROUP BY {group_by}")


def summarise_table(conn, table, numerical_cols, groupings, display=True):
    total_rows = access.execute(conn, "SELECT count(*) FROM pp_data")
    if display:
        print(f"total_rows: {total_rows}")

    numerical_cols_results = {}
    for col in numerical_cols:
        stat = numcol_summary(conn, table, col)
        numerical_cols_results[col] = stat
        if display:
            print(
                f'{col} summary statistics:\n min/avg/max:{stat["min"]:.3g}/{stat["avg"]:.3g}/{stat["max"]:.3g} stddev:{stat["stddev"]:.3g}')

    grouped_results = {}
    for group_by in groupings:
        group_counts = group_count(conn, table, group_by)
        grouped_results = {group_by: group_counts}
        if display:
            print(f"{group_by} group counts:\n {group_counts}")

    return {
        "total_rows": total_rows,
        "numerical_cols": numerical_cols_results,
        "groupings": grouped_results}

# ===== Open street maps =====


def plot_edges(bbox, **kwargs):
    graph = ox.graph_from_bbox(* access.toggle_format(bbox))
    nodes, edges = ox.graph_to_gdfs(graph)
    options = {"edgecolor": "dimgray", "linewidth": 0.5}  # Defaults
    options.update(kwargs)
    return edges.plot(**options)


# ===== Assessing and visualising POIs =====


def plot_transactions_and_pois(bbox, transactions, poi_specs):
    """
    TODO: DOCUMENT
    """
    fig, ax = plt.subplots(figsize=(8, 8))

    # Plot street edges
    plot_edges(bbox, ax=ax)

    # Plot transactions
    transactions.plot(ax=ax, alpha=0.5, c=transactions["price"])

    # Plot pois
    for poi_spec in poi_specs:
        pois = poi_spec["pois"]
        if len(pois) > 0:
            pois.plot(
                ax=ax,
                color=poi_spec.get("color"),
                markersize=poi_spec.get("markersize"),
                marker=poi_spec.get("marker"))

    plt.tight_layout()
    return (fig, ax)

def get_smallest_distances_2D(gdf1, gdf2, k=3):
    """
    For every entry in gdf1, calculate the k smallest distances from it to entries in gdf2
    :param gdf1: a GeoDataFrame
    :param gdf2: a GeoDataFrame
    :return a series of float64 series, containing k smallest distances (or less depending on length of gdf2)
    """
    # TODO-someday: use ox.distance.shortest path as alternative weighting
    ys = gdf2.geometry.to_crs(epsg=3310)
    return gdf1.geometry.to_crs(epsg=3310).map(
        lambda x: ys.distance(x).nsmallest(k))


# ==== Assessing and visualising transactions ====

def periodic_average(df, period, valcol, datecol):
    """
    Take the average of a column in a dataframe grouping by a date column
    :param df: the Dataframe
    :param period: a offset alias
    :param valcol: the name of the column containing the values to be averaged
    :param datecol: the name of the column containing the dates
    TODO: Maybe make this no longer DataFrame specific if possible
    """

    return df.groupby(df[datecol].dt.to_period(period)).apply(lambda df2: np.mean(df2[valcol]))


def periodic_average_by_group(df, period, valcol, datecol, groupcol):
    """
    Take the average of a column in a dataframe grouping by a date column and another column
    :param df: the Dataframe
    :param period: a offset alias
    :param valcol: the name of the column containing the values to be averaged
    :param datecol: the name of the column containing the dates
    :param groupcol: the name of the additional column to group by
    """
    return sample.groupby(sample[groupcol]).apply(lambda df: periodic_average(df, period, valcol, datecol))

def plot_price_trend(transactions, period="Y", **kwargs):
    """
    Plot average transaction price over time
    :param transactions: the transactions
    :param period: a offset alias
    """
    options = {"ylabel":"average price per "+period, "logy":True}
    options.update(kwargs)
    periodic_average(transactions, period, "price", "date_of_transfer").plot(**options)

def plot_price_trends(transactions, period="Y"):
    """
    Plot average transaction price over time, as a whole and broken down by property type
    :param transactions: the transactions
    :param period: a offset alias
    """

    fig, axs = plt.subplots(1,2,figsize=(16,8))
    plot_price_trend(transactions, ax=axs[0])

    by_type = periodic_average_by_group(df, period, "price", "date_of_transfer", "property_type")
    by_type.unstack(level=0).plot(ax=axs[1], logy=True,ylabel="average price per "+period)
    plt.tight_layout()

def plot_logprice_frequency(transactions):
    """
    Visualise the frequency of log-prices via a histogram, overall and by property type
    :param transactions: a GeoDataFrame of transactions
    """
    fig, axs = plt.subplots(2,3,figsize=(16,16))
    sns.histplot(np.log(transactions.price),kde=True,ax=axs[0][0]).set(title = "log price: all transactions")
    for i,property_type in enumerate(access.property_types):
        j = i+1
        sns.histplot(np.log(transactions[transactions.property_type==property_type].price),kde=True,ax=axs[j%2][j//2]).set(title=f"log price: type {property_type}")

def plot_average_price_geographically(transactions, bins_across=20, **kwargs):
    """
    Visualise the geographic distribution of average price
    :param transactions: a GeoDataFrame of transactions
    :param bins_across: the number of bins in each dimension
    :param **kwargs: arguments for sns.heatmap
    """
    average_prices = stats.binned_statistic_2d(transactions.longitude_f, transactions.latitude_f, transactions.price, bins=bins_across)
    x_centres = list(map(lambda coord: f"{coord:.3f}",(average_prices.x_edge[:-1]+average_prices.x_edge[1:])/2))
    y_centres = list(map(lambda coord: f"{coord:.3f}",(average_prices.y_edge[:-1]+average_prices.y_edge[1:])/2))
    df = pd.DataFrame(np.rot90(average_prices.statistic), index=pd.Index(y_centres[::-1],name="latitude"), columns=x_centres)
    sns.heatmap(df, **kwargs).set(title="average transaction price")
    
def plot_purchase_volume_geographically(transactions, bins_across=20, **kwargs):
    """
    Visualise the geographic distribution of purchase volume
    :param transactions: a GeoDataFrame of transactions
    :param bins_across: the number of bins in each dimension
    :param **kwargs: arguments for sns.histplot
    """
    txs = transactions
    bins = (np.linspace(min(txs.longitude_f),max(txs.longitude_f),bins_across),np.linspace(min(txs.latitude_f),max(txs.latitude_f),bins_across))
    sns.histplot(x=txs.longitude, y=txs.latitude, weights=txs.price, bins=bins, cbar=True, **kwargs).set(title = "total transaction volume (£)")

def plot_transactions_and_prices_geographically(transactions, bins_across=20, average_kwargs={}, volume_kwargs={}, geocodes = [], bbox=None, alpha = 0.1):
    """
    Produces 4 geographic plots showing transaction count, average price, total volume and a visualisation of all transactions, respectively.
    :param transactions: a GeoDataFrame of transactions
    :param bins_across: the number of bins in each dimension for the average price and total volume plots
    :param average_kwargs: keyword arguments to be passed to the plot_average_prices function
    :param volume_kwargs: keyword arguments to be passed to the plot_puchase_volume function
    :param geocodes: an iterable of geocodes to be looked up such that their outline can be inclued in the all-transactions visualisation
    :param bbox: if not None, a bounding box that will be passed to plot_edges for all-transactions visualisation
    :param alpha: the alpha value of transactions in the all-transactions visualisation
    """
    fig, axs = plt.subplots(2,2,figsize=(8,8))
    txs = transactions
    
    sns.histplot(x=txs.longitude,y=txs.latitude, cbar=True,ax=axs[0][0]).set(title="number of transactions")
    
    plot_average_price_geographically(transactions, bins_across=bins_across, ax=axs[0][1], **average_kwargs)
    
    plot_purchase_volume_geographically(transactions, bins_across=bins_across, ax=axs[1][0], **volume_kwargs)
    
    # Plot all transactions
    if bbox != None:
        plot_edges(bbox,ax=axs[1][1])
        
    for geocode in geocodes:
        area = ox.geocode_to_gdf(geocode)
        area.plot(ax=axs[1][1], facecolor="white", edgecolor="black", markersize=0.01, linewidth=0.2)
    p1,p99 = map(int,np.percentile(transactions.price,(1,99)))
    txs = txs.sample(frac=1) #Shuffle transactions to avoid aliasing
    txs.plot(ax=axs[1][1],c=txs["price"],cmap="inferno_r",vmin=p1,vmax=p99,alpha=alpha)
    plt.tight_layout()


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
