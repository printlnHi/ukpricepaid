from .config import *

from . import access

import osmnx as ox
import geopandas as gpd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import seaborn as sns
from scipy import stats
from scipy import spatial
from collections import Counter

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
    options = {
        "edgecolor": "dimgray",
        "linewidth": 0.5,
        "zorder": 0}  # Defaults
    options.update(kwargs)
    return edges.plot(**options)


# ==== Assessing and visualising transactions ====

example_locations = ["aldgate", "selwyn", "beverly"]
example_coords = {"selwyn": (52.2011, 0.1056), "aldgate": (
    51.5145, -0.0708), "beverly": (53.865815, -0.451361)}


def periodic_average(df, period, valcol, datecol):
    """
    Take the average of a column in a dataframe grouping by a date column
    :param df: the Dataframe
    :param period: a offset alias
    :param valcol: the name of the column containing the values to be averaged
    :param datecol: the name of the column containing the dates
    TODO: Maybe make this no longer DataFrame specific if possible
    """

    return df.groupby(
        df[datecol].dt.to_period(period)).apply(
        lambda df2: np.mean(
            df2[valcol]))


def periodic_average_by_group(df, period, valcol, datecol, groupcol):
    """
    Take the average of a column in a dataframe grouping by a date column and another column
    :param df: the Dataframe
    :param period: a offset alias
    :param valcol: the name of the column containing the values to be averaged
    :param datecol: the name of the column containing the dates
    :param groupcol: the name of the additional column to group by
    """
    return df.groupby(
        df[groupcol]).apply(
        lambda df2: periodic_average(
            df2,
            period,
            valcol,
            datecol))


def plot_price_trend(transactions, period="Y", **kwargs):
    """
    Plot average transaction price over time
    :param transactions: the transactions
    :param period: a offset alias
    """
    options = {"ylabel": "average price per " + period, "logy": True}
    options.update(kwargs)
    periodic_average(
        transactions,
        period,
        "price",
        "date_of_transfer").plot(
        **options)


def plot_price_trends(transactions, period="Y", axs=None, title=""):
    """
    Plot average transaction price over time, as a whole and broken down by property type
    :param transactions: the transactions
    :param period: a offset alias
    :param axs: a sequence of axes that each plot will be plotted on. If none, a 1x plot of figsize (16,8) will be created
    """

    axs_is_none = axs is None
    if axs_is_none:
        fig, axs = plt.subplots(1, 2, figsize=(16, 8))
    plot_price_trend(
        transactions,
        ax=axs[0],
        ylabel=f"{title} average price per {period}")

    by_type = periodic_average_by_group(
        transactions,
        period,
        "price",
        "date_of_transfer",
        "property_type")
    by_type.unstack(
        level=0).plot(
        ax=axs[1],
        logy=True,
        ylabel=f"{title} average price per {period}")
    if axs_is_none:
        plt.tight_layout()


def plot_logprice_frequency(transactions, axs=None, title=""):
    """
    Visualise the frequency of log-prices via a histogram, overall and by property type
    :param transactions: a GeoDataFrame of transactions
    :param axs: a sequence of 6 axes that each histogram should be plotted on. If None, a 2x3 plot of figsize (16,16) will be created
    :param title_name: a string to be included at the stand of each plot's title
    """
    axs_is_none = axs is None
    if axs_is_none:
        fig, axs = plt.subplots(2, 3, figsize=(16, 16))
        axs = axs.flatten()
    assert (len(axs) == 6)

    sns.histplot(
        np.log(
            transactions.price),
        kde=True,
        ax=axs[0]).set(
            title=f"{title} log price: all transactions")
    for i, property_type in enumerate(access.property_types):
        sns.histplot(np.log(transactions[transactions.property_type == property_type].price),
                     kde=True, ax=axs[i + 1]).set(title=f"{title} log price: type {property_type}")
    if axs_is_none:
        plt.tight_layout()


def plot_average_price_geographically(transactions, bins_across=20, **kwargs):
    """
    Visualise the geographic distribution of average price
    :param transactions: a GeoDataFrame of transactions
    :param bins_across: the number of bins in each dimension
    :param **kwargs: arguments for sns.heatmap
    """
    options = {"norm": LogNorm()}
    options.update(kwargs)

    average_prices = stats.binned_statistic_2d(
        transactions.longitude_f,
        transactions.latitude_f,
        transactions.price,
        bins=bins_across)
    x_centres = list(map(
        lambda coord: f"{coord:.3f}", (average_prices.x_edge[:-1] + average_prices.x_edge[1:]) / 2))
    y_centres = list(map(
        lambda coord: f"{coord:.3f}", (average_prices.y_edge[:-1] + average_prices.y_edge[1:]) / 2))
    df = pd.DataFrame(np.rot90(average_prices.statistic), index=pd.Index(
        y_centres[::-1], name="latitude"), columns=x_centres)
    sns.heatmap(df, **options).set(title="average transaction price")


def plot_purchase_volume_geographically(
        transactions, bins_across=20, **kwargs):
    """
    Visualise the geographic distribution of purchase volume
    :param transactions: a GeoDataFrame of transactions
    :param bins_across: the number of bins in each dimension
    :param **kwargs: arguments for sns.histplot
    """
    txs = transactions
    bins = (np.linspace(min(txs.longitude_f), max(txs.longitude_f), bins_across),
            np.linspace(min(txs.latitude_f), max(txs.latitude_f), bins_across))
    sns.histplot(x=txs.longitude,
                 y=txs.latitude,
                 weights=txs.price,
                 bins=bins,
                 cbar=True,
                 **kwargs).set(title="total transaction volume (£)")


def plot_transactions(transactions, **kwargs):
    options = {"hue_norm": LogNorm(), "alpha": 0.1}
    options.update(kwargs)
    sns.scatterplot(
        x=transactions.longitude,
        y=transactions.latitude,
        hue=transactions.price,
        **options)


def plot_transactions_and_prices_geographically(
        transactions,
        bins_across=20,
        average_kwargs={},
        volume_kwargs={},
        geocodes=[],
        bbox=None,
        alpha=0.1):
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
    fig, axs = plt.subplots(2, 2, figsize=(8, 8))
    txs = transactions

    sns.histplot(
        x=txs.longitude,
        y=txs.latitude,
        cbar=True,
        ax=axs[0][0]).set(
        title="number of transactions")

    plot_average_price_geographically(
        transactions,
        bins_across=bins_across,
        ax=axs[0][1],
        **average_kwargs)

    plot_purchase_volume_geographically(
        transactions,
        bins_across=bins_across,
        ax=axs[1][0],
        **volume_kwargs)

    if bbox is not None:
        plot_edges(bbox, ax=axs[1][1])

    for geocode in geocodes:
        area = ox.geocode_to_gdf(geocode)
        area.plot(
            ax=axs[1][1],
            facecolor="white",
            edgecolor="black",
            markersize=0.01,
            linewidth=0.2)
    p1, p99 = map(int, np.percentile(transactions.price, (1, 99)))
    txs = txs.sample(frac=1)  # Shuffle transactions to avoid aliasing
    plot_transactions(txs, ax=axs[1][1], alpha=alpha)
    plt.tight_layout()


# ===== Assessing and visualising POIs =====

def plot_transactions_and_pois(bbox, transactions, poi_specs, **kwargs):
    fig, ax = plt.subplots(figsize=(8, 8))

    plot_edges(bbox, ax=ax)

    plot_transactions(transactions, ax=ax, **kwargs)

    # Plot pois
    for poi_spec in poi_specs:
        pois = poi_spec["pois"]
        if len(pois) > 0:
            pois.plot(
                ax=ax,
                **poi_spec.get("plot_kwargs", {})
            )

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


def display_every_amenity(bbox, transactions, ax=None, **kwargs):
    """
    Plot all amenities in a bounding box, and transactions, and print the values of the amenity tags
    :param bbox: the bounding box
    :param transactions: a GeoDataFrame of transactions
    :param ax: the axis to plot on, if None a (8,8) plot is created
    """
    ax_is_none = ax is None
    if ax_is_none:
        fig, ax = plt.subplots(figsize=(8, 8))

    plot_edges(bbox, ax=ax)

    plot_transactions(transactions, ax=ax, **kwargs)

    all_amenities = access.collect_pois(bbox, {"amenity": True})
    all_amenities.plot(ax=ax, alpha=0.5)

    for amenity, count in Counter(all_amenities["amenity"]).most_common():
        print(f"{amenity}: {count}", end="  ")
    if ax_is_none:
        plt.tight_layout()


def get_distances_2D(gdf1, gdf2, k=50):
    """
    Calculates an ordered list of the k closest centroid distances from gdf2 geometry to each point in gdf1 geometry, assuming geometry is latitude,longitude
    :param gdf1: a GeoDataFrame
    :param gdf2: a GeoDataFrame
    :param k: the maximium number of distances to be included in each list
    """
    def convert_point_array(arr):
        return np.array(list(map(lambda point: [point.x, point.y], arr)))
    xs = convert_point_array(gdf1.geometry.to_crs(epsg=3310).centroid)
    ys = convert_point_array(gdf2.geometry.to_crs(epsg=3310).centroid)
    matrix = spatial.distance.cdist(xs, ys)
    return np.sort(matrix, axis=1)[:, :k]


def make_poi_features(bbox, transactions, tagsets, to_make, max_dist=5000):
    """
    Gather all pois in a certain bounding box belonging to tagsets and make features for each transactions based on pois in their vicint
    :param bbox: the bbox in which to gather pois
    :param transactions: a GeoDataFrame of transactions
    :param tagsets: a dictionary of tagsets
    :param to_make: a sequence of tuples specifiying the features to create where the first argument is either "closest" or ("count",radius) and the second argument is a tagset. A 'closest' feature is the distance to the nearest poi of the tagset from each transaction. A 'count' feature is the number of pois of the tagset within radius meters of each transaction.
    :param max_dist: the maximum distance in meters to clip 'closest' features to
    :return a dataframe with the same index as transactions, and a column for each tuple. a closest
    """
    pois = {}
    print("downloading all tagsets")
    for tagset in tagsets:
        pois[tagset] = access.collect_pois(bbox, tagsets[tagset])

    print("computing distances to transactions")
    distances = {}
    for tagset in tagsets:
        if len(pois[tagset]) == 0:
            print(f"no POIs for {tagset}")
            continue
        distances[tagset] = get_distances_2D(transactions, pois[tagset])

    print("calculating features")
    result = gpd.GeoDataFrame(index=transactions.index)
    for (metric, tagset) in to_make:
        name = f"{metric}-{tagset}"
        if tagset not in distances:
            print(f"no distances for {tagset}")
            continue
        feature = None
        if metric == "closest":
            feature = np.clip(distances[tagset][:, 0], 50, max_dist)
        if type(metric) == tuple and metric[0] == "count":
            radius = metric[1]
            feature = np.sum(distances[tagset] < radius, axis=1)
        if feature is not None:
            result[name] = np.array(feature)
    return result


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
