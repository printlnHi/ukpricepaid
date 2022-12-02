# This file contains code for suporting addressing questions in the data

"""Address a particular question that arises from the data"""

from .config import *

from . import access, assess

import datetime
import geopandas as gpd
import statsmodels.api as sm
import numpy as np
from shapely.geometry import Point
import pandas as pd
from sklearn import model_selection
from sklearn import metrics

def train_model(y,X):
    model = sm.OLS(y,X)
    return model.fit()

def grow_bounding_box(conn, target, initial_width=2, initial_height=2, max_growth=5, transaction_requirement=1000):
    growth = 1
    transactions = None
    while growth <= max_growth:
        bbox = access.km_bbox(target, initial_width*growth, initial_height*growth)
        transactions = access.inner_join(conn, bbox)
        if len(transactions) >= transaction_requirement:
            print(f"grew bounding box to {initial_width * growth} km wide and {initial_height * growth} km high.\n Found {len(transactions)} transactions")
            return (bbox, (initial_width * growth, initial_height * growth), transactions)
        else:
            growth = (growth * 1.5) // 0.5 * 0.5
    print(f"WARNING: grew bounding box to {initial_width * growth} km wide and {initial_height * growth} km high.\n Only found {len(transactions)} transactions, less than requirement of {transaciton_requirement}")

def predict_price_with_features(conn, latitude, longitude, date, property_type, make_poi_features, tagsets, monthly_average_price_for_type, to_return = "pred", output=0):
    """
    predict price for a property by constructing a certain set of poi_features, perform 5-fold cross validation to understand reliability.
    :param conn: a database connection
    :param longitude: the longitude coordinate (N-S)
    :param latitude: the latitude coordinate (E-W)
    :param date: the date to predict
    :param property_type: the property type that is being predicted
    :param monthly_average_price_for_type: a function which gives a GeoDataFrame of transactions returns a series indicating the monthly average price for each traction's date and property type
    :to_return either "pred" indicating that the prediction should be returned or "cross_MSE" indicating the cross validation average Mean Squared Error should be returned.
    :output an integer between 0 and 2 indicating the verbosity of intermediate output
    """
    target = (latitude,longitude)
    
    assert(access.in_bbox(target,access.mainland_bbox))
    assert(property_type in access.property_types)
    
    bbox, (width,height), transactions = grow_bounding_box(conn, target)
    transactions = transactions[["price","date_of_transfer","property_type","latitude","longitude","geometry"]]
    
    # Create transaction with dummy price for the query
    geometry = Point(latitude,longitude)
    query_transaction = gpd.GeoDataFrame([[-1,date,property_type,latitude,longitude,geometry]],columns=transactions.columns,index=[len(transactions)],crs="EPSG:4326")
    combined_transactions = pd.concat((transactions, query_transaction))
    combined_transactions.date_of_transfer = pd.to_datetime(combined_transactions.date_of_transfer)
    
    poi_bbox = access.km_bbox(target, width*2, height*2)
    features = assess.make_poi_features(poi_bbox, combined_transactions, tagsets, make_poi_features, max_dist=min(width,height)*1000)
    features["log-MAP"] = np.log(monthly_average_price_for_type(combined_transactions))
    features["const"] = np.ones(len(features))
    
    data_features = features.iloc[:-1]
    query_features = features.iloc[-1]
        
    y = np.log(transactions.price)
    X = data_features.to_numpy()
    query_X = query_features.to_numpy()

    n_splits = 5
    kf = model_selection.KFold(n_splits=n_splits,shuffle=True,random_state=0)
    
    cross_MAE = []
    cross_MSE = []
    
    for train_index, test_index in kf.split(X):
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]
        m_results = train_model(y, X)
        y_pred = m_results.predict(X_test)
        cross_MAE.append(metrics.mean_absolute_error(y_test,y_pred))
        cross_MSE.append(metrics.mean_squared_error(y_test,y_pred))
    if output > 0:
        print(f"{n_splits}-fold cross validation:\n model MAE {np.mean(cross_MAE):.4f}±{np.std(cross_MAE,ddof=1):.4f}\n model MSE {np.mean(cross_MSE):.4f}±{np.std(cross_MSE,ddof=1):.4f}")
    
    m_results = train_model(np.log(transactions.price),X)
    
    if output > 1:
        print(m_results.summary())
        print(m_results.params)
    
    pred = m_results.get_prediction(query_X)
    y_pred = np.exp(pred.summary_frame(alpha=0.05).iloc[0])  
    
    if to_return == "cross_MSE":
        return cross_MSE
    elif to_return == "pred":
        return y_pred
    else:
        raise ValueError("to_return should be 'cross_MSE' or 'pred'")
