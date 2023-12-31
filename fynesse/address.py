# This file contains code for supporting addressing questions in the data

from . import assess
import numpy as np
import statsmodels.api as sm
import pandas as pd
from datetime import datetime


def fit_model(houses_data):
    """
        Generates fit model for data.
    """

    houses_data["latitude"] = houses_data["latitude"].astype(float)
    houses_data["longitude"] = houses_data["longitude"].astype(float)

    houses_data["delta_date_of_transfer"] = pd.to_datetime(houses_data["date_of_transfer"]).astype(int) // 10 ** 9 / 86400

    training_data, test_data = assess.split_training_and_validation_data(houses_data, 0.9)
    X_training, Y_training = training_data.drop(columns=['price']), training_data['price']
    X_test, Y_test = test_data.drop(columns=['price']), test_data['price']

    features_columns = [
        "delta_date_of_transfer",
        "school",
        "restaurant",
        "leisure",
        "healthcare",
        "food",
        "mall",
        "public_transport"
    ]

    design = np.concatenate([X_training[col].values.reshape(-1, 1) for col in features_columns], axis=1)
    m_linear_basis = sm.OLS(Y_training, design)
    results_linear_basis = m_linear_basis.fit_regularized(alpha=0.5, L1_wt=1)

    design_pred = np.concatenate([X_test[col].values.reshape(-1, 1) for col in features_columns], axis=1)
    Y_predicted = results_linear_basis.predict(design_pred)
    assess.plot_test_against_predicted(Y_test, Y_predicted)

    coefficients = results_linear_basis.params

    y_mean = np.mean(Y_test)
    TSS = np.sum((Y_test - y_mean) ** 2)
    RSS = np.sum((Y_test - Y_predicted) ** 2)
    R_squared = 1 - (RSS / TSS)
    print("R^2 - {}\n".format(R_squared))
    print("Coefficients - {}\n".format(coefficients))

    return results_linear_basis


def make_prediction(fitted_model, latitude, longitude, date):
    """
        Makes prediction using the fitted model and data about given latitude and longitude.
    """

    house_prediction_data = assess.compute_tags_metrics_for_location(
        latitude,
        longitude
    )

    date_split = date.split("-")
    datetime_date = datetime(int(date_split[0]), int(date_split[1]), int(date_split[2]))
    unix_start_date = datetime(1970, 1, 1)

    house_prediction_data["delta_date_of_transfer"] = (datetime_date - unix_start_date).days
    new_entry_df = pd.DataFrame([house_prediction_data])
    prediction = abs(fitted_model.predict(new_entry_df)[0])
    return prediction


def predict_price(conn, latitude, longitude, date, property_type):
    """
        Predictor wrapper over helper methods.
    """

    min_date, max_date = assess.get_date_range(date)
    bounding_box = assess.compute_bounding_box_cardinals(latitude, longitude)

    houses_data = assess.join_osm_with_prices_coordinates(conn, bounding_box, min_date, max_date, property_type)
    fitted_model = fit_model(houses_data)
    return make_prediction(fitted_model, latitude, longitude, date)
