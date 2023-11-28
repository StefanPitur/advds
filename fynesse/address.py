# This file contains code for supporting addressing questions in the data

import assess
import access
import numpy as np
import statsmodels.api as sm
import pandas as pd


def fit_model(houses_data):
    houses_data["latitude"] = houses_data["latitude"].astype(float)
    houses_data["longitude"] = houses_data["longitude"].astype(float)

    training_data, test_data = assess.split_training_and_validation_data(houses_data, 0.9)
    X_training, Y_training = training_data.drop(columns=['price']), training_data['price']
    X_test, Y_test = test_data.drop(columns=['price']), test_data['price']

    features_columns = [
        "latitude", "longitude",
        "school-walking_distance", "school-cycling_distance", "school-driving_distance",
        "restaurant-walking_distance", "restaurant-cycling_distance", "restaurant-driving_distance",
        "leisure-walking_distance", "leisure-cycling_distance", "leisure-driving_distance",
        "healthcare-walking_distance", "healthcare-cycling_distance", "healthcare-driving_distance",
        "shop-walking_distance", "shop-cycling_distance", "shop-driving_distance",
        "public_transport-walking_distance", "public_transport-cycling_distance", "public_transport-driving_distance"
    ]

    design = np.concatenate([X_training[col].values.reshape(-1, 1) for col in features_columns], axis=1)
    m_linear_basis = sm.OLS(Y_training, design)
    results_linear_basis = m_linear_basis.fit()

    design_pred = np.concatenate([X_test[col].values.reshape(-1, 1) for col in features_columns], axis=1)
    Y_predicted = results_linear_basis.predict(design_pred)
    assess.plot_test_against_predicted(Y_test, Y_predicted)

    print(results_linear_basis.summary())
    return results_linear_basis


def make_prediction(fitted_model, latitude, longitude):
    house_prediction_bounding_box = assess.compute_bounding_box_cardinals(latitude, longitude)
    house_prediction_data = assess.compute_tags_count_per_distance_category(
        access.retrieve_pois_from_bbox_given_tags(house_prediction_bounding_box),
        latitude,
        longitude
    )
    house_prediction_data["latitude"] = latitude
    house_prediction_data["longitude"] = longitude
    new_entry_df = pd.DataFrame([house_prediction_data])

    print(f"Predicted price - {fitted_model.predict(new_entry_df)}")
    return fitted_model.predict(new_entry_df)


def predict_price(conn, latitude, longitude, date, property_type):
    min_date, max_date = assess.get_date_range(date)
    bounding_box = assess.compute_bounding_box_cardinals(latitude, longitude)

    houses_data = assess.join_osm_with_prices_coordinates(conn, bounding_box, min_date, max_date, property_type, 25)
    fitted_model = fit_model(houses_data)
    make_prediction(fitted_model, latitude, longitude)