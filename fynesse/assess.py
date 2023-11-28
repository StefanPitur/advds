import pandas as pd
import numpy as np
from haversine import haversine, Unit
import datetime
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
from .config import config
from . import access


def compute_tags_count_per_distance_category(pois_df, latitude, longitude, tags_list=config["default_tags_list"],
                                             category_distance_boundaries=config[
                                                 "default_category_distance_boundaries"]):
    tag_count_per_distance_category = {}

    for tag in tags_list:
        try:
            pois_by_tag = pois_df[pois_df[tag].notnull()]
        except Exception:
            for category_id, category_distance in category_distance_boundaries.items():
                tag_count_per_distance_category[str(tag) + "-" + category_id] = 0
            continue

        previous_matched_len = 0

        for category_id, category_distance in category_distance_boundaries.items():
            matched_pois = pois_by_tag[pois_by_tag["geometry"].apply(
                lambda geom: haversine((geom.centroid.x, geom.centroid.y), (longitude, latitude),
                                       unit=Unit.KILOMETERS) <= category_distance)]

            tag_count_per_distance_category[str(tag) + "-" + category_id] = len(matched_pois) - previous_matched_len
            previous_matched_len = len(matched_pois)

    return tag_count_per_distance_category


def join_osm_with_prices_coordinates(conn, bounding_box, min_date, max_date, house_type, house_sample_size=None):
    house_rows = access.get_prices_coordinates_for_coords_and_timedelta(conn, bounding_box, min_date, max_date,
                                                                        house_type)

    houses_df = pd.DataFrame(
        data=house_rows,
        columns=["price", "date_of_transfer", "postcode", "property_type", "new_build_flag", "tenure_type", "locality",
                 "town_city", "district", "county", "country", "latitude", "longitude"]
    )

    if house_sample_size is None:
        house_sample_size = len(houses_df)

    sampled_houses_df = houses_df.sample(n=house_sample_size)
    sampled_houses_features_categories_columns = sampled_houses_df.apply(
        lambda house: get_distances_features_from_a_house(house),
        axis=1)
    sampled_houses_features_categories_columns_df = pd.DataFrame(sampled_houses_features_categories_columns.tolist())

    sampled_houses_df.reset_index(drop=True, inplace=True)
    sampled_houses_features_categories_columns_df.reset_index(drop=True, inplace=True)

    return pd.concat([sampled_houses_df, sampled_houses_features_categories_columns_df], axis=1)


def display_corr_between_features_and_price(sampled_houses_df):
    filtered_houses_df = sampled_houses_df[[
        'price',
        'school-walking_distance', 'school-cycling_distance', 'school-driving_distance',
        'restaurant-walking_distance', 'restaurant-cycling_distance', 'restaurant-driving_distance',
        'leisure-walking_distance', 'leisure-cycling_distance', 'leisure-driving_distance',
        'healthcare-walking_distance', 'healthcare-cycling_distance', 'healthcare-driving_distance',
        'shop-walking_distance', 'shop-cycling_distance', 'shop-driving_distance',
        'public_transport-walking_distance', 'public_transport-cycling_distance', 'public_transport-driving_distance'
    ]]

    filtered_houses_df.fillna(0, inplace=True)
    return filtered_houses_df.corr()


def get_distances_features_from_a_house(house):
    bounding_box = compute_bounding_box_cardinals(float(house.latitude), float(house.longitude))
    house_pois = access.retrieve_pois_from_bbox_given_tags(bounding_box)

    return compute_tags_count_per_distance_category(
        house_pois,
        house.latitude,
        house.longitude
    )


def compute_bounding_box_cardinals(latitude, longitude,
                                   box_width=config["default_bounding_box"],
                                   box_height=config["default_bounding_box"]):
    north = latitude + box_height / 2
    south = latitude - box_height / 2
    west = longitude - box_width / 2
    east = longitude + box_width / 2

    return north, south, west, east


def get_date_range(date, days_range=365):
    date_split = date.split("-")
    datetime_date = datetime.date(int(date_split[0]), int(date_split[1]), int(date_split[2]))

    lower_bound_date = datetime_date - datetime.timedelta(days=(days_range // 2))
    upper_bound_date = datetime_date + datetime.timedelta(days=(days_range // 2))

    return lower_bound_date.strftime("%Y-%m-%d"), upper_bound_date.strftime("%Y-%m-%d")


def split_training_and_validation_data(data, train_size=config["default_training_size"]):
    return train_test_split(data, train_size=train_size)


def plot_test_against_predicted(Y_test, Y_predicted):
    bins = np.linspace(1, len(Y_test), len(Y_test))
    fig = plt.figure(figsize=(15, 10))
    plt.bar(bins - 0.1, Y_test, width=0.2, color='b', label="Actual house price")
    plt.bar(bins + 0.1, Y_predicted, width=0.2, color='g', label="Predicted house price")
    plt.legend(loc='upper left')
    plt.show()
