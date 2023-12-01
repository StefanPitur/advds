import pandas as pd
import numpy as np
from haversine import haversine, Unit
import datetime
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
from . import access
from .config import *


def compute_tags_categories(tags):
    tags_matches = []
    for (i, j) in tags.items():
        if isinstance(j, bool):
            tags_matches.append((i, "*"))
        else:
            for k in j:
                tags_matches.append((i, k))

    return tags_matches


def calculate_distance(poi, latitude, longitude):
    point = (poi["geometry"].centroid.y, poi["geometry"].centroid.x)
    return haversine(point, (latitude, longitude), unit=Unit.KILOMETERS)


def compute_tags_metrics_for_location(latitude, longitude,
                                      tags=config["default_tags"],
                                      tags_distances=config["default_tags_distances"],
                                      tags_metrics=config["default_tags_metrics"],
                                      tags_aggregation=config["default_tags_aggregation"]):
    bounding_box = compute_bounding_box_cardinals(latitude, longitude)
    pois_df = access.retrieve_pois_from_bbox_given_tags(bounding_box, tags)

    pois_df["distance"] = pois_df.apply(calculate_distance, args=(latitude, longitude), axis=1)
    tags_list = compute_tags_categories(tags)

    tag_computed = {}

    for (tag_tuple, tag_distance, tag_metric) in zip(tags_list, tags_distances, tags_metrics):
        pois_df_by_distance = pois_df[pois_df["distance"] <= tag_distance]
        (tag, tag_value) = tag_tuple

        if tag_value == "*":
            aggregated_tag = tag
            filtered_pois = pois_df_by_distance[pois_df_by_distance[tag].notnull()]
        else:
            aggregated_tag = tags_aggregation[tag_value]
            filtered_pois = pois_df_by_distance[pois_df_by_distance[tag] == tag_value]

        if tag_metric == "distance":
            tag_computed[aggregated_tag] = min(
                tag_computed.get(aggregated_tag, filtered_pois["distance"].min()),
                filtered_pois["distance"].min()
            )
        elif tag_metric == "count":
            tag_computed[aggregated_tag] = tag_computed.get(aggregated_tag, 0) + np.sqrt(filtered_pois.shape[0])
        else:
            raise NotImplemented("Need to provide implementation for custom metrics")

    return tag_computed


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
    house_sample_size = min(len(houses_df), house_sample_size)

    sampled_houses_df = houses_df.sample(n=house_sample_size)
    sampled_houses_features_categories_columns = sampled_houses_df.apply(
        lambda house: compute_tags_metrics_for_location(float(house.latitude), float(house.longitude)),
        axis=1)
    sampled_houses_features_categories_columns_df = pd.DataFrame(sampled_houses_features_categories_columns.tolist())

    sampled_houses_df.reset_index(drop=True, inplace=True)
    sampled_houses_features_categories_columns_df.reset_index(drop=True, inplace=True)

    return pd.concat([sampled_houses_df, sampled_houses_features_categories_columns_df], axis=1)


def display_corr_between_features_and_price(sampled_houses_df):
    filtered_houses_df = sampled_houses_df[[
        'price',
        'school',
        'restaurant',
        'leisure',
        'healthcare',
        'food',
        'mall',
        'public_transport'
    ]]

    return filtered_houses_df.corr()


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
