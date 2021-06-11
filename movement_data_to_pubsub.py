import networkx as nx
import osmnx as ox
import pandas as pd
import time
import random
import os
import datetime
import pytz
# from google.cloud import pubsub_v1
#
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "bq-looker-gis.json"

# Defining the map boundaries
north, east, south, west = 43.971564, -79.002807, 43.342461, -79.895757
# Downloading the map as a graph object
G = ox.graph_from_bbox(north, south, east, west, network_type='drive')

# Define destination locations
s1 = (43.639492, -79.379882)  # harbour front
s2 = (43.641138, -79.388900)  # rodgers centre
s3 = (43.640113, -79.374857)  # ferry terminal
s4 = (43.651347, -79.382221)  # scotia bank
s5 = (43.653939, -79.378925)  # massey hall
s6 = (43.706773, -79.398314)  # yonge-eglington
s7 = (43.677557, -79.409451)  # casa loma
s8 = (43.636669, -79.421906)  # liberty village
s9 = (43.666982, -79.394483)  # rom
s10 = (43.651759, -79.397942)  # chinatown
s11 = (43.648570, -79.414358)  # trinity bellwoods park
s12 = (43.655312, -79.435155)  # dufferin mall
s13 = (43.648660, -79.464024)  # high park

major_points = [s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13]

# Define various user routes
users = 40
users_route = [None] * users

points_in_route = [3, 4]

random.seed(1)

for i in range(users):
    points = random.sample(points_in_route, 1)[0]
    users_route[i] = random.sample(major_points, points)

cols = ['Unix_Time', 'User', 'Long', 'Lat']
master_df = pd.DataFrame(columns=cols)

user = 0
start_time = int(time.time())

for u_r in users_route:
    user += 1
    time_l = []
    user_l = []
    long_l = []
    lat_l = []
    time_l = []
    user_current_time = start_time
    for i in range(0, len(u_r) - 1):
        origin_node = ox.get_nearest_node(G, u_r[i])
        destination_node = ox.get_nearest_node(G, u_r[i + 1])
        route = nx.shortest_path(G, origin_node, destination_node, weight='length')

        for index, j in enumerate(route):
            point = G.nodes[j]
            long_l.append(point['x'])
            lat_l.append(point['y'])
            user_l.append('user' + str(user))
            time_l.append(user_current_time)
            user_current_time += 10

    df = pd.DataFrame(columns=cols)
    df['User'] = user_l
    df['Long'] = long_l
    df['Lat'] = lat_l
    df['Unix_Time'] = time_l
    master_df = master_df.append(df, ignore_index=True)

# master_df = df = pd.read_csv('input.csv', index_col=None, usecols=['Unix_Time', 'User', 'Long', 'Lat'])

timestamps = master_df['Unix_Time'].unique().copy()
timestamps.sort()

project_id = "drvn-sb"
topic_name = "gis_users_for_cloud_function"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
project_path = publisher.project_path(project_id)


i = 0
step = 100
while i <= master_df.shape[0] - 1:
    print(i)
    data_to_insert = master_df.loc[i: i + step]
    i = i + step + 1
    print("********************************DATA**************************************")
    print(data_to_insert)
    # data = data_to_insert.to_json(orient='records')
    # data = str(data).encode("utf-8")
    # future = publisher.publish(topic_path, data)
    # print("********************************RESULT**************************************")
    # print(future.result())
    time.sleep(10)

# i = 1
# step = 3
# while i < len(timestamps):
#     min_index = i
#     max_index = min(i + step, len(timestamps))
#     timestamps_to_insert = timestamps[min_index:max_index]
#
#     data_to_insert = master_df[master_df["Unix_Time"].isin(timestamps_to_insert)]
#
#     print("********************************DATA**************************************")
#     print(data_to_insert)
#     data = data_to_insert.to_json(orient='records')
#     data = str(data).encode("utf-8")
#     future = publisher.publish(topic_path, data)
#     print("********************************RESULT*************************************")
#     print(future.result())
#
#     i += step
#     print("%s%% Complete" % round(i * 100 / len(timestamps)))
#     print("-------------------------------------------------")
#     time.sleep(5)
