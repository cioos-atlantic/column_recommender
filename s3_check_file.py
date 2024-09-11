from pathlib import Path
import pickle
import pandas as pd
import random
import numpy as np
import networkx as nx
from tqdm import tqdm
from s1_check_dbs import get_csv_variable_names, get_nc_variable_names


if __name__ == '__main__':
    # file_path = "/srv/data/erddap/dfo/bio/bottle/test_bottle_process.csv"
    check_file_path = "./buoy_data_001.csv"
    

    print("::: This program ignore the suggestion of Object type column ::: ")
    column_dtypes = pickle.load(open("./columns_min_max.pkl", 'rb'))
    co_occurrences = pickle.load(open('./csv_nc_col_relation.pkl', 'rb'))
    G = nx.Graph()

    file_extension = Path(check_file_path).suffix
    if file_extension.lower() == ".nc":
        df_columns = get_nc_variable_names(check_file_path)
    elif file_extension.lower() == ".csv":
        df_columns = get_csv_variable_names(check_file_path)
    new_columns_ = []
    columns_to_find_relation = []
    for col_name, mmd  in df_columns.items():
        if col_name in column_dtypes:
            df_col_dtype = mmd['dtype']
            if df_col_dtype not in column_dtypes[col_name]:
                print(f" ===> Unexpected Data Type of column: {col_name}")
                continue
            mm_ = column_dtypes[col_name][df_col_dtype]
            if (None in mm_.values()) and np.isnan(mmd['Min']) and np.isnan(mmd['Max']):
                continue    # ignoring columns having None values
            columns_to_find_relation.append(col_name)
            if df_col_dtype == object :
                continue
            elif (None in mm_.values()) or np.isnan(mmd['Min']) or np.isnan(mmd['Max']):
                print(f"Min/Max value in {col_name} .pkl file should be updated..! {mmd['Max']} <-> {mm_['Max']}  ~ {mmd['min']} <-> {mm_['Min']}")
                continue
            elif (mmd['Max'] > mm_["Max"]) or (mm_["Min"] > mmd['Min']):
                print(f"Min/Max value in {col_name} .pkl file should be updated..! {mmd['Max']} <-> {mm_['Max']}  ~ {mmd['min']} <-> {mm_['Min']}")
                continue
            # elif (df[col_name].max() <= mm_["Max"]) and (mm_["Min"] <= df[col_name].min()):
            #     print(f"Everything is good with {col_name}")
            #     continue
        else:
            print("A new column name detected..!")
            new_columns_.append(col_name)


    # Creating a Graph of column-names
    ignore_ = ["datetime", "lat", "lon", "longitude", "latitude", "buoy_name", "buoy_id", "time", "station"]
    for word1, word2 in tqdm(co_occurrences):
        if ((word1 in ignore_) or (word2 in ignore_)
                or ("name" in word1) or ("name" in word2)
                or ("time" in word1) or ("time" in word2)
                or ("date" in word1) or ("date" in word2)):
            continue

        G.add_node(word1)
        G.add_node(word2)
        G.add_edge(word1, word2)

    list_of_possible_nodes = { }
    for ncol_ in new_columns_:
        c_dtype = df_columns[ncol_]['dtype']
        c_max = df_columns[ncol_]['Max']
        c_min = df_columns[ncol_]['Min']
        for o_c in columns_to_find_relation:
            if G.has_node(o_c):
                ngbors = G.neighbors(o_c)
                for neigbor in ngbors:
                    if c_dtype in column_dtypes[neigbor].keys(): # check datatype of a node with unknow column-name
                        if neigbor not in list_of_possible_nodes:
                            list_of_possible_nodes[neigbor] = 0

                        list_of_possible_nodes[neigbor]+=1
                        if c_min >= column_dtypes[neigbor][c_dtype]["Min"]:
                            list_of_possible_nodes[neigbor] += 0.25
                        if c_max <= column_dtypes[neigbor][c_dtype]["Max"]:
                            list_of_possible_nodes[neigbor] += 0.25


    sum_of_all = sum(list_of_possible_nodes.values())
    list_of_possible_nodes = {k: ((v)/sum_of_all)*100 for k, v in list_of_possible_nodes.items()}


    def get_top_k_items(score_dict, top_k):
        sorted_items = sorted(score_dict.items(), key=lambda item: item[1], reverse=True)
        return sorted_items[:top_k]

    print(get_top_k_items(list_of_possible_nodes, 5))