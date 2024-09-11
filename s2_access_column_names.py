import pickle
import numpy as np
import sys
import re
import itertools
import ast
import pandas as pd

def dtype(type_name):
    return np.dtype(type_name)


def transform_string(input_string):
    # Convert the input string into a dictionary
    input_dict = ast.literal_eval(input_string)

    # Initialize an empty dictionary to store the transformed data
    output_dict = {}

    # Iterate over each key in the input dictionary
    for key, value in input_dict.items():
        # Check if 'Min' is not 'nan' and 'Max' is not 'masked'
        if pd.notna(value['Min']) and value['Max'] != 'masked':
            # Add the key and its value to the output dictionary
            output_dict[key] = {key: value}

    return output_dict


def observe_column_datatypes( col_file, column_dtypes = {}, line_counter = 0):
    with open(col_file, 'r') as fin:
        sys.stdout.write(f"Reading f{col_file} \n")
        line = fin.readline()
        while line:
            line_counter+=1
            sys.stdout.write(f' \r Line number: {line_counter} ')
            sys.stdout.flush()
            line = line.replace("<class 'str'>", "dtype('str_')")
            line = line.replace("array", "np.array")
            line = re.sub(r"(nan|masked|'na')","None", line)

            # dtype_dict = eval(line , {"dtype": dtype, "np": np})
            dtype_dict = eval(line)

            for c_name, c_dtype in dtype_dict.items():
                c_name = c_name.lower()
                if c_name not in column_dtypes:
                    column_dtypes[c_name] = {}
                if c_dtype["dtype"] not in column_dtypes[c_name]:
                    column_dtypes[c_name][c_dtype["dtype"]] = {}

                if "Max" not in column_dtypes[c_name][c_dtype["dtype"]]:
                    column_dtypes[c_name][c_dtype["dtype"]]["Max"] = c_dtype["Max"]
                elif c_dtype["Max"] is not None:
                    if type(column_dtypes[c_name][c_dtype["dtype"]]["Max"]) != type(c_dtype["Max"]):
                        if type(column_dtypes[c_name][c_dtype["dtype"]]["Max"]) == float:
                            c_dtype["Max"] = float(c_dtype["Max"])
                            c_dtype["Min"] = float(c_dtype["Min"])
                    if column_dtypes[c_name][c_dtype["dtype"]]["Max"] is not None:
                        column_dtypes[c_name][c_dtype["dtype"]]["Max"] = max(column_dtypes[c_name][c_dtype["dtype"]]["Max"], c_dtype["Max"])
                    else:
                        column_dtypes[c_name][c_dtype["dtype"]]["Max"] = c_dtype["Max"]

                if "Min" not in column_dtypes[c_name][c_dtype["dtype"]]:
                    column_dtypes[c_name][c_dtype["dtype"]]["Min"] = c_dtype["Min"]
                elif c_dtype["Min"] is not None:
                    if column_dtypes[c_name][c_dtype["dtype"]]["Min"] is not None:
                        column_dtypes[c_name][c_dtype["dtype"]]["Min"] = min(column_dtypes[c_name][c_dtype["dtype"]]["Min"],  c_dtype["Min"] )
                    else:
                        column_dtypes[c_name][c_dtype["dtype"]]["Min"] = c_dtype["Min"]
            line = fin.readline()
    return column_dtypes



def extract_col_cooccurrence(csv_col_file, filter = None):
    check_filter = filter is not None
    line_counter = 0
    set_of_pairs = set()
    with open(csv_col_file, 'r') as fin:
        sys.stdout.write(f"Reading f{csv_col_file} \n")
        line = fin.readline()
        while line:
            line_counter+=1
            sys.stdout.write(f' \r Line number: {line_counter} ')
            sys.stdout.flush()
            line = line.replace("<class 'str'>", "dtype('str_')")
            line = line.replace("array", "np.array")
            line = re.sub(r"(nan|masked|'na')","None", line)
            dtype_dict = eval(line, {"dtype": dtype, "np": np})
            if check_filter:
                dtype_dict = { k: v for k, v in dtype_dict.items() if k in filter  }
            lst_of_pairs  = set(itertools.combinations(dtype_dict.keys(), 2))
            set_of_pairs.update(lst_of_pairs)

            #necessary
            line = fin.readline()

    return set_of_pairs

if __name__ == '__main__':
    csv_nc_col_file = "log_csv_nc_28-08-2024~11:52AM.txt"
    column_dtypes = {}

    column_dtypes = observe_column_datatypes(csv_nc_col_file, column_dtypes=column_dtypes)
    for c_name, c_type_set in column_dtypes.items():
        if c_type_set.__len__() > 1:
            print(f" ===> {c_name} : {c_type_set}")
        else:
            print(f"{c_name} : {c_type_set} ")

    pickle.dump(column_dtypes, open("./columns_min_max.pkl",'wb'))
    # column_dtypes = pickle.load(open("./columns_min_max.pkl",'rb'))

    # filtered = [c_name for c_name, c_type_set in column_dtypes.items() if c_type_set.__len__() > 0 ]
    # set_of_pairs_csv = extract_col_cooccurrence(csv_nc_col_file, filter=column_dtypes)
    # relation_filepath = './csv_nc_col_relation.pkl'
    # with open(relation_filepath,'wb') as fout:
    #     pickle.dump(set_of_pairs_csv, fout)
    # print(f"Generated: f{relation_filepath}")

