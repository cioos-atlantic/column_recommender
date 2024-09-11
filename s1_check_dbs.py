import os
import pickle
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from netCDF4 import Dataset
import numpy as np
import time
import random
p = 1 # 1% of the lines

def get_csv_variable_names(file_path, random_percentage = p):
    df = pd.read_csv(file_path, encoding="ISO-8859-1",  skiprows=lambda i: i>0 and random.random() > random_percentage)
    # df = pd.read_csv(file_path, encoding="ISO-8859-1")

    # variable_names_set.update( set(df.columns))
    df_columns = {}
    if len(df.columns) != len(df.dtypes):
        print("Conditions false of dtype, change loop")
        exit(-1)
    for col_nam, dtyp in df.dtypes.items():
        try:
            df_columns[col_nam] = {'dtype': dtyp, 'Min': df[col_nam].min(), 'Max': df[col_nam].max()}
        except TypeError as e:
            df_columns[col_nam] = {'dtype': dtyp, 'Min': None, 'Max': None}
    return df_columns

def get_nc_variable_names(file_path, random_percentage=p):
    try:
        # Open the NetCDF file
        dataset = Dataset(file_path, 'r')

        # Get variable names
        # variable_names = set(dataset.variables.keys())
        results = {}
        for var_name, variable in dataset.variables.items():
            if len(variable.shape) > 0:  # Ensure the variable has data
                # Get random indices based on the size of the first dimension
                num_records = variable.shape[0]
                num_samples = max(1, int(num_records * random_percentage))
                random_indices = random.sample(range(variable.shape[0]), min(num_samples, variable.shape[0]))

                # Extract data for random indices
                data_samples = variable[random_indices]

                # Compute min and max
                min_value = np.min(data_samples)
                max_value = np.max(data_samples)

                # Store results
                results[var_name] = {'dtype': variable.dtype, 'Min': min_value, 'Max': max_value}
            else:
                results[var_name] = {'dtype': variable.dtype, 'Min': None, 'Max': None}
        # Close the NetCDF file
        dataset.close()

        return results
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return set([])

def update_colname_linked_files(colname_linked_files, df_columns, associated_filename):
    for cname in df_columns.keys():
        if cname not in colname_linked_files:
            colname_linked_files[cname] = [associated_filename]
        else:
            colname_linked_files[cname].append(associated_filename)

    return colname_linked_files

if __name__ == '__main__':
    colname_linked_files = {}
    log_file = f"log_csv_nc_{time.strftime('%d-%m-%Y~%l:%M%p')}.txt"
    data_path = "/srv/data/erddap/"
    dictionary_ = {}

    lst_ = []
    for (dirpath, dirnames, filenames) in os.walk(data_path):
        for fname in filenames:
            fpath = os.path.join(dirpath, fname)
            lst_.append(fpath)
    print(f"All Files: {lst_.__len__()}")
    log_file_out_ = open(log_file,'w')

    for fpath in tqdm(lst_):
            file_extension  = Path(fpath).suffix
            if file_extension not in dictionary_:
                dictionary_[file_extension]  = 1
            else:
                dictionary_[file_extension] = dictionary_[file_extension] + 1

            if file_extension == ".csv":
                try:
                    df_columns = get_csv_variable_names(fpath, p)
                    log_file_out_.write(str(df_columns) + "\n")
                    colname_linked_files = update_colname_linked_files(colname_linked_files, df_columns, fpath)
                except Exception as e:
                    print(f"Error reading file: {fpath} : {e}")
            if file_extension == ".nc":
                try:
                    df_columns = get_nc_variable_names(fpath, random_percentage=p)
                    if df_columns.__len__() > 0:
                        log_file_out_.write(str(df_columns)+"\n")
                        colname_linked_files = update_colname_linked_files(colname_linked_files, df_columns, fpath)
                except Exception as e:
                    print(f"Error reading {fpath}: {e}")

    pickle.dump(colname_linked_files, open("./columns_linked_file.pkl", 'wb'))
    print(dictionary_)
