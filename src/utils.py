import json
import os
import csv
import dask.dataframe as dd

def read_json_files_from_directory(directory):
    data = []
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, 'r') as f:
                    # Assuming each JSON file contains an array of records
                    file_data = json.load(f)
                    if isinstance(file_data, list):
                        data.extend(file_data)
                    else:
                        data.append(file_data)
            except Exception as e:
                print(f"Error reading {filepath}: {e}")
    return data

def read_csv_files_from_directory(directory):
    pharmacies = []

    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, mode='r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        pharmacies.append(row)
            except Exception as e:
                print(f"Error reading {filepath}: {e}")
    
    return pharmacies



def read_csv_files_with_dask(base_directories):
    # Glob pattern for all CSV files in the given directories
    csv_pattern = [os.path.join(directory, '*.csv') for directory in base_directories]

    dask_dataframe = dd.read_csv(csv_pattern,dtype={'npi': 'str', 'ndc': 'str'})

    return dask_dataframe.repartition(npartitions=20)

def read_json_files_with_dask(base_directories):
    # Glob pattern for all Json files in the given directories
    json_pattern = [os.path.join(directory, '*.json') for directory in base_directories]

    dask_dataframe = dd.read_json(json_pattern,lines=False,dtype={'npi': 'str', 'ndc': 'str'})

    return dask_dataframe.repartition(npartitions=10)