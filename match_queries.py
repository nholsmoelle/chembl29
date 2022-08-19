#TODO Funktion, mit der match_queries.tsv durchgegangen wird 
# und fuer jede Zeile eine Datei erstellt wird mit den einzelnen match-strings
# match_query_templates_file = "match_queries.tsv"  #input("File containing match query templates")
import csv
import os
import warnings
from os.path import exists
import numpy
import pandas as pd

warnings.simplefilter(action='ignore', category=FutureWarning)


def write_match_queries(match_query_templates_file="match_queries.tsv"):
    with open(match_query_templates_file) as file:
        match_query_templates = csv.reader(file, delimiter="\t")
        for row in match_query_templates:
            path_to_project = '/home/nina/PycharmProjects/chembl29/'
            output_table_path, query_template = [col.strip() for col in row]
            output_table_path = path_to_project + output_table_path
            print(query_template)
            # table = pd.read_csv(output_table_path, delimiter='\t')
            # column_names = table.columns
            split = [s.strip() for s in query_template.split(":")]
            table_a_name, a_key_column = [s.strip() for s in split[1].split("{")]
            table_b_name, b_key_column = [s.strip() for s in split[3].split("{")]
            table_a_path = path_to_project + f'output_files/data_tables/{table_a_name}.tsv'
            table_b_path = path_to_project + f'output_files/data_tables/{table_b_name}.tsv'
            if not exists(table_a_path) or not exists(table_b_path):
                print(f"{table_a_name}.tsv or {table_b_name}.tsv does not exist\n")
                continue
            # table_a = pd.read_csv(table_a_path, delimiter='\t', chunksize=1000)
            # table_b = pd.read_csv(table_b_path, delimiter='\t', chunksize=1000)
            with open(table_a_path) as table_a:
                line_a = table_a.readline()  # Header
                idx_a = [i for i, col in enumerate(line_a.split('\t')) if col.strip() == a_key_column][0]
                while line_a:
                    line_a = table_a.readline()
                    value_a = line_a.split('\t')[idx_a].strip()
                    if value_a in [None, "", "NaN"]:
                        continue
                    print(query_template.replace("value_a", value_a))
                    table_b = pd.read_csv(table_b_path, delimiter='\t', chunksize=1000)
                    # table_b_columns = None
                    table_b_columns = []
                    for chunk in table_b:
                        # if not table_b_columns:
                        #     table_b_columns = chunk.columns
                        if len(table_b_columns) == 0:
                            table_b_columns = chunk.columns
                            os.remove(output_table_path)
                            os.open(output_table_path)
                        b_values = chunk[b_key_column].values
                        if value_a in b_values:
                            print(query_template.replace("value_a", value_a).replace("value_b", value_a))
                            with open("match_queries.cyp") as
                        elif type(b_values[0]) == numpy.int64:
                            # print(type(b_values[0]))
                            print(query_template.replace("value_a", value_a).replace("value_b", str(numpy.int64(float(value_a)))))
                        break
                    print()
                    break





write_match_queries()
