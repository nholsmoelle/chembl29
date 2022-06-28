'''
Extracting all relevant ChEMBL29 data from the mySQL database into TSV files.
These files are then subsequently to be loaded into a Neo4j graph database.
'''

import csv
import mysql.connector as con
import pandas as pd
import warnings

warnings.filterwarnings('ignore', category=UserWarning)
pd.set_option('display.max_columns', None)


def get_keys(table_name):
    query_primary_key = f"SHOW keys FROM {table_name} WHERE Key_name = 'PRIMARY'"
    query_foreign_keys = f"SHOW keys FROM {table_name} WHERE Key_name LIKE 'fk_%'"
    query_unique_keys = f"SHOW keys FROM {table_name} WHERE Key_name LIKE 'uk_%'"
    query_keys_of_interest = f"SHOW keys FROM {table_name} WHERE Seq_in_index = 1;"
    pk = pd.read_sql_query(query_primary_key, cnx)['Column_name'].tolist()[0]
    fks = pd.read_sql_query(query_foreign_keys, cnx)['Column_name'].tolist()
    uks = pd.read_sql_query(query_unique_keys, cnx)['Column_name'].tolist()
    koi = pd.read_sql_query(query_keys_of_interest, cnx)['Column_name'].tolist()
    koi.remove(pk)

    return pk, fks, uks, koi


def create_tsv(query, new_filename):
    pk, fks, uks, koi = get_keys(new_filename)
    print(f"Table: {new_filename}\n"
          f"PK: [{pk}]\n"
          f"FKs: {fks}\n"
          f"UKs: {uks}\n"
          f"KoI: {koi}\n")
    results = pd.read_sql_query(query, cnx)
    results.to_csv(f"output_files/data_tables/{new_filename}.tsv", index=False, sep='\t')
    # for fk in fks:
    #     edge_table_name = f"{pk}-X-{fk}"
    #     results[[str(pk), str(fk)]].to_csv(f"output_files/edge_tables/{edge_table_name}.tsv", index=False, sep='\t')
    # for uk in uks:
    #     edge_table_name = f"{pk}-X-{uk}"
    #     results[[str(pk), str(uk)]].to_csv(f"output_files/edge_tables/{edge_table_name}.tsv", index=False, sep='\t')
    for k in koi:
        edge_table_name = f"{pk}-X-{k}"
        results[[str(pk), str(k)]].to_csv(f"output_files/edge_tables/{edge_table_name}.tsv", index=False, sep='\t')


def sql_to_tsv(filename, limit=None):
    with open(filename) as tsv_file:
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            tables = row[0].split(", ")
            table_name = tables[0]
            query = f'SELECT * FROM chembl29.{table_name}'
            # if len(row) > 1:
            #     column_names = row[1]
            #     query = f'SELECT {column_names} FROM chembl29.{table_name}'
            if limit:
                query += f" LIMIT {limit}"
            query += ";"
            create_tsv(query, table_name)



user = "pycharm"  #todo user input
password = "PyCharm_P455w0r7"  #todo user input
# user = input("user name: ")
# password = input("user password: ")
# server = "Local instance 3306"  # specify to run this script
# port = 3306  # specify to run this script
host = "localhost"
database = "chembl29"  # specify to run this script
filename = "tables_of_interest.tsv"  # specify to run this script
# tsv_file = open(filename)
cnx = con.connect(username=user,
                              password=password,
                              host=host,
                              database=database)
cursor = cnx.cursor()
# with open(filename) as tsv_file:
#     read_tsv = csv.reader(tsv_file, delimiter="\t")
#     for i, row in enumerate(read_tsv):
#         print(i, row)

sql_to_tsv(filename)
# sql_to_tsv(filename, limit=10)
