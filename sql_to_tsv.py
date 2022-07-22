'''
Extracting all relevant ChEMBL29 data from the mySQL database into TSV files.
These files are then subsequently to be loaded into a Neo4j graph database.
'''

import csv
import mysql.connector as con
import pandas as pd
import warnings
import sys

from typing import List

warnings.filterwarnings('ignore', category=UserWarning)
pd.set_option('display.max_columns', None)


def fetch_table_names(filename):
    '''
    Fetching table information from a predefined file
    :param filename: name of the predefined file
    :return:
    '''
    with open(filename) as tsv_file:
        tables = []
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            try:
                tables.append(row[0])
                # print(row[0])
            except:
                pass

    return tables


def get_primary_keys(tables):
    pks = {}
    for table in tables:
        query_primary_key = f"SHOW keys FROM {table} WHERE Key_name = 'PRIMARY'"
        primary_key = pd.read_sql_query(query_primary_key, cnx)['Column_name'].tolist()[0]
        pks[table] = primary_key

    return pks


def fetch_references(table_name=None):
    if not table_name:
        cursor.execute(f"SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME "
                       f"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                       f"WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL;")
        data = cursor.fetchall()
    else:
        cursor.execute(f"SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME "
                       f"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                       f"WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL AND TABLE_NAME = '{table_name}';")
        data = cursor.fetchall()

    return data


def write_cypher_query(querytype, file_loc, parameters: List[str]):
    if querytype == "node" and len(parameters) > 1:
        node_label = parameters.pop(0)
        query = f":auto USING PERIODIC COMMIT " \
                f"LOAD CSV WITH HEADERS from 'file:///home/nina/PycharmProjects/chembl29/{file_loc}' AS line " \
                f"FIELDTERMINATOR '\\t' " \
                f"CREATE (:{node_label} {{"
        while len(parameters) > 1:
            param = parameters.pop()
            query += f"{param}:line.{param}, "
        param = parameters.pop()
        query += f"{param}:line.{param}}});\n"
        with open("create_queries.cyp", "a") as cypher_file:
            cypher_file.write(query)

    if querytype == "edge":
        table_a = parameters.pop(0)
        table_b = parameters.pop(0)
        a_key_column = parameters.pop(0)
        b_key_column = parameters.pop(0)
        query = f"MATCH (a:{table_a}), (b:{table_b}) " \
                f"WHERE a.{a_key_column} = b.{b_key_column} " \
                f"CREATE (a)-[:OF]->(b);\n"
        with open("match_queries.cyp", "a") as cypher_file:
            cypher_file.write(query)


def create_tsv(query, table_name):
    if query:
        new_file = f"output_files/data_tables/{table_name}.tsv"  # Define file name/location
        results = pd.read_sql_query(query, cnx)  # Query the data into a dataframe
        # results = pd.read_sql_query(query, cnx, chunksize=10000)  # Query the data into a dataframe
        # for chunk in results:
        for col, dt in results.dtypes.items():
            if str(dt) == "object":
                for idx in results.index:
                    if results[col][idx] \
                            and (str(results[col][idx]) not in ["None", "nan"]) \
                            and ("\"" in results[col][idx]):
                        results[col][idx] = results[col][idx].replace("\"", "\'\'")
                        print("\t"*5, results[col][idx])
        results.to_csv(new_file,
                       index=False,
                       sep='\t',
                       chunksize=100000)  # Write data into a TSV file
        parameters = [table_name] + results.columns.tolist()  # Extract column names as cypher query parameters
        write_cypher_query(querytype="node",
                           file_loc=new_file,
                           parameters=parameters)  # Use data for preparing cypher query to create nodes
        return results


def sql_to_tsv(filename, limit=None):
    COLUMN_NAME = 1
    CONSTRAINT_NAME = 2
    REFERENCED_TABLE_NAME = 3
    REFERENCED_COLUMN_NAME = 4
    tables = fetch_table_names(filename)
    for i, table in enumerate(tables):
        query = f"SELECT * FROM chembl29.{table}"
        if limit:
            query += f" LIMIT {limit}"
        query += ";"
        data = create_tsv(table_name=table, query=query)
        refs = fetch_references(table_name=table)
        for j, ref in enumerate(refs):
            column = ref[COLUMN_NAME]
            constraint = ref[CONSTRAINT_NAME]
            ref_table = ref[REFERENCED_TABLE_NAME]
            ref_column = ref[REFERENCED_COLUMN_NAME]
            new_filename = f"{table}-{column}---edge---{ref_table}-{ref_column}"
            new_file = f"output_files/edge_tables/{new_filename}.tsv"
            pk = get_primary_keys([table])[table]
            data[[str(pk), str(column)]].to_csv(new_file,
                                                index=False,
                                                sep='\t',
                                                chunksize=100000)
            parameters = [table, ref_table, column, ref_column]
            write_cypher_query(querytype="edge",
                               file_loc=new_file,
                               parameters=parameters)
            print(f"{column} -> {ref_table} ({j+1}/{len(refs)}) edge table created for node table {table} ({i+1}/{len(tables)})")



user = "pycharm"  #TODO user input
password = "PyCharm_P455w0r7"  #TODO user input
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

### Cleaning up the cypher query file
with open("create_queries.cyp", "w") as file:
    file.write("")
with open("match_queries.cyp", "w") as file:
    file.write("")


sql_to_tsv(filename, limit=None)


