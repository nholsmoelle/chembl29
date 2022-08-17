'''
Extracting all relevant ChEMBL29 data from the mySQL database into TSV files.
These files are then subsequently to be loaded into a Neo4j graph database.
'''

import csv
import os

import mysql.connector as con
import numpy
import pandas as pd
import warnings
import sys
from os.path import exists
from typing import List

### Optional:
warnings.filterwarnings('ignore', category=UserWarning)
pd.set_option('display.max_columns', None)


def check_size(table_name):
    query = f"SELECT COUNT(*) from {table_name}"
    cnt = pd.read_sql_query(query, cnx)
    n_rows = int(cnt[cnt.columns[0]][0])
    cursor.reset(free=True)
    return n_rows


def fetch_tables(filename):
    with open(filename) as tsv_file:
        tables = {}
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            try:
                additional_command = ""
                if len(row) > 1:
                    additional_command = row[1]
                tables[row[0]] = additional_command
            except:
                print(f"ERROR: Could not fetch tables from {filename}")
    return tables


def get_primary_key(table):
    query_primary_key = f"SHOW keys FROM {table} WHERE Key_name = 'PRIMARY'"
    primary_key = pd.read_sql_query(query_primary_key, cnx)['Column_name'].tolist()[0]
    cursor.reset(free=True)
    return primary_key


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
        # query = f":auto USING PERIODIC COMMIT 1000 " \
        # query = f":auto USING PERIODIC COMMIT " \
        query = f"LOAD CSV WITH HEADERS from 'file://{path_to_project}/{file_loc}' AS line " \
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


def highlighted(text, random=False, background_color='\033[47m', text_color='\033[30m'):
    if random:
        background_color = f'\033[{numpy.random.randint(40, 48)}m'
    return background_color + text + text_color


def replace_double_quotes(df: pd.DataFrame, column):
    for idx in df.index:
        if df[column][idx] \
                and (str(df[column][idx]) not in ["None", "nan"]) \
                and ("\"" in df[column][idx]):
            df[column][idx] = df[column][idx].replace("\"", "\'\'")
            # df.iloc[column, idx] = df[column][idx].replace("\"", "\'\'")
    return df


def create_tsv(table_name, query, chunk_size=None):
    cyp_node_parameters = None
    cyp_edge_parameters = None
    data_file = f"output_files/data_tables/{table_name}.tsv"  # Define data file name/location
    edge_file = None
    column, constraint, ref_table, ref_column = None, None, None, None
    pk = get_primary_key(table_name)
    refs = fetch_references(table_name=table_name)
    if "LIMIT" in query:
        table_size = int(query.split("LIMIT")[-1].strip(";").strip())
    else:
        table_size = check_size(table_name)
    # n_chunks = int(table_size / chunk_size) + 1
    n_chunks = int(numpy.ceil(table_size / chunk_size))
    if exists(data_file):
        os.remove(data_file)
    result = pd.read_sql_query(query, cnx, chunksize=chunk_size)  # Query the data into a dataframe
    i = 0
    for chunk in result:
        i += 1
        sys.stdout.write("\r" + "\t"*5 + f" Chunk {i}/{n_chunks}")
        for col, dt in chunk.dtypes.items():  # Replacing double quotes in every object-type column
            if str(dt) == "object":
                chunk = replace_double_quotes(chunk, col)
        if not exists(data_file):  # Write first data chunk into a TSV file
            cyp_node_parameters = [table_name] + chunk.columns.tolist()
            chunk.to_csv(data_file, index=False, sep='\t', chunksize=100000, mode='w')
            # print("\t" * 5, f"Created {data_file}")
            write_cypher_query(querytype="node", file_loc=data_file, parameters=cyp_node_parameters)
            for ref in refs:
                column = ref[1]  # Foreign key (constraint) column name
                constraint = ref[2]  # Foreign key (constraint) name
                ref_table = ref[3]  # Name of foreign key's referenced (target) table
                ref_column = ref[4]  # Name of referenced/key column in the target table
                edge_file_name = f"{table_name}-{column}---edge---{ref_table}-{ref_column}"
                edge_file = f"output_files/edge_tables/{edge_file_name}.tsv"
                if exists(edge_file):
                    os.remove(edge_file)
                cyp_edge_parameters = [table_name, ref_table, column, ref_column]
                chunk[[pk, column]].to_csv(edge_file, index=False, sep='\t', chunksize=100000, mode='w')
                # print("\t" * 5, f"Created {edge_file}")
                write_cypher_query(querytype="edge", file_loc=edge_file, parameters=cyp_edge_parameters)
        else:  # Append data chunk to the TSV file
            chunk.to_csv(data_file, index=False, sep='\t', chunksize=100000, mode='a', header=False)
            # print("\t" * 5, f"Added data to {data_file}")
            for ref in refs:
                chunk[[pk, column]].to_csv(edge_file, index=False, sep='\t', chunksize=100000, mode='a')
                # print("\t" * 5, f"Added data to {edge_file}")
    cursor.reset(free=True)


def sql_to_tsv(filename, limit=None):
    tables = fetch_tables(filename)
    chunk_size = 100000
    for table in tables.items():
        msg = f"\nTable: {table[0]}\n"
        sys.stdout.write(msg)
        generate(table_data=table, chunk_size=chunk_size, limit=limit)


def generate(table_data, chunk_size=None, limit=None):
    table = table_data[0]
    query = f"SELECT * FROM chembl29.{table}"
    query += " " + table_data[1]
    query = query.strip()
    if limit:
        query += f" LIMIT {limit}"
    query += ";"
    create_tsv(table_name=table,
               query=query,
               chunk_size=chunk_size)


path_to_project = '/home/nina/PycharmProjects/chembl29' #input("Absolute path to project: ")
user = "pycharm"  #input("User name: ")
password = "PyCharm_P455w0r7"  #input("User password: ")
# server = "Local instance 3306"  # specify to run this script
# port = 3306  # specify to run this script
host = "localhost"
database = "chembl29"  # specify to run this script
filename = "tables_of_interest.tsv"  # specify to run this script
cnx = con.connect(username=user,
                              password=password,
                              host=host,
                              database=database)
cursor = cnx.cursor(buffered=True)

### Cleaning up the cypher query files
if exists("create_queries.cyp"):
    with open("create_queries.cyp", "w") as file:
        file.write("")
if exists("match_queries.cyp"):
    with open("match_queries.cyp", "w") as file:
        file.write("")


sql_to_tsv(filename, limit=None)
cnx.close()

