# # This is a sample Python script.
#
# # Press Shift+F10 to execute it or replace it with your code.
# # Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
#
#
# def print_hi(name):
#     # Use a breakpoint in the code line below to debug your script.
#     print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
#
#
# # Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     print_hi('PyCharm')
#
# # See PyCharm help at https://www.jetbrains.com/help/pycharm/
import csv
# import pymysql
import mysql.connector
import pandas as pd


def sql_to_csv(filename, limit=10):
    with open(filename) as tsv_file:
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            tables = row[0].split(", ")
            table_name = tables[0]
            column_names = row[1]
            csv_name = row[2]
            query = f'SELECT {column_names} FROM chembl29.{table_name}'
            # if len(tables) == 1:  # Extracting relevant table data
            #     table_name = tables[0]
            #     column_names = row[1]
            #     query = f'SELECT {column_names} FROM chembl29.{table_name}'
            # elif len(tables) > 10:  # Merging two tables
            #     table_names = row[1]
            #     query = ''
    if limit: query += f" LIMIT {limit}"
    query += ";"
    print(query)
    print(f"CSV name: {csv_name}")
    pd.set_option('display.max_columns', None)
    print(pd.read_sql_query(query, cnx))
    results = pd.read_sql_query(query, cnx)
    results.to_csv(f"output_csv_files/{csv_name}.csv", index=False)


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
cnx = mysql.connector.connect(username=user,
                              password=password,
                              host=host,
                              database=database)
cursor = cnx.cursor()
# with open(filename) as tsv_file:
#     read_tsv = csv.reader(tsv_file, delimiter="\t")
#     for i, row in enumerate(read_tsv):
#         print(i, row)

sql_to_csv(filename)
