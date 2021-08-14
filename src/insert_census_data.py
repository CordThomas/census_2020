"""
Inserts the geo heading and p1 data from the legacy file format
into the census sqlite database created by create_database.
"""
import os
import sqlite3
from sqlite3 import Error

census_file = '../data/census_2020.db'
geo_heading_table = 'geo_heading'
p1_table = 'p1'
geo_heading_file = '../data/ca2020.pl/cageo2020.pl'
p1_file = '../data/ca2020.pl/ca000012020.pl'


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def execute_many_sql(census_db, sql_statement, sql_field_values):
    """ create a table from the create_table_sql statement
    :param census_db: Connection object
    :param sql_statement: the insert statement string
    :param sql_field_values: the array of insert values
    :return:
    """
    try:
        c = census_db.cursor()
        # print (sql_statement)
        # print(sql_field_values)
        c.executemany(sql_statement, sql_field_values)
    except Error as e:
        print(e)


def execute_sql(census_db, sql_statement):
    """ create a table from the create_table_sql statement
    :param census_db: Connection object
    :param sql_statement: the insert statement string
    :param sql_field_values: the array of insert values
    :return:
    """
    try:
        c = census_db.cursor()
        # print (sql_statement)
        # print(sql_field_values)
        c.execute(sql_statement)
    except Error as e:
        print(e)


def get_fields_holder(src_file):

    first_line = src_file.readline()
    fields = first_line.split('|')
    fields_holder = ',?' * (len(fields))
    return fields_holder[1:]


def populate_table_geo_headings(census_db, table_name, source_file):
    """Populate the named table from the source file"""

    src_file = open(source_file, 'r', encoding='cp1252')

    field_holders = get_fields_holder(src_file)

    src_file.seek(0, os.SEEK_SET)

    log_rec_nos = []

    sql_statement = 'INSERT INTO ' + table_name + ' VALUES (' + field_holders + ');'
    sql_values = []
    record_count = 0
    for record in src_file:
        record_count += 1
        values = record.replace('\r\n', '').split('|')
        # Only inserting records from LA County
        if values[14] == '037':
            sql_values.append(values)
            log_rec_nos.append(values[7])
            if record_count % 1000 == 0:
                print("Populating {} with {} records".format(str(record_count), str(len(sql_values))))
                execute_many_sql(census_db, sql_statement, sql_values)

            if record_count % 10000 == 0:
                census_db.commit()

    # One last execute to catch the remaining records
    execute_many_sql(census_db, sql_statement, sql_values)
    census_db.commit()

    return log_rec_nos


def populate_table_p1(census_db, table_name, source_file, log_rec_nos):
    """Populate the named table from the source file"""

    src_file = open(source_file, 'r', encoding='cp1252')

    field_holders = get_fields_holder(src_file)

    src_file.seek(0, os.SEEK_SET)

    sql_statement = 'INSERT INTO ' + table_name + ' VALUES (' + field_holders + ');'
    sql_values = []
    record_count = 0
    for record in src_file:
        record_count += 1
        values = record.replace('\r\n', '').split('|')
        # Only inserting records from LA County - based on the geo_headings data
        if values[4] in log_rec_nos:
            sql_values.append(values)
            if record_count % 1000 == 0:
                print("Populating {} with {} records".format(str(record_count), str(len(sql_values))))
                execute_many_sql(census_db, sql_statement, sql_values)

            if record_count % 10000 == 0:
                census_db.commit()

    # One last execute to catch the remaining records
    execute_many_sql (census_db, sql_statement, sql_values)
    census_db.commit()


if __name__ == '__main__':
    census_db = create_connection(census_file)
    log_rec_nos = populate_table_geo_headings(census_db, geo_heading_table, geo_heading_file)
    populate_table_p1(census_db, p1_table, p1_file, log_rec_nos)
    if census_db:
        census_db.close()