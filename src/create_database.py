"""
Create a sqlite database with the census 2020 data
from the legacy file format found at
https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/

First, creates the database, then creates the geo table and then the P1 table.
"""
import sqlite3
from sqlite3 import Error

census_file = '../data/census_2020.db'


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def create_table(census_db, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = census_db.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_geo_table(census_db):
    """ creates the geo table - could script this from the XLSX spreadsheet, but
    hard coding for now"""

    create_geo_table_sql = """ CREATE TABLE IF NOT EXISTS geo_heading (
        FILEID text,
        STUSAB text,
        SUMLEV text,
        GEOVAR text,
        GEOCOMP text,
        CHARITER text,
        CIFSN text,
        LOGRECNO integer,
        GEOCODE text,
        REGION text,
        DIVISION text,
        STATE text,
        STATENS text,
        COUNTY text,
        COUNTYCC text,
        COUNTYNS text,
        COUSUB text,
        GEOID text,
        COUSUBCC text,
        COUSUBNS text,
        SUBMCD text,
        SUBMCDCC text,
        SUBMCDNS text,
        ESTATE text,
        ESTATECC text,
        ESTATENS text,
        CONCIT text,
        CONCITCC text,
        CONCITNS text,
        PLACE text,
        PLACECC text,
        PLACENS text,
        TRACT text,
        BLKGRP text,
        BLOCK text,
        AIANHH text,
        AIHHTLI text,
        AIANHHFP text,
        AIANHHCC text,
        AIANHHNS text,
        AITS text,
        AITSFP text,
        AITSCC text,
        AITSNS text,
        TTRACT text,
        TBLKGRP text,
        ANRC text,
        ANRCCC text,
        ANRCNS text,
        CBSA text,
        MEMI text,
        CSA text,
        METDIV text,
        NECTA text,
        NMEMI text,
        CNECTA text,
        NECTADIV text,
        CBSAPCI text,
        NECTAPCI text,
        UA text,
        UATYPE text,
        UR text,
        CD116 text,
        CD118 text,
        CD119 text,
        CD120 text,
        CD121 text,
        SLDU18 text,
        SLDU22 text,
        SLDU24 text,
        SLDU26 text,
        SLDU28 text,
        SLDL18 text,
        SLDL22 text,
        SLDL24 text,
        SLDL26 text,
        SLDL28 text,
        VTD text,
        VTDI text,
        ZCTA text,
        SDELM text,
        SDSEC text,
        SDUNI text,
        PUMA text,
        AREALAND integer,
        AREAWATR integer,
        BASENAME text,
        NAME text,
        FUNCSTAT text,
        GCUNI text,
        POP100 integer,
        HU100 integer,
        INTPTLAT text,
        INTPTLON text,
        LSADC text,
        PARTFLAG text,
        UGA text
    );"""

    create_table(census_db, create_geo_table_sql)


def create_p1_table(census_db):
    """ creates the geo table - could script this from the XLSX spreadsheet, but
    hard coding for now"""

    create_geo_table_sql = """ CREATE TABLE IF NOT EXISTS p1 (
        FILEID text,
        STUSAB text,
        CHARITER text,
        CIFSN text,
        LOGRECNO integer,
        P0010001 integer,
        P0010002 integer,
        P0010003 integer,
        P0010004 integer,
        P0010005 integer,
        P0010006 integer,
        P0010007 integer,
        P0010008 integer,
        P0010009 integer,
        P0010010 integer,
        P0010011 integer,
        P0010012 integer,
        P0010013 integer,
        P0010014 integer,
        P0010015 integer,
        P0010016 integer,
        P0010017 integer,
        P0010018 integer,
        P0010019 integer,
        P0010020 integer,
        P0010021 integer,
        P0010022 integer,
        P0010023 integer,
        P0010024 integer,
        P0010025 integer,
        P0010026 integer,
        P0010027 integer,
        P0010028 integer,
        P0010029 integer,
        P0010030 integer,
        P0010031 integer,
        P0010032 integer,
        P0010033 integer,
        P0010034 integer,
        P0010035 integer,
        P0010036 integer,
        P0010037 integer,
        P0010038 integer,
        P0010039 integer,
        P0010040 integer,
        P0010041 integer,
        P0010042 integer,
        P0010043 integer,
        P0010044 integer,
        P0010045 integer,
        P0010046 integer,
        P0010047 integer,
        P0010048 integer,
        P0010049 integer,
        P0010050 integer,
        P0010051 integer,
        P0010052 integer,
        P0010053 integer,
        P0010054 integer,
        P0010055 integer,
        P0010056 integer,
        P0010057 integer,
        P0010058 integer,
        P0010059 integer,
        P0010060 integer,
        P0010061 integer,
        P0010062 integer,
        P0010063 integer,
        P0010064 integer,
        P0010065 integer,
        P0010066 integer,
        P0010067 integer,
        P0010068 integer,
        P0010069 integer,
        P0010070 integer,
        P0010071 integer,
        P0020001 integer,
        P0020002 integer,
        P0020003 integer,
        P0020004 integer,
        P0020005 integer,
        P0020006 integer,
        P0020007 integer,
        P0020008 integer,
        P0020009 integer,
        P0020010 integer,
        P0020011 integer,
        P0020012 integer,
        P0020013 integer,
        P0020014 integer,
        P0020015 integer,
        P0020016 integer,
        P0020017 integer,
        P0020018 integer,
        P0020019 integer,
        P0020020 integer,
        P0020021 integer,
        P0020022 integer,
        P0020023 integer,
        P0020024 integer,
        P0020025 integer,
        P0020026 integer,
        P0020027 integer,
        P0020028 integer,
        P0020029 integer,
        P0020030 integer,
        P0020031 integer,
        P0020032 integer,
        P0020033 integer,
        P0020034 integer,
        P0020035 integer,
        P0020036 integer,
        P0020037 integer,
        P0020038 integer,
        P0020039 integer,
        P0020040 integer,
        P0020041 integer,
        P0020042 integer,
        P0020043 integer,
        P0020044 integer,
        P0020045 integer,
        P0020046 integer,
        P0020047 integer,
        P0020048 integer,
        P0020049 integer,
        P0020050 integer,
        P0020051 integer,
        P0020052 integer,
        P0020053 integer,
        P0020054 integer,
        P0020055 integer,
        P0020056 integer,
        P0020057 integer,
        P0020058 integer,
        P0020059 integer,
        P0020060 integer,
        P0020061 integer,
        P0020062 integer,
        P0020063 integer,
        P0020064 integer,
        P0020065 integer,
        P0020066 integer,
        P0020067 integer,
        P0020068 integer,
        P0020069 integer,
        P0020070 integer,
        P0020071 integer,
        P0020072 integer,
        P0020073 integer
    );"""

    create_table(census_db, create_geo_table_sql)

if __name__ == '__main__':
    census_db = create_connection(census_file)
    create_geo_table(census_db)
    create_p1_table(census_db)
    if census_db:
        census_db.close()


