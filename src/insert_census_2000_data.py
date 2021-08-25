"""
Inserts the geo heading and p1 data from the legacy file format
into the census sqlite database created by create_database.
"""
import os
import sqlite3
from sqlite3 import Error

user_home_dir = os.path.expanduser('~')
census_file = '../data/census_2020.db'
geo_heading_table = 'geo_heading_2000'
p1_table = 'sf1_2000'
geo_heading_file = user_home_dir + '/Development/Data/Census/ca2000.sf1/cageo2000.uf1'
sf1_file = user_home_dir + '/Development/Data/Census/ca2000.sf1/ca000012000.uf1'


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


def get_fields_holder(fields):
    """ create a paramaterized query string holder"""
    fields_holder = ',?' * fields
    return fields_holder[1:]


def extract_field_values_for_geo(row):

    fileid = row[0:6]
    stusab = row[6:8]
    sumlev = row[8:11]
    geocomp = row[11:13]
    chariter = row[13:16]
    cifsn = row[16:18]
    logrecno = row[18:25]
    region = row[25:26]
    division = row[26:27]
    statece = row[27:29]
    state = row[29:31]
    county = row[31:34]
    countysc = row[34:36]
    cousub = row[36:41]
    cousubcc = row[41:43]
    cousubsc = row[43:45]
    place = row[45:50]
    placecc = row[50:52]
    placedc = row[52:53]
    placesc = row[53:55]
    tract = row[55:61]
    blkgrp = row[61:62]
    block = row[62:66]
    iuc = row[66:68]
    concit = row[68:73]
    concitcc = row[73:75]
    concitsc = row[75:77]
    aianhh = row[77:81]
    aianhhfp = row[81:86]
    aianhhcc = row[86:88]
    aihhtli = row[88:89]
    aitsce = row[89:92]
    aits = row[92:97]
    aitscc = row[97:99]
    anrc = row[99:104]
    anrccc = row[104:106]
    msacmsa = row[106:110]
    masc = row[110:112]
    cmsa = row[112:114]
    macci = row[114:115]
    pmsa = row[115:119]
    necma = row[119:123]
    necmacci = row[123:124]
    necmasc = row[124:126]
    exi = row[126:127]
    ua = row[127:132]
    uasc = row[132:134]
    uatype = row[134:135]
    ur = row[135:136]
    cd106 = row[136:138]
    cd108 = row[138:140]
    cd109 = row[140:142]
    cd110 = row[142:144]
    sldu = row[144:147]
    sldl = row[147:150]
    vtd = row[150:156]
    vtdi = row[156:157]
    zcta3 = row[157:160]
    zcta5 = row[160:165]
    submcd = row[165:170]
    submcdcc = row[170:172]
    arealand = row[172:186]
    areawatr = row[186:200]
    name = row[200:290]
    funcstat = row[290:291]
    gcuni = row[291:292]
    pop100 = row[292:301]
    hu100 = row[301:310]
    intptlat = row[310:319]
    intptlon = row[319:329]
    lsadc = row[329:331]
    partflag = row[331:332]
    sdelm = row[332:337]
    sdsec = row[337:342]
    sduni = row[342:347]
    taz = row[347:353]
    uga = row[353:358]
    puma5 = row[358:363]
    puma1 = row[363:368]
    reserve2 = row[368:383]
    macc = row[383:388]
    uacp = row[388:393]
    reserved = row[393:400]

    return fileid, stusab, sumlev, geocomp, chariter, cifsn, logrecno, region, \
           division, statece, state, county, countysc, cousub, cousubcc, cousubsc, \
           place, placecc, placedc, placesc, tract, blkgrp, block, iuc, concit, \
           concitcc, concitsc, aianhh, aianhhfp, aianhhcc, aihhtli, aitsce, aits, \
           aitscc, anrc, anrccc, msacmsa, masc, cmsa, macci, pmsa, necma, necmacci, \
           necmasc, exi, ua, uasc, uatype, ur, cd106, cd108, cd109, cd110, sldu, sldl, \
           vtd, vtdi, zcta3, zcta5, submcd, submcdcc, arealand, areawatr, name, funcstat, \
           gcuni, pop100, hu100, intptlat, intptlon, lsadc, partflag, sdelm, sdsec, sduni, \
           taz, uga, puma5, puma1, reserve2, macc, uacp, reserved


def populate_table_geo_headings(census_db, table_name, source_file):
    """Populate the named table from the source file
    The geo_heading source file format is fixed width.
    """

    src_file = open(source_file, 'r', encoding='cp1252')

    field_holders = get_fields_holder(83)

    log_rec_nos = []

    sql_statement = 'INSERT INTO ' + table_name + ' VALUES (' + field_holders + ');'
    sql_values = []
    record_count = 0
    for record in src_file:
        record_count += 1
        # Only inserting records from LA County
        if record[31:34] == '037':

            fileid, stusab, sumlev, geocomp, chariter, cifsn, logrecno, region, \
            division, statece, state, county, countysc, cousub, cousubcc, cousubsc, \
            place, placecc, placedc, placesc, tract, blkgrp, block, iuc, concit, \
            concitcc, concitsc, aianhh, aianhhfp, aianhhcc, aihhtli, aitsce, aits, \
            aitscc, anrc, anrccc, msacmsa, masc, cmsa, macci, pmsa, necma, necmacci, \
            necmasc, exi, ua, uasc, uatype, ur, cd106, cd108, cd109, cd110, sldu, sldl, \
            vtd, vtdi, zcta3, zcta5, submcd, submcdcc, arealand, areawatr, name, funcstat, \
            gcuni, pop100, hu100, intptlat, intptlon, lsadc, partflag, sdelm, sdsec, sduni, \
            taz, uga, puma5, puma1, reserve2, macc, uacp, reserved = \
                extract_field_values_for_geo(record)

            sql_values.append([fileid, stusab, sumlev, geocomp, chariter, cifsn, logrecno, region, \
                division, statece, state, county, countysc, cousub, cousubcc, cousubsc, \
                place, placecc, placedc, placesc, tract, blkgrp, block, iuc, concit, \
                concitcc, concitsc, aianhh, aianhhfp, aianhhcc, aihhtli, aitsce, aits, \
                aitscc, anrc, anrccc, msacmsa, masc, cmsa, macci, pmsa, necma, necmacci, \
                necmasc, exi, ua, uasc, uatype, ur, cd106, cd108, cd109, cd110, sldu, sldl, \
                vtd, vtdi, zcta3, zcta5, submcd, submcdcc, arealand, areawatr, name, funcstat, \
                gcuni, pop100, hu100, intptlat, intptlon, lsadc, partflag, sdelm, sdsec, sduni, \
                taz, uga, puma5, puma1, reserve2, macc, uacp, reserved])

            log_rec_nos.append(logrecno)

            if record_count % 1000 == 0:
                print("Populating {} with {} records".format(str(record_count), str(len(sql_values))))
                # execute_many_sql(census_db, sql_statement, sql_values)
                sql_values = []

            # if record_count % 10000 == 0:
            #     census_db.commit()

    # One last execute to catch the remaining records
    # execute_many_sql(census_db, sql_statement, sql_values)
    # census_db.commit()
    #
    return log_rec_nos


def populate_table_sf1(census_db, table_name, source_file, log_rec_nos):
    """Populate the sf1 table from the source file"""

    src_file = open(source_file, 'r', encoding='cp1252')

    field_holders = get_fields_holder(227)

    sql_statement = 'INSERT INTO ' + table_name + ' VALUES (' + field_holders + ');'
    sql_values = []
    record_count = 0
    for record in src_file:
        record_count += 1
        values = record.split(',')
        # Only inserting records from LA County - based on the geo_headings data
        # print('  = ' + values[4])
        if values[4] in log_rec_nos:
            sql_values.append(values)
            if record_count % 1000 == 0:
                print("Populating {} with {} records".format(str(record_count), str(len(sql_values))))
                execute_many_sql(census_db, sql_statement, sql_values)
                sql_values = []

            if record_count % 10000 == 0:
                census_db.commit()

    # One last execute to catch the remaining records
    execute_many_sql (census_db, sql_statement, sql_values)
    census_db.commit()


if __name__ == '__main__':
    census_db = create_connection(census_file)
    log_rec_nos = populate_table_geo_headings(census_db, geo_heading_table, geo_heading_file)
    populate_table_sf1(census_db, p1_table, sf1_file, log_rec_nos)
    if census_db:
        census_db.close()