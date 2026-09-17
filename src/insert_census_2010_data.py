"""
Inserts the geo heading and p1 data from the legacy file format
into the census sqlite database created by create_database.
"""
import os
import sqlite3
from sqlite3 import Error
import pandas as pd

user_home_dir = os.path.expanduser('~')
census_file = '../data/census_2020.db'
geo_heading_table = 'geo_heading_2010'
p1_table = 'sf1_2010'
geo_heading_file = user_home_dir + '/Development/Data/Census/ca2010.sf1/cageo2010.sf1'
sf1_files = [user_home_dir + '/Development/Data/Census/ca2010.sf1/ca000012010.sf1',
             user_home_dir + '/Development/Data/Census/ca2010.sf1/ca000022010.sf1',
             user_home_dir + '/Development/Data/Census/ca2010.sf1/ca000032010.sf1']


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
    state = row[27:29]
    county = row[29:32]
    countycc = row[32:34]
    countysc = row[34:36]
    cousub = row[36:41]
    cousubcc = row[41:43]
    cousubsc = row[43:45]
    place = row[45:50]
    placecc = row[50:52]
    placesc = row[52:54]
    tract = row[54:60]
    blkgrp = row[60:61]
    block = row[61:65]
    iuc = row[65:67]
    concit = row[67:72]
    concitcc = row[72:74]
    concitsc = row[74:76]
    aianhh = row[76:80]
    aianhhfp = row[80:85]
    aianhhcc = row[85:87]
    aihhtli = row[87:88]
    aitsce = row[88:91]
    aits = row[91:96]
    aitscc = row[96:98]
    ttract = row[98:104]
    tblkgrp = row[104:105]
    anrc = row[105:110]
    anrccc = row[110:112]
    cbsa = row[112:117]
    cbsasc = row[117:119]
    metdiv = row[119:124]
    csa = row[124:127]
    necta = row[127:132]
    nectasc = row[132:134]
    nectadiv = row[134:139]
    cnecta = row[139:142]
    cbsapci = row[142:143]
    nectapci = row[143:144]
    ua = row[144:149]
    uasc = row[149:151]
    uatype = row[151:152]
    ur = row[152:153]
    cd = row[153:155]
    sldu = row[155:158]
    sldl = row[158:161]
    vtd = row[161:167]
    vtdi = row[167:168]
    reserve2 = row[168:171]
    zcta5 = row[171:176]
    submcd = row[176:181]
    submcdcc = row[181:183]
    sdelm = row[183:188]
    sdsec = row[188:193]
    sduni = row[193:198]
    arealand = row[198:212]
    areawatr = row[212:226]
    name = row[226:316]
    funcstat = row[316:317]
    gcuni = row[317:318]
    pop100 = row[318:327]
    hu100 = row[327:336]
    intptlat = row[336:347]
    intptlon = row[347:359]
    lsadc = row[359:361]
    partflag = row[361:362]
    reserve3 = row[362:368]
    uga = row[368:373]
    statens = row[373:381]
    countyns = row[381:389]
    cousubns = row[389:397]
    placens = row[397:405]
    concitns = row[405:413]
    aianhhns = row[413:421]
    aitsns = row[421:429]
    anrcns = row[429:437]
    submcdns = row[437:445]
    cd113 = row[445:447]
    cd114 = row[447:449]
    cd115 = row[449:451]
    sldu2 = row[451:454]
    sldu3 = row[454:457]
    sldu4 = row[457:460]
    sldl2 = row[460:463]
    sldl3 = row[463:466]
    sldl4 = row[466:469]
    aianhhsc = row[469:471]
    csasc = row[471:473]
    cnectasc = row[473:475]
    memi = row[475:476]
    nmemi = row[476:477]
    puma5 = row[477:482]
    reserved = row[482:500]

    return fileid, stusab, sumlev, geocomp, chariter, cifsn, logrecno, region, division, state, \
           county, countycc, countysc, cousub, cousubcc, cousubsc, place, placecc, placesc, tract, \
           blkgrp, block, iuc, concit, concitcc, concitsc, aianhh, aianhhfp, aianhhcc, aihhtli, \
           aitsce, aits, aitscc, ttract, tblkgrp, anrc, anrccc, cbsa, cbsasc, metdiv, csa, necta, \
           nectasc, nectadiv, cnecta, cbsapci, nectapci, ua, uasc, uatype, ur, cd, sldu, sldl, vtd, \
           vtdi, reserve2, zcta5, submcd, submcdcc, sdelm, sdsec, sduni, arealand, areawatr, name, \
           funcstat, gcuni, pop100, hu100, intptlat, intptlon, lsadc, partflag, reserve3, uga, statens, \
           countyns, cousubns, placens, concitns, aianhhns, aitsns, anrcns, submcdns, cd113, cd114, \
           cd115, sldu2, sldu3, sldu4, sldl2, sldl3, sldl4, aianhhsc, csasc, cnectasc, memi, nmemi, \
           puma5, reserved


def populate_table_geo_headings(census_db, table_name, source_file):
    """Populate the named table from the source file
    The geo_heading source file format is fixed width.
    """

    src_file = open(source_file, 'r', encoding='cp1252')

    field_holders = get_fields_holder(101)

    log_rec_nos = []

    sql_statement = 'INSERT INTO ' + table_name + ' VALUES (' + field_holders + ');'
    sql_values = []
    record_count = 0
    for record in src_file:
        record_count += 1
        # Only inserting records from LA County
        if record[29:32] == '037':

            fileid, stusab, sumlev, geocomp, chariter, cifsn, logrecno, region, division, state, \
            county, countycc, countysc, cousub, cousubcc, cousubsc, place, placecc, placesc, tract, \
            blkgrp, block, iuc, concit, concitcc, concitsc, aianhh, aianhhfp, aianhhcc, aihhtli, \
            aitsce, aits, aitscc, ttract, tblkgrp, anrc, anrccc, cbsa, cbsasc, metdiv, csa, necta, \
            nectasc, nectadiv, cnecta, cbsapci, nectapci, ua, uasc, uatype, ur, cd, sldu, sldl, vtd, \
            vtdi, reserve2, zcta5, submcd, submcdcc, sdelm, sdsec, sduni, arealand, areawatr, name, \
            funcstat, gcuni, pop100, hu100, intptlat, intptlon, lsadc, partflag, reserve3, uga, statens, \
            countyns, cousubns, placens, concitns, aianhhns, aitsns, anrcns, submcdns, cd113, cd114, \
            cd115, sldu2, sldu3, sldu4, sldl2, sldl3, sldl4, aianhhsc, csasc, cnectasc, memi, nmemi, \
            puma5, reserved = \
                extract_field_values_for_geo(record)

            sql_values.append([fileid, stusab, sumlev, geocomp, chariter, cifsn, logrecno, region, division, state, \
                county, countycc, countysc, cousub, cousubcc, cousubsc, place, placecc, placesc, tract, \
                blkgrp, block, iuc, concit, concitcc, concitsc, aianhh, aianhhfp, aianhhcc, aihhtli, \
                aitsce, aits, aitscc, ttract, tblkgrp, anrc, anrccc, cbsa, cbsasc, metdiv, csa, necta, \
                nectasc, nectadiv, cnecta, cbsapci, nectapci, ua, uasc, uatype, ur, cd, sldu, sldl, vtd, \
                vtdi, reserve2, zcta5, submcd, submcdcc, sdelm, sdsec, sduni, arealand, areawatr, name, \
                funcstat, gcuni, pop100, hu100, intptlat, intptlon, lsadc, partflag, reserve3, uga, statens, \
                countyns, cousubns, placens, concitns, aianhhns, aitsns, anrcns, submcdns, cd113, cd114, \
                cd115, sldu2, sldu3, sldu4, sldl2, sldl3, sldl4, aianhhsc, csasc, cnectasc, memi, nmemi, \
                puma5, reserved])

            log_rec_nos.append(logrecno)

            if len(sql_values) % 1000 == 0:
                print("Populating {} with {} records".format(str(record_count), str(len(sql_values))))
                execute_many_sql(census_db, sql_statement, sql_values)
                sql_values = []

            if record_count % 10000 == 0:
                census_db.commit()

    # One last execute to catch the remaining records
    execute_many_sql(census_db, sql_statement, sql_values)
    census_db.commit()

    return log_rec_nos


def populate_table_sf1(census_db, table_name, source_files, log_rec_nos):
    """In 2010, there are 3 tables for the first grouping.  First table
    just has the totals per census block, second table has the totals by
    single-race counts and the 3rd table has multi-race counts that we may want.

    So, we will first merge the 3 tables into a single pandas DataFrame and then
    insert the values into the sf1 database table"""

    frames = []
    for source_file in source_files:
        frame = pd.read_csv(source_file, dtype=str, header=None)
        cols = len(frame.columns)
        if len(frames) == 0:
            frames.append(frame)
        else:
            frame_slice = frame.iloc[:, 5:cols]
            frames.append(frame_slice)
    full_frame = pd.concat(frames, axis=1)

    field_holders = get_fields_holder(206)

    sql_statement = 'INSERT INTO ' + table_name + ' VALUES (' + field_holders + ');'
    sql_values = []
    record_count = 0
    for row in full_frame.itertuples(name=None):
        row_list = list(row)[1:]
        record_count += 1
        # Only inserting records from LA County - based on the geo_headings data
        if row_list[4] in log_rec_nos:
            sql_values.append(row_list)
            if len(sql_values) % 1000 == 0:
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
    populate_table_sf1(census_db, p1_table, sf1_files, log_rec_nos)
    if census_db:
        census_db.close()


"""fileid, stusab, chariter, cifsn, logrecno, p0010001, p0020001, p0020002, p0020003,
         p0020004, p0020005, p0020006, p0030001, p0030002, p0030003, p0030004, p0030005,
         p0030006, p0030007, p0030008, p0040001, p0040002, p0040003, p0050001, p0050002,
         p0050003, p0050004, p0050005, p0050006, p0050007, p0050008, p0050009, p0050010,
         p0050011, p0050012, p0050013, p0050014, p0050015, p0050016, p0050017, p0060001,
         p0060002, p0060003, p0060004, p0060005, p0060006, p0060007, p0070001, p0070002,
         p0070003, p0070004, p0070005, p0070006, p0070007, p0070008, p0070009, p0070010,
         p0070011, p0070012, p0070013, p0070014, p0070015, p0080001, p0080002, p0080003,
         p0080004, p0080005, p0080006, p0080007, p0080008, p0080009, p0080010, p0080011,
         p0080012, p0080013, p0080014, p0080015, p0080016, p0080017, p0080018, p0080019,
         p0080020, p0080021, p0080022, p0080023, p0080024, p0080025, p0080026, p0080027,
         p0080028, p0080029, p0080030, p0080031, p0080032, p0080033, p0080034, p0080035,
         p0080036, p0080037, p0080038, p0080039, p0080040, p0080041, p0080042, p0080043,
         p0080044, p0080045, p0080046, p0080047, p0080048, p0080049, p0080050, p0080051,
         p0080052, p0080053, p0080054, p0080055, p0080056, p0080057, p0080058, p0080059,
         p0080060, p0080061, p0080062, p0080063, p0080064, p0080065, p0080066, p0080067,
         p0080068, p0080069, p0080070, p0080071, p0090001, p0090002, p0090003, p0090004,
         p0090005, p0090006, p0090007, p0090008, p0090009, p0090010, p0090011, p0090012,
         p0090013, p0090014, p0090015, p0090016, p0090017, p0090018, p0090019, p0090020,
         p0090021, p0090022, p0090023, p0090024, p0090025, p0090026, p0090027, p0090028,
         p0090029, p0090030, p0090031, p0090032, p0090033, p0090034, p0090035, p0090036,
         p0090037, p0090038, p0090039, p0090040, p0090041, p0090042, p0090043, p0090044,
         p0090045, p0090046, p0090047, p0090048, p0090049, p0090050, p0090051, p0090052,
         p0090053, p0090054, p0090055, p0090056, p0090057, p0090058, p0090059, p0090060,
         p0090061, p0090062, p0090063, p0090064, p0090065, p0090066, p0090067, p0090068,
         p0090069, p0090070, p0090071, p0090072, p0090073"""