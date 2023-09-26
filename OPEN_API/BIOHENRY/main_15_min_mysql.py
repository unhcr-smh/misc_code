import csv
import json
from operator import length_hint
import requests
import time
from types import SimpleNamespace
from sqlalchemy import create_engine
import psycopg2
import MySQLdb
import datetime
import pytz
import os, sys, traceback

MYSQL = {
    'db': 'unhcr_fuel',
    'user': 'unhcr_fuel',
    'passwd': 'unhcr_fuel',
    'host': 'db4free.net',
}

def db_sql(cursor, conn, sql, vals):
    try:
        #cursor = create_m, valsysql_connection()
        cursor.execute(sql, vals)
        conn.commit()
    except Exception as e:
        print('EEEEEEEEE',e)


def db_select_sql(cursor, sql):
    try:
        #cursor = create_m, valsysql_connection()
        cursor.execute(sql)
        result = cursor.fetchall()
        print('RRRRRRR',result)
        for row in result:
            yield row
    except Exception as e:
        print('EEEEEEEEE',e)


def create_mysql_connection():
    ############return None, None
    print('Connecting to %s' % MYSQL.values())
    try:
        db_connection= MySQLdb.connect(**MYSQL)
        #("localhost","unhcr","unhcr","unhcr")
    # If connection is not successful
    except Exception as e:
        print("Can't connect to database",e)
        return 0
    # If Connection Is Successful
    print("Connected")
    cursor = db_connection.cursor()
    #conn = MySQLdb.connect(**MYSQL)
    #cursor = conn.cursor()
    return cursor, db_connection

def getData(report_data, date, tz, offset_hrs):
    result = 0
    liters = []
    t_delta_minute = datetime.timedelta(minutes=1)
    dtEnd = (date + datetime.timedelta(hours=30)).isoformat()
    dtStart = (date - datetime.timedelta(hours=offset_hrs)).isoformat()

    report_url = report_data["url"] % (dtStart, dtEnd)
    key = report_data["key"]
    fn = 'OPEN_API/BIOHENRY/data/galooli/%s_%s_liters_%s.csv' % (date.isoformat()[0:10], report_data["site"], key)
    if os.path.exists(fn):
        print('File exists, delete it to get new data %s' % fn)
        with open(fn, 'r') as file:
            # Read the lines from the file and remove newline characters
            liters = [line.strip() for line in file.readlines()]
        return [-1, fn, dtStart, liters]
    print( report_url, report_data["key"])

    # my local PG connection, not being used right now --- we are using CSV files for RetScreen
    # engine = create_engine('postgresql://unhcr:unhcr@localhost:5432/eyedro_meters')
    # con = psycopg2.connect(database="eyedro_meters", user="unhcr", password="unhcr", host="localhost", port="5432")
    # print('SQL Connection String Created')

    #Nigeria - SO Ogoja 009-80AA3 --- guest house
    #Nigeria - SO Ogoja 009-80AA5 --- office --- no data 3-8 to 3-13 2023 --- we need full days

    # table_names = ['00980AA5']
    # https://login.galooli.com
    # ID: unhcrbgl@hotmail.com
    # pw: Unhcrbgl2009@2
    # Define cookies  --- login on browser  and get "Token" cookie https://space-fleet.galooli.com
    cookies = {
        "Token": "hub_eyJhbGciOiJBMjU2S1ciLCJlbmMiOiJBMjU2Q0JDLUhTNTEyIiwidHlwIjoiSldUIn0.b-L4Y0pz8YVfjz5l-VTMe6jbuWtC_Hh2X9IVp3kG-8qJuGYsg5IkqTbaefvbbmVeCAskzVDLz20bIjcYijO5orlNGUy-7i--.9JHdlMztPUg-FQkxNQz5xQ.QULPI_oBWq9PQr3kz6-s9LxymohG4lINiU5EZ-zYKm4rb01y6WpL3kCruket9lMrP--y7tfiCN0G78DZrgC5QE_HJiU4CU8OeRJCMhe-qU5mB9EiZ8tPdfMzrZF_tG244FxWWZEOnDn4GOv0FTW-a1aZCt8UEgYhzQO-KGQvQVwEpQfV_88wCle0okrnDrbFx-7-YFPjt6mdXrtmVBs98DJqeoWKRaOYyq9Fx9dVGxiznq3LglFrLyxhbJB0kZ6WCsIINTsTVAX7ukQgFUL6tctEsWTsj4nvlFEMH2-lNJdeLiyCHVa_9jbsE0WZ6h_W6d4YE37d1EV3UbyYPuirkA.h25dTYAMyGgoi1UrFL-hieHfk62Rl_hq2_bdwUrJ0x0"
    }

    GetDataUrl = "https://space-fleet.galooli.com/_Base/ReportGetPageAny?reportUID="
    CancelReportUrl = "https://space-fleet.galooli.com/_Base/ReportFinish?reportUID="


    # Define headers -- nothing needed here
    headers = {
        "Custom-Header": "SomeValue"
    }

    print(date)
    utc_timezone = pytz.timezone('UTC')

    #TODO: API only gets 15 min data, so we download 1 minute data 1 week at a time via the website: 
    # # https://unhcr.eyedro.com/#toolsexporter
    # meterDataWh = meter_response('00980AA5', ts2Epoch(dtStart))['Data']['Wh'][0]
    # print(meterDataWh[0:4])
    # # for d in meterDataWh:
    # #     print(d)
    # #     exit()


    # Send the GET request with headers and cookies
    response = requests.get(report_url, headers=headers, cookies=cookies)

    # Print the response content
    #print('RRRRRRR',response.text,'RRRRRRR')
    # {ManagmentActionResult, NumbersOfPages, ReportUID, ReportDescription}
    if response.text is None or response.text == 'https://login.galooli.com?sourceUrl=fleet&error=-1':
        print('????????????????????????????????????????????????')
        time.sleep(2)
        response = requests.get(report_url, headers=headers, cookies=cookies)
    if not '&error=-1' in response.text:
        x = json.loads(response.text, object_hook=lambda d: SimpleNamespace(**d))
        print(x.NumbersOfPages)
        data = [None] * x.NumbersOfPages

        for idx in list(range(x.NumbersOfPages)):
            time.sleep(1)
            print (idx)
            response = requests.get(GetDataUrl + x.ReportUID, headers=headers, cookies=cookies)
            print('RRRR',response)
            if response.text is None:
                print(idx,'????????????????????????????????????????????????')
                time.sleep(2)
                response = requests.get(report_url, headers=headers, cookies=cookies)
            x = json.loads(response.text, object_hook=lambda d: SimpleNamespace(**d))
            print('zzzzzzzzzzzzzz',x)
            if x is None:
                return [result, fn, dtStart, liters]
            data[x.PageNumber - 1] = x.PageData
            #####print(x.PageData)

        response = requests.get(CancelReportUrl + x.ReportUID, headers=headers, cookies=cookies)
        #print(response)
        start = True
        l1=l2=hr1=hr2=dl1=dl2 = 0
        #ttl1=ttl2=0
        tzz = pytz.timezone(tz)
        print('DDDDDDDDD',data)

        for val in data:
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!', val is None)
            if val is None:
                print('DDDDDDDDD',data)
                continue
            for z in val:
                ll1 = getattr(z,"ul2.Analog_Bead.1150001")
                ll2 = getattr(z,"ul2.Analog_Bead.1150002")
                h1 = getattr(z,"ul2.Analog_Bead.1150012")
                h2 = getattr(z,"ul2.Analog_Bead.1150013")
                d = getattr(z,"ul2_Record_Time")
                dt = d[6:10]+'-'+d[3:5]+'-'+d[0:2]+'T'+d[11:17]+'00'
                # 15 minute change
                epoch = ts2Epoch(dt) - 15 * 60
                utc_datetime = datetime.datetime.utcfromtimestamp(epoch)
                # Set the timezone to UTC
                ###utc_datetime = datetime_obj.astimezone(tzz)#utc_timezone)
                # Print the datetime object in a specific format
                formatted_datetime = utc_datetime.strftime('%Y-%m-%dT%H:%M:%S')
                formatted_datetime_end = (utc_datetime + datetime.timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%S')
                #print('AAAAAAA',dt, epoch, datetime_obj.strftime('%Y-%m-%d %H:%M'),formatted_datetime)
                #exit()

                if start:
                    start = False
                    print('@@@@@@',z,'@@@@@@')
                    l1 = ll1
                    l2 = ll2
                    dl1 = ll1
                    dl2 = ll2
                    hr1 = h1
                    hr2 = h2
                    lastTs = utc_datetime - t_delta_minute
                # else:
                    # idx = find_index_of_element_in_list_of_lists(meterDataWh,int(epoch))
                    # if idx != -1:
                    #     print('MMMMMMMMMMMMMMM',meterDataWh[idx])
                    #     #print(liters)
                    #     print("Formatted datetime:", formatted_datetime)

                    #     liters.append({"start":formatted_datetime, "wh": meterDataWh[idx][1],"l1":ll1, "l2":ll2, "dl1": -ttl1, "dl2": -ttl2,
                    #     ####liters.append({"start":formatted_datetime, "wh": meterDataWh[idx][1],"l1":ll1, "l2":ll2, "dl1": ll1-dl1, "dl2": ll2-dl2, 
                    #                 "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2})
                    #     dl1 = ll1
                    #     dl2 = ll2
                    #     ttl1 = 0
                    #     ttl2 = 0
                    #     continue

                tdelta = utc_datetime - lastTs
                ####print('TTTTTTT',tdelta, tdelta == t_delta_minute, tdelta  < datetime.timedelta(minutes=2) )
                if l1-ll1 < 0:
                    l1=ll1
                if l2-ll2 < 0:
                    l2=ll2
                if tdelta == t_delta_minute:
                    lastData = {"key":key+str(epoch),"start":formatted_datetime, "end":formatted_datetime_end, "epoch": epoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2,
                                    "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                    liters.append(lastData)
                elif (tdelta  == datetime.timedelta(minutes=2)):
                    missingTs = utc_datetime - t_delta_minute
                    missingEpoch = missingTs.timestamp()
                    formatted_missingTs = missingTs.strftime('%Y-%m-%d %H:%M')
                    formatted_missingTs_end = (missingTs + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M')
                    liters.append({"key":key+str(epoch),"start":formatted_missingTs, "end":formatted_missingTs_end, "epoch": missingEpoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2,
                                    "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2})
                    lastData = {"key":key+str(epoch),"start":formatted_datetime,  "end":formatted_datetime_end,
                                "epoch": epoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2,
                                    "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                    liters.append(lastData)
                elif (tdelta  == datetime.timedelta(minutes=0)): # duplicate timestamp
                    print('DDDDDDDDDDDDDDDD',utc_datetime, epoch)
                    continue
                else: # more than 2 minute gap, use last data with adjusted ts
                    z = 0
                    # lastData = {"start":formatted_datetime, "epoch": epoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2, 
                    #             "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                    # liters.append(lastData)
                    while z < 60:
                        z += 1
                        missingTs = lastTs + t_delta_minute
                        missingEpoch = missingTs.timestamp()
                        print('MMMMMMMM',missingEpoch, epoch, epoch - missingEpoch, utc_datetime, missingTs, lastTs)
                        if missingEpoch >= epoch:
                            lastData = {"key":key+str(epoch),"start":formatted_datetime, "end":formatted_datetime_end, "epoch": epoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2, 
                                        "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                            liters.append(lastData)
                            break

                        formatted_missingTs = missingTs.strftime('%Y-%m-%d %H:%M')
                        formatted_missingTs_end = (missingTs + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M')
                        #lastData.update({'ts':formatted_missingTs})
                        #lastData.update({'epoch': missingEpoch})
                        missingData = lastData = {"key":key+str(epoch),"start":formatted_missingTs, "end":formatted_missingTs_end, "epoch": missingEpoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2, 
                                    "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                        print('LLLLL', missingData)
                        liters.append(missingData)
                        lastTs = missingTs
                        lastData = missingData
                lastTs = utc_datetime
                l1 = ll1
                l2 = ll2
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    else:
        print('!!!!!!!!!! Bad Response !!!!!!!!!', response.text)
        return [result, fn, dtStart, liters]

    if len(liters) == 0:
        return [result, fn, dtStart, liters]
    print(len(liters),'############',liters[0])
    print(len(liters),'############',liters[10])
    print(len(liters),'############',liters[385])
    print(len(liters),'############',liters[386])
    print('fn',fn)

    with open(fn, 'w', newline='') as f:
        writer = csv.writer(f)
        # write header
        writer.writerow(["key", "start", "end", "epoch", "tankl1", "tankl2", "deltal1", "deltal2", "hrs1", "hrs2", "deltahrs1", "deltahrs2"])
        for x in liters:
            data = [x["key"], x["start"],x["end"],x['epoch'],x["l1"], x["l2"], x["dl1"], x["dl2"], x["hr1"], x["hr2"], x["dhr1"], x["dhr2"]]
            writer.writerow(data)
    with open(fn, 'r') as file:
        # Read the lines from the file and remove newline characters
        lines = [line.strip() for line in file.readlines()]
        return [1, fn, dtStart, lines]

def concatenate_files_in_name_order(directory, output_file):
    # Get a list of all files in the directory
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

    # Sort the files based on their names
    sorted_files = sorted(files)

    # Concatenate the contents of the sorted files
    with open(output_file, 'w') as output:
        start = True
        for file_name in sorted_files:
            if file_name[0:7] == 'liters_':
                print(file_name)
                file_path = os.path.join(directory, file_name)
                with open(file_path, 'r') as input_file:
                    if start:
                         output.write('ts,wh,l1,l2,dl1,dl2,hr1,hr2,dhr1,dhr2\n')
                         start = False
                    output.write(input_file.read())

def ts2Epoch(dt, offset_hrs=0):
    p = '%Y-%m-%dT%H:%M:%S'
    epoch = datetime.datetime(1970, 1, 1)
    e = (datetime.datetime.strptime(dt, p) - datetime.timedelta(hours= offset_hrs) - epoch).total_seconds()
    ####print(dt,e,datetime.datetime.strptime(dt, p) - datetime.timedelta(hours= offset_hrs))
    return int(e)

def find_index_of_element_in_list_of_lists(list_of_lists, target_element):
    for index, sublist in enumerate(list_of_lists):
        if target_element in sublist:
            return index

    return -1  # Element not found

'''
This function takes as its input a meter serial number and an epoch timestamp and calls the GetData API to retrieve
the prior day's readings (96 steps at 15-minute intervals). It returns the response as JSON text
'''
def meter_response(serial, timestamp):
    ##API_BASE_URL = "https://api.eyedro.com/customcmd"
    ##USER_KEY = "UNHCRMHiYgbHda9cRv4DuPp28DnAnfeV8s6umP5R"
    USER_KEY_GET_DATA = "UNHCRp28DnAV8s6uHdMHiYgba95RcRv4DnfeuPmP"
    print('EyeDro Endpoint and Key Set', serial, timestamp)
    # Set URL with serial and timestamp,
    meter_url = "https://api.eyedro.com/customcmd?Cmd=Unhcr.GetData&DeviceSerial=" + str(serial) + "&DateStartSecUtc=" + str(timestamp) + f"&DateNumSteps=96&UserKey={USER_KEY_GET_DATA}"
    response = requests.get(meter_url, timeout=600)
    print('!!!!!!!!!!!!!!!!!',response)
    return json.loads(response.text)

# Define the URL this is for Detailed Daily report DG1 & DG2 Abuja Nigeria
####ReportUrl = "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214084&objType=u&startTime=%s%2000%3A00%3A00&endTime=%s%2023%3A59%3A59&favoriteId=10588" % (dtStart, dtEnd)
# Define the URL this is for Detailed Daily report DG1 & DG2 Ogoja Nigeria
###ReportUrl = "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214695&objType=u&startTime=%s%2000%3A00%3A00&endTime=%s%2023%3A59%3A59&favoriteId=10588" % (dtStart, dtEnd)
#TODO: no Taraba, using Takum
    # ID: unhcrbgl@hotmail.com
    # pw: Unhcrbgl2009@2

fuel_kwh_header = 'key,start,end,epoch,tankl1,tankl2,deltal1,deltal2,hrs1,hrs2,deltahrs1,deltahrs2'
# example with multiple GBs
calabar_gbs = [{"label": "GEN", "id": "00980B76"}, {"label": "GRID", "id": "00980A9C"}]
report_data = [
    {"site": "OGOJA", "meters": [{"label": "HOUSE", "id": "00980AA3"}], "key":"OGOJA_GUEST_HOUSE_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214015&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    
    {"site": "ABUJA", "meter_id": "00980785", "key":"ABUJA_OFFICE_DG1_and_DG2_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214084&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    {"site": "ADIKPO", "meter_id": "00980AAF", "key":"ADIKPO_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214687&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    {"site": "CALABAR", "meters": calabar_gbs, "key":"CALABAR_BASE_TANK_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214680&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    {"site": "LAGOS", "meter_id": "00980A9E", "key":"UNHCR_LAGOS_OFFICE_DG1_and_DG2_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214694&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    {"site": "OGOJA", "meters": [{"label": "HOUSE", "id": "00980AA3"}], "key":"OGOJA_GUEST_HOUSE_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214015&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    {"site": "OGOJA", "meters": [{"label": "OFFICE", "id": "00980AA5"}], "key":"UNHCR_OGOJA_OFFICE_DG1_and_DG2_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214695&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    {"site": "TARABA", "meter_id": "00980AA1", "key":"TARABA_DG1_And_DG2_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214697&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
]


# set these before calling getData()
year = 2023
month = 9
day = 20
date = datetime.datetime(year, month, day)
offset_hrs = 1
tz = 'Africa/Algiers'
days = 6

cnt_processed = 0
site_idx = 0

end_idx = 1 #len(report_data)

cursor, conn = create_mysql_connection()
while site_idx < end_idx:
    meters =  report_data[site_idx]['meters']
    h = fuel_kwh_header
    meter_ids = ''
    fkidx = 1
    for m in meters:
        meter_ids += '_' + m["label"] +'-' + m["id"]
        h += ',GB' + str(fkidx)+'_kwh'
        fkidx += 1
    #     print(m,m["id"])
    #     meterDataWh = meter_response(m["id"], ts2Epoch(date.isoformat()))
    #     #meterDataWh = meterDataWh['Data']['Wh'][0]
    #     print(len(meterDataWh), '\n',meterDataWh['Data'],'\n')
    h += '\n'
    print(h)
    print(meter_ids)
    #exit()
    
    
    print('idx:',site_idx, date,  report_data[site_idx]["key"])
    loop = days
    while loop > 0:
        print(loop)
        res = getData(report_data[site_idx], date, tz, offset_hrs)
        cnt_processed += res[0]
        fn = res[1]
        dtStart = res[2]
        liters = res[3]
        #####print('LLLLLLLLLLLLLLLL',liters)
        if len(liters) == 0:
            exit()
        #liters[0] += ',123456'
        print(len(liters), liters[0],'\n', liters[0].split(','))

        fn = fn.replace('/galooli/', '/combined/').replace('_liters_', '%s_' % meter_ids).replace('.csv','combined.csv')
        if os.path.exists(fn) and cursor is not None:
            print('File exists, delete it to get new data %s' % fn)
            ######loop = 0
            #with open(fn, 'r') as file:

            sql = '''SELECT TABLE_NAME AS `Table`,ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024) AS `Size (KB)`
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = "unhcr_fuel" AND TABLE_NAME = "unhcr_fuel_kwh";'''

            try:
                # rows = db_select_sql(cursor, sql) #'select * from unhcr_fuel.unhcr_fuel_wh')
                # for row in rows:
                #     print('XXXXXX','Table size %s kb' % row[1])
                #     for x in row:
                #         print('RRRRRRR',x)

                #key,start,end,epoch,tankl1,tankl2,deltal1,deltal2,hrs1,hrs2,deltahrs1,deltahrs2,kwh
                sql = 'REPLACE INTO unhcr_fuel.unhcr_fuel_gb_kwh (start, end, tank1, tank2, delta1, delta2, site_key, gb1, gb2, gb3, gb4, gb5, gb6) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                vals = []
                with open(fn, 'r') as f:
                    f.readline() # read the header
                    for l in f.readlines():
                        data_list = l.split(',')
                        #print(list[12][:-1],list)
                        key = data_list[0] + '_' +meter_ids
                        values = [data_list[1], data_list[2], data_list[4], data_list[5],data_list[6],data_list[7],key,None,None,None,None,None,None]
                        
                        x = 7
                        print('SSSSSSSSS', vals, len(values),len(data_list),'\n\n', data_list)
                        for y in range(12,len(data_list)):
                            values[x] = data_list[y].replace('\n','')
                            x += 1
                            print(x,y,data_list[y])
                        print(values)
                        #exit()
                        vals.append(values)
                        #print('SSSSSSSSS', len(values),vals[0],'\n\n',data_list)
                        #print(sql % values)
                        #exit()
                    cursor.executemany(sql, vals)
                    print('YYYYYYYYYYYYY\n', "%s record inserted." % cursor.rowcount, '\nYYYYYYYY')
                    conn.commit()
            except Exception as e:
                print('SSSSSSS',e, traceback.print_exc(file=sys.stdout))
                exit()
        else:
            kwh = []
            cnt = -1
            for m in meters:
                meterDataWh = meter_response(m["id"], ts2Epoch(date.isoformat()))
                meterDataWh = meterDataWh['Data']['Wh'][0]
                kwh.append(meterDataWh)
                if cnt == -1:
                    cnt = len(meterDataWh)
                print(cnt, len(meterDataWh), m["id"],'\n')
                if cnt != len(meterDataWh) or cnt != 96:
                    print('????????????????????????????????????')
                    exit()
            #######################################
            # https://unhcr.eyedro.com/#toolsexporter
            # meterDataWh = meter_response(report_data[site_idx]['meter_id'], ts2Epoch(date.isoformat()))
            # meterDataWh = meterDataWh['Data']['Wh'][0]
            # print(fn, "\n",len(meterDataWh), meterDataWh[0:4])
            # print('MMMMM',len(meterDataWh),'MMMMM')
            fuel_idx = 1
            fuel_kwh_combo = []
            fuel_epoch = -1
            len_fuel = len(liters)

            kwh_idx = -1
            dl1_idx = -6
            dl2_idx = -5

            dl1_sum = 0
            dl2_sum = 0

            for x in list(range(0,cnt)):
                if fuel_idx >= len_fuel:
                    break
                #print('11111',len(kwh),kwh[0][x][0],fuel_epoch)

                while kwh[0][x][0] != fuel_epoch and fuel_idx < len_fuel:
                    fuel = liters[fuel_idx].split(',')
                    fuel_epoch = int(fuel[3].split('.')[0]) - offset_hrs *60*60
                    # 1693612800 1693609200
                    if kwh[0][x][0] != fuel_epoch:
                        fuel_idx += 1
                        if fuel_epoch >= kwh[0][x][0] - 15 * 60:
                            dl1_sum += round(float(fuel[dl1_idx]), 4)
                            dl2_sum += round(float(fuel[dl2_idx]), 4)
                        continue
                    wh = 0
                    for k in list(range(0,len(kwh))):
                        wh += kwh[k][x][1]
                    if wh < 100:
                        dl1_sum = 0
                        dl2_sum = 0
                    fuel[dl1_idx] = str(dl1_sum)[:4]
                    fuel[dl2_idx] = str(dl2_sum)[:4]
                    dt = datetime.datetime.utcfromtimestamp(fuel_epoch)
                    fuel[1] = dt.isoformat().replace('T', ' ')
                    fuel[2] = (dt + datetime.timedelta(minutes=15)).isoformat().replace('T', ' ')
                    fuel[3] = str(fuel_epoch)
                    fuel_str = ','.join(fuel)
                    #print('FFF',fuel_str)
                    for k in list(range(0,len(kwh))):
                        fuel_str += ',' + str(kwh[k][x][1]/1000) #kwh
                        #print('FFF',fuel_str)
                    fuel_kwh_combo.append(fuel_str)
                    print(x, fuel_idx,'YYYYYYY', fuel_kwh_combo[0], 'ZZZZZ\n',fuel,'\n',fuel_str)
                    dl1_sum = 0
                    dl2_sum = 0


            fuel_kwh = fuel_kwh_combo[10].split(',')
            print('FFFFFFFF',len(meterDataWh), len(fuel_kwh_combo), fuel_kwh, 'kwh:',fuel_kwh[kwh_idx], 'dl2:',fuel_kwh[dl2_idx], 'dl1:',fuel_kwh[dl1_idx],'\n',fn)

            with open(fn, "w") as f:
                # write header
                f.write(h)
                # Write each item in the list to the file, followed by a newline
                for item in fuel_kwh_combo:
                    f.write(item + "\n")
            # for f in fuel_kwh_combo:
            #     f = f.split(',')
            #     print(f,'XXX',len(f),'yyy',f[-1], f[-2],'\n',fn)

        #exit()
            # sql = "REPLACE INTO unhcr_fuel.unhcr_fuel_wh (ts, tank1, tank2, delta1, delta2, wh, site_key) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            # vals = ('2020-08-06', 1.2, 2.3,9.4,7.5,999,'2019-08-06_a_b')
            # cursor, conn = create_mysql_connection()
            # try:
            #     rows = db_sql(cursor, conn, sql, vals) #'select * from unhcr_fuel.unhcr_fuel_wh')
            #     #for row in rows:
                    
            #     #cursor.execute('select * from unhcr_fuel_wh')
            #     print('YYYYYYYYYYYYY\n', "%s record inserted." % cursor.rowcount, '\nYYYYYYYY')
            # except Exception as e:
            #     print('SSSSSSS',e)
        
            # # for table in result:
            # #     yield table[0]
            
            # with open(fn, 'r') as fuel_file:
            #         idx = 0
                
            #         #print('11111111111111111',fuel_file)
            #         for line in fuel_file:
            #             print(line)
            #             idx += 1
            #             if idx > 2:
            #                 break
                        
            # for d in meterDataWh:
            #     print(d)
            #exit()
            
            
            
            
            
            
            
            
        loop -= 1
        date += datetime.timedelta(days=1)
        #######################################
    site_idx += 1
    loop = days
    date = datetime.datetime(year, month, day)
print('@@@@@@@@@@@@@@@@ processed: %s  out of %s @@@@@@@@@@@@@@@@@@@@@@' %(cnt_processed, len(report_data)))
exit()

#directory = './'
#output_file = 'liters7-2023.csv'
#concatenate_files_in_name_order(directory, output_file)

##### "D:\OneDrive - UNHCR\GEEPP\RETSCREEN\worked-data\Nigeria-SO-Ogoja-Office-03-01-2023-present-adjusted.csv"
# last timestamp from last entry
epInit = 1692658800
dd = 21
ddd = dd
print('!!!!!!!!!!',ddd)
while ddd < 31:
    ddd += 1

    # Open the input file for reading
    input_file_path = './OPEN_API/BIOHENRY/data/eyedro/one-minute-data_Nigeria-SO-Ogoja-Office_8-22_9-02-2023.csv'
    galooli_file_path = './OPEN_API/BIOHENRY/data/galooli/liters_2023-08-%s.csv' % ('0%s' % ddd)[-2:]
    output_file_path = './OPEN_API/BIOHENRY/data/output4.csv'

    print(galooli_file_path)

    #True for write, False for append
    header = False
    # last of previous  month epoch
    epStart = epInit + (86400 * (ddd-dd-1))
    epEnd = epStart + 86340
    print(('0%s' % ddd)[-2:], ' epstart: ', epStart, epInit)

    with open(input_file_path, 'r') as input_file:
        # Open the output file for writing
        with open(output_file_path, 'a') as output_file:
            with open(galooli_file_path, 'r') as fuel_file:
                site_idx = 0
                fuel_data = []
                #print('11111111111111111',fuel_file)
                for line in fuel_file:
                    splitLine = line.split(',')
                    #print('SSSSSS',splitLine)
                    #exit()
                    ts = splitLine[0]
                    dt = datetime.datetime.fromisoformat(ts)#.astimezone(pytz.timezone('Africa/Algiers'))
                    ep = int(splitLine[2].split('.')[0])#####int(dt.timestamp())
                    splitLine[2] = ep
                    splitLine[4] = splitLine[4][0:6]
                    splitLine[5] = splitLine[5][0:6]
                    #print('2222222222222222',ts,dt,ep,epStart,epStart-ep)
                    #exit()
                    if (ep == epStart or ep == epEnd):
                        print(site_idx, ep, 'ZZZZZZZZ',splitLine)#ep, dt, dt.isoformat())#[0:16])
                        #splitLine[0] = dt.isoformat()[0:16]
                        #splitLine[5] = splitLine[5][:-1]
                        #exit()
                    fuel_data.append(splitLine)
                print(len(fuel_data), epStart, galooli_file_path, 'FFFFFFFF', fuel_data[0])
                site_idx = 0
                for line in input_file:
                    # Modify the line (example: convert to uppercase)
                    if not header:
                        #print(line,line.split(',')[1])
                        splitLine = line.split(',')
                        if not splitLine[1].isnumeric():
                            tz = splitLine[0].split('(')[1].split(')')[0]
                            continue
                        ep = int(splitLine[1])
                        #if (ep == epStart or ep == epEnd):
                            #print('QQQQQQ',idx, len(fuel_data))
                        if ep < epStart:
                            continue
                        # if ep == epStart:
                        #     idx = 0
                        dtUtc = datetime.datetime.fromtimestamp(ep)#.astimezone(pytz.timezone(tz))
                        dt = dtUtc.astimezone(pytz.timezone(tz))
                        splitLine[0] = dt.isoformat()[0:16]
                        #print(splitLine[0])
                        #print('QQQQ',idx,ep, fuel_data[idx])
                        if ep != fuel_data[site_idx][2]:
                            print(site_idx, tz, 'XXXXXXXXXX',ep, fuel_data[site_idx][2], fuel_data[site_idx][0],splitLine[0] )
                        assert(ep == fuel_data[site_idx][2])

                        #print(splitLine,'!!!!!!!',','.join(splitLine[2:])[:-1])

                        lineOut = '%s,%s,%s\n' %(','.join(fuel_data[site_idx][0:2]),','.join(splitLine[1:])[:-1], ','.join(fuel_data[site_idx][3:7]))
                        #print('LLLLLLLL','%s%s' %(lineOut,'XXXXX'),'\n\n',splitLine,fuel_data[idx][6:10])
                        #exit()
                        output_file.write(lineOut)
                        if ep == epEnd:
                            break
                        site_idx += 1
                        # if idx > 3:
                        #     exit()
                    else:
                        header = False
                        tz = line.split(',')[0].split('(')[1].split(')')[0]
                        print(tz)
                        line = line[:-1] + ',tank1,tank2,dl1,dl2\n'
                        output_file.write(line)
                    # Write the modified line to the output file
            #       output_file.write(modified_line)

print("File processing complete.")
dir = 'D:/OneDrive - UNHCR/GEEPP/RETSCREEN/raw-data/Ogoja-office/'
files = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]

# Sort the files based on their names
print(epEnd + 60, epEnd - epStart)