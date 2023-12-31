import csv
import json
from operator import length_hint
import requests
import time
from types import SimpleNamespace
from sqlalchemy import create_engine
import psycopg2
import datetime
import pytz
import os



def getData(date, tz, offset_hrs, report_url):
    t_delta_minute = datetime.timedelta(minutes=1)

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
        "Token": "hub_eyJhbGciOiJBMjU2S1ciLCJlbmMiOiJBMjU2Q0JDLUhTNTEyIiwidHlwIjoiSldUIn0.LU8t5tev2f5NPG_c5kr2soGBhtGm0CjDGDCOWbjKGNsyOGe523e3xkytNVqgsVrM8pcy34NXXmyt5OOj_G2df__PTS_ZiltS.377NjcehGbN_T08Jp44ZRg.xhKIrji8bnEQJ5tCmNFrmLRniHEpJ8t5zVGdoDPdw1vr5SEd6-irbC0LZcDFsqeIWYiH6Fh0VtC1NxvzWxVBNSMteNlcfsemsHeHnqiqqPM6sjjpYOJ1Sv7E4Tvd9aYtMb9IwQ4SVIPC4-labJ0-yB7BXiV4eXL1Baa503R2ZerDD46Ftlwj8pId3lAwL3WA52n2y1Cilv5Gn66XKMVl2vcR6iZI8o5l-Mk7hfN_N54OiEvdAg5kWvP7qk9y03UdaCo_TsAbkkF8JTLUrApfy6A6FsksNPaOrR_Xn_JXRs2dgFo5cXRm5Hsc_pAnEQOVOcsRr2xQJ1_Zp4AXURTHgg.Y-fwJiFiAkwa5aBRnMycKQPLQFlapGTADxLvjIpxaps"
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
    # https://unhcr.eyedro.com/#toolsexporter
    # meterDataWh = meter_response(table_names[0], ts2Epoch(dtStart))['Data']['Wh'][0]
    # for d in meterDataWh:
    #     print(d)



    # Send the GET request with headers and cookies
    response = requests.get(report_url, headers=headers, cookies=cookies)

    # Print the response content
    #print('RRRRRRR',response.text,'RRRRRRR')
    # {ManagmentActionResult, NumbersOfPages, ReportUID, ReportDescription}
    if response.text is None or response.text == 'https://login.galooli.com?sourceUrl=fleet&error=-1':
        print('????????????????????????????????????????????????')
        time.sleep(2)
        response = requests.get(report_url, headers=headers, cookies=cookies)
    if response.text and not '&error=-1' in response.text:
        x = json.loads(response.text, object_hook=lambda d: SimpleNamespace(**d))
        print(x.NumbersOfPages)
        data = [None] * x.NumbersOfPages
        ######print(data)
        for idx in list(range(x.NumbersOfPages)):
            time.sleep(1)
            print (idx)
            response = requests.get(GetDataUrl + x.ReportUID, headers=headers, cookies=cookies)
            ####print(response)
            if response.text is None:
                print(idx,'????????????????????????????????????????????????')
                time.sleep(2)
                response = requests.get(report_url, headers=headers, cookies=cookies)
            x = json.loads(response.text, object_hook=lambda d: SimpleNamespace(**d))
            data[x.PageNumber - 1] = x.PageData
            #####print(x.PageData)

        response = requests.get(CancelReportUrl + x.ReportUID, headers=headers, cookies=cookies)
        #print(response)
        start = True
        liters = []
        l1=l2=hr1=hr2=dl1=dl2 = 0
        #ttl1=ttl2=0
        tzz = pytz.timezone(tz)
        for val in data:
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!', val is None)
            if val is None:
                print('DDDDDDDDD',data)
            for z in val:
                ll1 = getattr(z,"ul2.Analog_Bead.1150001")
                ll2 = getattr(z,"ul2.Analog_Bead.1150002")
                h1 = getattr(z,"ul2.Analog_Bead.1150012")
                h2 = getattr(z,"ul2.Analog_Bead.1150013")
                d = getattr(z,"ul2_Record_Time")
                dt = d[6:10]+'-'+d[3:5]+'-'+d[0:2]+' '+d[11:16]
                epoch = ts2Epoch(dt, offset_hrs)
                datetime_obj = datetime.datetime.fromtimestamp(epoch)
                # Set the timezone to UTC
                utc_datetime = datetime_obj.astimezone(tzz)#utc_timezone)
                # Print the datetime object in a specific format
                formatted_datetime = utc_datetime.strftime('%Y-%m-%d %H:%M')
                formatted_datetime_end = (utc_datetime + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M')
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

                    #     liters.append({"ts":formatted_datetime, "wh": meterDataWh[idx][1],"l1":ll1, "l2":ll2, "dl1": -ttl1, "dl2": -ttl2,
                    #     ####liters.append({"ts":formatted_datetime, "wh": meterDataWh[idx][1],"l1":ll1, "l2":ll2, "dl1": ll1-dl1, "dl2": ll2-dl2, 
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
                    lastData = {"ts":formatted_datetime, "end":formatted_datetime_end, "epoch": epoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2,
                                    "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                    liters.append(lastData)
                elif (tdelta  == datetime.timedelta(minutes=2)):
                    missingTs = utc_datetime - t_delta_minute
                    missingEpoch = missingTs.timestamp()
                    formatted_missingTs = missingTs.strftime('%Y-%m-%d %H:%M')
                    formatted_missingTs_end = (missingTs + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M')
                    liters.append({"ts":formatted_missingTs, "end":formatted_missingTs_end, "epoch": missingEpoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2, 
                                    "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2})
                    lastData = {"ts":formatted_datetime,  "end":formatted_datetime_end, 
                                "epoch": epoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2, 
                                    "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                    liters.append(lastData)
                elif (tdelta  == datetime.timedelta(minutes=0)): # duplicate timestamp
                    print('DDDDDDDDDDDDDDDD',utc_datetime, epoch)
                    continue
                else: # more than 2 minute gap, use last data with adjusted ts
                    z = 0
                    # lastData = {"ts":formatted_datetime, "epoch": epoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2, 
                    #             "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                    # liters.append(lastData)
                    while z < 60:
                        z += 1
                        missingTs = lastTs + t_delta_minute
                        missingEpoch = missingTs.timestamp()
                        print('MMMMMMMM',missingEpoch, epoch, epoch - missingEpoch, utc_datetime, missingTs, lastTs)
                        if missingEpoch >= epoch:
                            lastData = {"ts":formatted_datetime, "end":formatted_datetime_end, "epoch": epoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2, 
                                        "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                            liters.append(lastData)
                            break

                        formatted_missingTs = missingTs.strftime('%Y-%m-%d %H:%M')
                        formatted_missingTs_end = (missingTs + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M')
                        #lastData.update({'ts':formatted_missingTs})
                        #lastData.update({'epoch': missingEpoch})
                        missingData = lastData = {"ts":formatted_missingTs, "end":formatted_missingTs_end, "epoch": missingEpoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2, 
                                    "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                        print('LLLLL', missingData)
                        liters.append(missingData)
                        lastTs = missingTs
                        lastData = missingData
                lastTs = utc_datetime
                # if l1-ll1 > 0:
                #     ttl1 += l1-ll1
                # if l2-ll2 > 0:
                #     ttl2 += l2-ll2
                l1 = ll1
                l2 = ll2
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    else:
        print('!!!!!!!!!! Bad Response !!!!!!!!!', response.text)
        exit()

    print(len(liters),'############',liters[0])
    print(len(liters),'############',liters[384])
    print(len(liters),'############',liters[385])
    print(len(liters),'############',liters[386])
    fn = 'OPEN_API/BIOHENRY/data/liters_%s.csv' % dtStart[0:10]
    print('fn',fn)
    with open(fn, 'w', newline='') as f:
        writer = csv.writer(f)
        # don't write header
        ##writer.writerow(["ts", "end", "wh", "l1", "l2", "dl1", "dl2", "hr1", "hr2", "dhr1",  "dhr2"])
        for x in liters:
            writer.writerow([x["ts"],x["end"],x['epoch'],x["l1"], x["l2"], x["dl1"], x["dl2"], x["hr1"], x["hr2"], x["dhr1"], x["dhr2"]])


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
    p = '%Y-%m-%d %H:%M'
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
    print('EyeDro Endpoint and Key Set')
    # Set URL with serial and timestamp,
    meter_url = "https://api.eyedro.com/customcmd?Cmd=Unhcr.GetData&DeviceSerial=" + str(serial) + "&DateStartSecUtc=" + str(timestamp) + f"&DateNumSteps=96&UserKey={USER_KEY_GET_DATA}"
    response = requests.get(meter_url, timeout=600)
    return json.loads(response.text)

# Define the URL this is for Detailed Daily report DG1 & DG2 Abuja Nigeria
####ReportUrl = "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214084&objType=u&startTime=%s%2000%3A00%3A00&endTime=%s%2023%3A59%3A59&favoriteId=10588" % (dtStart, dtEnd)
# Define the URL this is for Detailed Daily report DG1 & DG2 Ogoja Nigeria
###ReportUrl = "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214695&objType=u&startTime=%s%2000%3A00%3A00&endTime=%s%2023%3A59%3A59&favoriteId=10588" % (dtStart, dtEnd)
ReportUrls = [
    {"site": "OGOJA", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214695&objType=u&startTime=%s&endTime=%s&favoriteId=10588"}
]

# set these before calling getData()
year = 2023
month = 8
day = 10
date = datetime.datetime(year, month, day)
offset_hrs = 1
tz = 'Africa/Algiers'
recs = 1

print('111111111')
while 1 == 1:
    if recs == 0:
        break
    dtStart = '%s 00:00' % date.isoformat()[0:10]
    dtEnd = '%s 00:00' % (date + datetime.timedelta(days=1)).isoformat()[0:10]
    print( ReportUrls[0]["url"] % (dtStart, dtEnd))
    getData(date, tz, offset_hrs, ReportUrls[0]["url"] % (dtStart, dtEnd))
    recs -= 1
    date += datetime.timedelta(days=1)


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
    galooli_file_path = './OPEN_API/BIOHENRY/data/liters_2023-08-%s.csv' % ('0%s' % ddd)[-2:]
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
                idx = 0
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
                        print(idx, ep, 'ZZZZZZZZ',splitLine)#ep, dt, dt.isoformat())#[0:16])
                        #splitLine[0] = dt.isoformat()[0:16]
                        #splitLine[5] = splitLine[5][:-1]
                        #exit()
                    fuel_data.append(splitLine)
                print(len(fuel_data), epStart, galooli_file_path, 'FFFFFFFF', fuel_data[0])
                idx = 0
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
                        if ep != fuel_data[idx][2]:
                            print(idx, tz, 'XXXXXXXXXX',ep, fuel_data[idx][2], fuel_data[idx][0],splitLine[0] )
                        assert(ep == fuel_data[idx][2])

                        #print(splitLine,'!!!!!!!',','.join(splitLine[2:])[:-1])

                        lineOut = '%s,%s,%s\n' %(','.join(fuel_data[idx][0:2]),','.join(splitLine[1:])[:-1], ','.join(fuel_data[idx][3:7]))
                        #print('LLLLLLLL','%s%s' %(lineOut,'XXXXX'),'\n\n',splitLine,fuel_data[idx][6:10])
                        #exit()
                        output_file.write(lineOut)
                        if ep == epEnd:
                            break
                        idx += 1
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