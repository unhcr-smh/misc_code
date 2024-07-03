import csv
import decimal
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
import shutil
import argparse


"""
    Python example:

DeviceSerial    = '00980AA1'    # meter serial number
DateStartSecUtc = 1704067200    # must be on a 15 minute boundary
DateNumSteps    = 96            # number of time intervals -- 96 = 1 day (not sure of the limit) 
UserKey         = 'UNHCRp28DnAV8s6uHdMHiYgba95RcRv4DnfeuPmP'    # our super secret




'https://api.eyedro.com/customcmd?Cmd=Unhcr.GetData&DeviceSerial=00980AA1&DateStartSecUtc=1704067200&DateNumSteps=96&UserKey=UNHCRp28DnAV8s6uHdMHiYgba95RcRv4DnfeuPmP'

Response:

response = requests.get(meter_url, timeout=600)
    print('!!!!!!!!!!!!!!!!!',response)
    return json.loads(response.text)

JSON response:
meterDataWh = {'DateMsUtc': 1709049034223, 'Errors': [], 'Cmd': 'Unhcr.GetData', 'DeviceSerial': '00980AA1', 'LastCommSecUtc': 1706869969, 'Data': {'Wh': [...]}, 'CmdVersion': 'Latest'}

The Data:
data = meterDataWh['Data']['Wh'][0]

array of arrays = [[1704067200, 1020],[1704068100, 1014],....]  # timestamp, wh

    """

# Create a parser object
parser = argparse.ArgumentParser(description='A simple calculator.')

# Add arguments to the parser
parser.add_argument('x', type=float, help='The first number')

MYSQL = {
    'db': 'unhcr_fuel',
    'user': 'unhcr_fuel',
    'passwd': 'unhcr_fuel',
    'host': 'db4free.net',
}

def db_sql(cursor, conn, sql, vals):
    try:
        cursor.execute(sql, vals)
        conn.commit()
    except Exception as e:
        print('EEEEEEEEE',e)


def db_select_sql(cursor, sql):
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        print('RRRRRRR',result)
        for row in result:
            yield row
    except Exception as e:
        print('EEEEEEEEE',e)


def create_mysql_connection():
    #return None, None   # for no mysql queries
    print('Connecting to %s' % MYSQL.values())
    try:
        db_connection= MySQLdb.connect(**MYSQL)
    except Exception as e:
        print("Can't connect to database",e)
        return 0
    # If Connection Is Successful
    print("Connected")
    cursor = db_connection.cursor()
    return cursor, db_connection

def getData(report_data, date, tz, offset_hrs, bulk = False):
    result = 0
    liters = []
    t_delta_minute = datetime.timedelta(minutes=1)
    dtEnd = (date + datetime.timedelta(hours=30)).isoformat()
    dtStart = (date - datetime.timedelta(hours=offset_hrs)).isoformat()

    report_url = report_data["url"] % (dtStart, dtEnd)
    key = report_data["key"]
    fn = 'NIGERIA_FUEL_BIOHENRY/data/galooli/%sBASE_TANK_NEW.csv' % report_data["key"]
    if bulk == True:
        fn = fn.replace('_liters_','_bulk_')
    if os.path.exists(fn):
        print('File exists, delete it to get new data %s' % fn)
        with open(fn, 'r') as file:
            # Read the lines from the file and remove newline characters
            liters = [line.strip() for line in file.readlines()]
        return  gen_file_from_csv(fn.replace('TANK','TANK_CSV'), dtStart, liters)  ##[-1, fn, dtStart, liters]
    print( report_url, report_data["key"])

    # https://login.galooli.com
    # ID: hermes@unhcr.org
    # pw: Unhcr.0077
    
    # ID: unhcrbgl@hotmail.com
    # pw: Unhcrbgl2009@4
    # Define cookies  --- login on browser  and get "Token" cookie https://space-fleet.galooli.com
    cookies = {
        "Token": "hub_eyJhbGciOiJBMjU2S1ciLCJlbmMiOiJBMjU2Q0JDLUhTNTEyIiwidHlwIjoiSldUIn0.7G9HG1GwRUhorxV4vbMprdjMAjgbFKah6i2Wp4TyHrXnCuxt4h4U9IxYZ4rSpIeOoi8P_DZIHdvn_Jnjw2YPXzW6tU9A94HM.7YDBx9MTjWR5nqMQY1flmw.qIIhFlYS_NvEOAB1VLzIGMWF8d2kkEevYEJxtwQyzexHkVzRYAqFwQWXMAmE5DfOhXhw-6i0pF8AZV7LmsdooRicqEdRyi4V-5FE7nDPwsZ2JwgDlJQiBkRUj7wDi7bOEq0XLBumXh2GHlmVlGspOEXGCuTxXZPNlOqi-dkDfXp_mEV_VP_V42eErWdWw3zg1WWkov31DFTx5V7Jg78q7t9iV3AaMbYIKZyANgbFitF7cLwOq57quOunoAexSWcVPH9cHLoTINW5fyruuC_SAcbrnoLo56J_ZVcLuLewO4eWncL6mLaWLwEVfoDOil4pOg3gF2Y9EYgw2VR_2YcYpw.MJG5eYDwKQgzErXhN7pWwl1_oYnaaUcXNrd2dDu9I7w"
    }

    GetDataUrl = "https://space-fleet.galooli.com/_Base/ReportGetPageAny?reportUID="
    CancelReportUrl = "https://space-fleet.galooli.com/_Base/ReportFinish?reportUID="


    # Define headers -- nothing needed here
    headers = {
        "Custom-Header": "SomeValue"
    }
    
    reportDefJSON = {"ReportName":"Generator+Performance:+01/10/2023+13:39:38","ReportTypeCode":18,"Fields":[{"FieldDisplayOptions":{"DisplayName":"Unit+Id","ColumnDisplayName":"Unit+Id","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"unit_id"},{"FieldDisplayOptions":{"DisplayName":"Unit+Name","ColumnDisplayName":"Unit+Name","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"unit_name"},{"FieldDisplayOptions":{"DisplayName":"Group+Name","ColumnDisplayName":"Group+Name","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"group_name"},{"FieldDisplayOptions":{"DisplayName":"Cluster+Name","ColumnDisplayName":"Cluster+Name","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"fleet_name"},{"FieldDisplayOptions":{"DisplayName":"Site+Layout","ColumnDisplayName":"Site+Layout","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"unit_layout_code"},{"FieldDisplayOptions":{"DisplayName":"Start+date","ColumnDisplayName":"Start+date","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"sumcalc_StartDate"},{"FieldDisplayOptions":{"DisplayName":"Duration+(Days)","ColumnDisplayName":"Duration+(Days)","HiddenInResultsTable":False,"IsCalculatedField":True,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"sumcalc_TotalDays"},{"FieldDisplayOptions":{"DisplayName":"","ColumnDisplayName":"Generator+1+Operational+Fuel+[Liter]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"generators_DG1OperationalConsump"},{"FieldDisplayOptions":{"DisplayName":"","ColumnDisplayName":"Generator+2+Operational+Fuel+[Liter]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"generators_DG2OperationalConsump"},{"FieldDisplayOptions":{"DisplayName":"","ColumnDisplayName":"Generator+2+Overall+Fuel+[Liter]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"generators_DG2TotalConsump"},{"FieldDisplayOptions":{"DisplayName":"","ColumnDisplayName":"Generator+1+Overall+Fuel+[Liter]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"generators_DG1TotalConsump"},{"FieldDisplayOptions":{"DisplayName":"DG1+Total+Engine+Hours","ColumnDisplayName":"DG1+Total+Engine+Hours","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"generators_DG1TotalEngine"},{"FieldDisplayOptions":{"DisplayName":"DG2+Total+Engine+Hours","ColumnDisplayName":"DG2+Total+Engine+Hours","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"generators_DG2TotalEngine"},{"FieldDisplayOptions":{"DisplayName":"Generator.Generator1.KVA","ColumnDisplayName":"Generator.Generator1.KVA","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"additional_info.KVA_Gen1"},{"FieldDisplayOptions":{"DisplayName":"Generator.Generator2.KVA","ColumnDisplayName":"Generator.Generator2.KVA","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"additional_info.KVA_Gen2"},{"FieldDisplayOptions":{"DisplayName":"DG1+-+Load+Power+Avg+[KW]","ColumnDisplayName":"DG1+-+Load+Power+Avg+[KW]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"energy_DG1avgLoad"},{"FieldDisplayOptions":{"DisplayName":"DG1+-+Load+Power+Max+[KW]","ColumnDisplayName":"DG1+-+Load+Power+Max+[KW]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"energy_DG1maxLoad"},{"FieldDisplayOptions":{"DisplayName":"DG2+-+Load+Power+Avg+[KW]","ColumnDisplayName":"DG2+-+Load+Power+Avg+[KW]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"energy_DG2avgLoad"},{"FieldDisplayOptions":{"DisplayName":"DG2+-+Load+Power+Max+[KW]","ColumnDisplayName":"DG2+-+Load+Power+Max+[KW]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"energy_DG2maxLoad"},{"FieldDisplayOptions":{"DisplayName":"Generator+1+-+Total+Energy+[KWH]","ColumnDisplayName":"Generator+1+-+Total+Energy+[KWH]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"energy_generator1TotalEnergyKWH"},{"FieldDisplayOptions":{"DisplayName":"Generator+2+-+Total+Energy+[KWH]","ColumnDisplayName":"Generator+2+-+Total+Energy+[KWH]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"energy_generator2TotalEnergyKWH"},{"FieldDisplayOptions":{"DisplayName":"Consumed","ColumnDisplayName":"Consumed","HiddenInResultsTable":False,"IsCalculatedField":True,"IsTextualCalculatedField":False,"CalculationFormulaText":"generators_DG1OperationalConsump+generators_DG2OperationalConsump","CalculatedFields":[],"IsActiveCalculatedField":True},"UniqueName":"calculated_field_1"},{"FieldDisplayOptions":{"DisplayName":"","ColumnDisplayName":"Generator+1+Total+Fuel+Drop+[Liter]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"generators_DG1TotalFuelDrop"},{"FieldDisplayOptions":{"DisplayName":"","ColumnDisplayName":"Generator+2+Total+Fuel+Drop+[Liter]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"generators_DG2TotalFuelDrop"},{"FieldDisplayOptions":{"DisplayName":"","ColumnDisplayName":"Generator+1+Total+Refueled+[Liter]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"generators_DG1TotalRefueled"},{"FieldDisplayOptions":{"DisplayName":"","ColumnDisplayName":"Generator+2+Total+Refueled+[Liter]","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"generators_DG2TotalRefueled"},{"FieldDisplayOptions":{"DisplayName":"Drop","ColumnDisplayName":"Drop","HiddenInResultsTable":False,"IsCalculatedField":True,"IsTextualCalculatedField":False,"CalculationFormulaText":"generators_DG1TotalFuelDrop+generators_DG2TotalFuelDrop","CalculatedFields":[],"IsActiveCalculatedField":True},"UniqueName":"calculated_field_2"},{"FieldDisplayOptions":{"DisplayName":"Refueled","ColumnDisplayName":"Refueled","HiddenInResultsTable":False,"IsCalculatedField":True,"IsTextualCalculatedField":False,"CalculationFormulaText":"generators_DG1TotalRefueled+generators_DG2TotalRefueled","CalculatedFields":[],"IsActiveCalculatedField":True},"UniqueName":"calculated_field_3"},{"FieldDisplayOptions":{"DisplayName":"Liter/Gallon+per+kWh","ColumnDisplayName":"Liter/Gallon+per+kWh","HiddenInResultsTable":False,"IsCalculatedField":True,"IsTextualCalculatedField":False,"CalculationFormulaText":"(generators_DG1OperationalConsump+generators_DG2OperationalConsump)/(energy_generator1TotalEnergyKWH+energy_generator2TotalEnergyKWH)","CalculatedFields":[],"IsActiveCalculatedField":True},"UniqueName":"calculated_field_4"},{"FieldDisplayOptions":{"DisplayName":"Liter/Gallon+per+Hour","ColumnDisplayName":"Liter/Gallon+per+Hour","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"generators_DG1loadBasedFuelConsumption"},{"FieldDisplayOptions":{"DisplayName":"Avg.+Engine+Hours","ColumnDisplayName":"Avg.+Engine+Hours","HiddenInResultsTable":False,"IsCalculatedField":True,"IsTextualCalculatedField":False,"CalculationFormulaText":"generators_DG1TotalEngine+generators_DG2TotalEngine","CalculatedFields":[],"IsActiveCalculatedField":True},"UniqueName":"calculated_field_6"},{"FieldDisplayOptions":{"DisplayName":"Average+load+%","ColumnDisplayName":"Average+load+%","HiddenInResultsTable":False,"IsCalculatedField":True,"IsTextualCalculatedField":False,"CalculationFormulaText":"energy_DG1avgLoad/(additional_info.KVA_Gen1*0.8)","CalculatedFields":[],"IsActiveCalculatedField":True},"UniqueName":"calculated_field_7"},{"FieldDisplayOptions":{"DisplayName":"Load+split","ColumnDisplayName":"Load+split","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":True,"CalculationFormulaText":"IF(NUM('energy_DG1avgLoad/(additional_info.KVA_Gen1*0.8)==0');'No+Load+Information'\n;IF(NUM('energy_DG1avgLoad/(additional_info.KVA_Gen1*0.8)>=1');'100%+'\n;IF(NUM('energy_DG1avgLoad/(additional_info.KVA_Gen1*0.8)>=0.8');'80%-100%'\n;IF(NUM('energy_DG1avgLoad/(additional_info.KVA_Gen1*0.8)>=0.6');'60%-80%'\n;IF(NUM('energy_DG1avgLoad/(additional_info.KVA_Gen1*0.8)>=0.3');'30%-60%';'<30%')))))","CalculatedFields":[],"IsActiveCalculatedField":True},"UniqueName":"calculated_field_8"},{"FieldDisplayOptions":{"DisplayName":"DG+Panel+1","ColumnDisplayName":"DG+Panel+1","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"Config_Site_Global.Digital_Bead.2160000"},{"FieldDisplayOptions":{"DisplayName":"DG+Running+1","ColumnDisplayName":"DG+Running+1","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"Config_Site_Global.Digital_Bead.2160010"},{"FieldDisplayOptions":{"DisplayName":"Generator+configured","ColumnDisplayName":"Generator+configured","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":True,"CalculationFormulaText":"IF('Config_Site_Global.Digital_Bead.2160000'='Yes';'True';\nIF('Config_Site_Global.Digital_Bead.2160010'='Yes';'True';'False'))","CalculatedFields":[],"IsActiveCalculatedField":True},"UniqueName":"calculated_field_9"},{"FieldDisplayOptions":{"DisplayName":"Generator.Maintenance.Cycle+hours+for+periodic+maintenance","ColumnDisplayName":"Generator.Maintenance.Cycle+hours+for+periodic+maintenance","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"additional_info.Cycle_hours_for_periodic_maintenance"},{"FieldDisplayOptions":{"DisplayName":"Generator.Maintenance.Periodic+maintenance+visit+cost","ColumnDisplayName":"Generator.Maintenance.Periodic+maintenance+visit+cost","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"additional_info.Periodic_maintenance_visit_cost"},{"FieldDisplayOptions":{"DisplayName":"Generator.Fuel.Cost+per+Liter/Gallon","ColumnDisplayName":"Generator.Fuel.Cost+per+Liter/Gallon","HiddenInResultsTable":False,"IsCalculatedField":False,"IsTextualCalculatedField":False,"CalculationFormulaText":None,"CalculatedFields":[],"IsActiveCalculatedField":False},"UniqueName":"additional_info.Cost_per_Liter_Gallon"},{"FieldDisplayOptions":{"DisplayName":"Maintenance+cost","ColumnDisplayName":"Maintenance+cost","HiddenInResultsTable":False,"IsCalculatedField":True,"IsTextualCalculatedField":False,"CalculationFormulaText":"(generators_DG1TotalEngine+generators_DG2TotalEngine)*IF(additional_info.Periodic_maintenance_visit_cost>0;additional_info.Periodic_maintenance_visit_cost;90)/IF(additional_info.Cycle_hours_for_periodic_maintenance>0;additional_info.Cycle_hours_for_periodic_maintenance;250)","CalculatedFields":[],"IsActiveCalculatedField":True},"UniqueName":"calculated_field_10"},{"FieldDisplayOptions":{"DisplayName":"Fuel+Cost","ColumnDisplayName":"Fuel+Cost","HiddenInResultsTable":False,"IsCalculatedField":True,"IsTextualCalculatedField":False,"CalculationFormulaText":"(generators_DG1TotalConsump+generators_DG2TotalConsump)*IF(additional_info.Cost_per_Liter_Gallon>0;additional_info.Cost_per_Liter_Gallon;0.78)","CalculatedFields":[],"IsActiveCalculatedField":True},"UniqueName":"calculated_field_11"}],"RequiredFields":[],"Filters":{}}
    execParamsJSON = {"ZonObjects":"u7214078","PeriodType":0,"StartTime":"2023-10-11+10:26:15","EndTime":"2023-10-18+10:26:15","Language":0,"AdditionalWhere":"","AllowOverridingTimeConstraints":True,"IgnoreStockFleets":True}
    print(date)
    utc_timezone = pytz.timezone('UTC')
    
    
    try:
        response = requests.post('https://space-galooli.galooli.com/_Base/ReportInitiate',
                        json = {"reportDefJSON":reportDefJSON,
                                "execParamsJSON":execParamsJSON},
                        headers=headers,
                        cookies=cookies)
    except Exception as err:
        print('EEEEEEEEE',err)
    x = json.loads(response.text, object_hook=lambda d: SimpleNamespace(**d))
    
    
    

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

    # try:
    #     response = requests.post('https://space-galooli.galooli.com/Dashboard/InitiateDashboard',
    #                     json = {"myReportId":-10842,"reportDefJSON":x.ReportJSONs.ReportDefJSON,
    #                             "execParamsJSON":x.ReportJSONs.ExecParamsJSON},
    #                     headers=headers,
    #                     cookies=cookies)
    # except Exception as err:
    #     print('EEEEEEEEE',err)
            
    if not '&error=-1' in response.text:
        x = json.loads(response.text, object_hook=lambda d: SimpleNamespace(**d))
        
        print('XXXXXXXXXXXXXX\n',x.ReportJSONs.ExecParamsJSON, '\nYYYYYYYYYY\n\n',x.ReportJSONs.ReportDefJSON, '\nZZZZZZZZZ\n\n')
        
    try:
        response = requests.post('https://space-galooli.galooli.com/Dashboard/InitiateDashboard',
                        json = {"myReportId":-10842,"reportDefJSON":x.ReportJSONs.ReportDefJSON,
                                "execParamsJSON":x.ReportJSONs.ExecParamsJSON},
                        headers=headers,
                        cookies=cookies)
    except Exception as err:
        print('EEEEEEEEE',err)
        
        
        # try:
        #     response = requests.post('https://space-galooli.galooli.com/_Base/ReportInitiate',
        #                     json = {"reportDefJSON":x.ReportJSONs.ReportDefJSON,
        #                             "execParamsJSON":x.ReportJSONs.ExecParamsJSON},
        #                     headers=headers,
        #                     cookies=cookies)
        # except Exception as err:
        #     print('EEEEEEEEE',err)
        x = json.loads(response.text, object_hook=lambda d: SimpleNamespace(**d))
        data = [None] * x.NumbersOfPages

        for idx in list(range(x.NumbersOfPages)):
            time.sleep(1)
            print (idx)
            response = requests.get(GetDataUrl + x.ReportUID, headers=headers, cookies=cookies)
            print('RRRR',response)
            if response.text is None or response.text == '':
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
            #print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!', val is None)
            if val is None:
                print('DDDDDDDDD',data)
                continue
            for z in val:
                ll1 = getattr(z,"ul2.Analog_Bead.1150001")
                ll2 = getattr(z,"ul2.Analog_Bead.1150002")
                h1 = getattr(z,"ul2.Analog_Bead.1150012")
                h2 = getattr(z,"ul2.Analog_Bead.1150013")
                d = getattr(z,"ul2_Record_Time")
                dt = d[0:4]+'-'+d[6:8]+'-'+d[10:11]+'T'+d[11:17]+'00'
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
            #print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
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
        if bulk:
            writer.writerow(["key", "start", "end", "epoch", "tankl1"])
            for x in liters:
                data = [x["key"], x["start"],x["end"],x['epoch'],x["l1"]]
                writer.writerow(data)
        else:
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

Python example:
Parameters: serial: string, greenbox serial number, example:  '00980AA1' 
            timestamp: number, Unix epoch, must be on 15 minute boundary, example: 1710720000

# get a day of 15 minute data (actually 96 15 minute intervals)
def meter_response(serial, timestamp):
    USER_KEY_GET_DATA = "UNHCRp28DnAV8s6uHdMHiYgba95RcRv4DnfeuPmP"
    print('EyeDro Endpoint and Key Set', serial, timestamp)
    # Set URL with serial and timestamp,
    meter_url = "https://api.eyedro.com/customcmd?Cmd=Unhcr.GetData&DeviceSerial=" + str(serial) + "&DateStartSecUtc=" + str(timestamp) + f"&DateNumSteps=96&UserKey={USER_KEY_GET_DATA}"
    response = requests.get(meter_url, timeout=600)
    print('!!!!!!!!!!!!!!!!!',response)
    return json.loads(response.text)

    {"Serial":"00980AA1","Label":"Nigeria - FO Takum","LastCommSecUtc":1710942887}
    {"Serial":"00980B77","Label":"Nigeria - FO Takum (Office) Gen 2 (B)","LastCommSecUtc":1710942886} 100kVa
    {"Serial":"00980B7A","Label":"Nigeria - FO Takum (Office) Gen 2","LastCommSecUtc":1710942890}  80kVa

key,start,end,epoch,tankl1,tankl2,deltal1,deltal2,hrs1,hrs2,deltahrs1,deltahrs2,GB1_kwh
key,start,end,epoch,tankl1,tankl2,deltal1,deltal2,hrs1,hrs2,deltahrs1,deltahrs2,GB1_kwh,GB2_kwh,GB3_kwh

'''

def meter_response(serial, timestamp):
    ##API_BASE_URL = "https://api.eyedro.com/customcmd"
    ##USER_KEY = "UNHCRMHiYgbHda9cRv4DuPp28DnAnfeV8s6umP5R"
    USER_KEY_GET_DATA = "UNHCRp28DnAV8s6uHdMHiYgba95RcRv4DnfeuPmP"
    print('EyeDro Endpoint and Key Set', serial, timestamp)
    # Set URL with serial and timestamp,
    meter_url = "https://api.eyedro.com/customcmd?Cmd=Unhcr.GetData&DeviceSerial=" + str(serial) + "&DateStartSecUtc=" + str(timestamp) + f"&DateNumSteps=96&UserKey={USER_KEY_GET_DATA}"
    #get devices
    #meter_url = 'https://api.eyedro.com/customcmd?Cmd=Unhcr.GetDeviceInventoryList&UserKey=UNHCRMHiYgbHda9cRv4DuPp28DnAnfeV8s6umP5R'
    response = requests.get(meter_url, timeout=600)
    print('!!!!!!!!!!!!!!!!!',response)
    return json.loads(response.text)

def archive_folders(source_folder, destination_folder, match):

    files = os.listdir(source_folder) # get the list of files in the source folder

    for file in files: # loop through the files
        if file.endswith(match): # check if the file name starts with 'pws'
            shutil.move(os.path.join(source_folder, file), os.path.join(destination_folder, file)) # move the file to the destination folder
            print('moving %s' %file)
        
    
    
def archive_data(cursor, conn):
    #### archive data
    dt = datetime.date.today()  - datetime.timedelta(days=90)
    sql = "REPLACE INTO unhcr_fuel.unhcr_fuel_gb_kwh_archive SELECT * FROM unhcr_fuel.unhcr_fuel_gb_kwh WHERE start < '%s'" % dt
    try:
        cursor.execute(sql)
        conn.commit()
        cnt = cursor.rowcount
        print('YYYYYYYYYYYYY\n', "%s record inserted." % cnt, '\nYYYYYYYY')
        if cnt > 0:
            sql = "DELETE FROM unhcr_fuel.unhcr_fuel_gb_kwh WHERE start < '%s'" % dt
            cursor.execute(sql)
            conn.commit()
            cnt = cursor.rowcount
            print('YYYYYYYYYYYYY\n', "%s record deleted." % cnt, '\nYYYYYYYY')
    except Exception as err:
        print('EEEEEEEEE',err)
        print(sql)
        return False
    return True
# Define the URL this is for Detailed Daily report DG1 & DG2 Abuja Nigeria
####ReportUrl = "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214084&objType=u&startTime=%s%2000%3A00%3A00&endTime=%s%2023%3A59%3A59&favoriteId=10588" % (dtStart, dtEnd)
# Define the URL this is for Detailed Daily report DG1 & DG2 Ogoja Nigeria
###ReportUrl = "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214695&objType=u&startTime=%s%2000%3A00%3A00&endTime=%s%2023%3A59%3A59&favoriteId=10588" % (dtStart, dtEnd)
#TODO: no Taraba, using Takum
# https://login.galooli.com
    # ID: hermes@unhcr.org
    # pw: Unhcr.0077
    
    # ID: unhcrbgl@hotmail.com
    # pw: Unhcrbgl2009@4

def gen_file_from_csv(fn, dtStart, data):
    start = True
    key = None ######'CALABAR_BASE_TANK_'
    liters = []
    t_delta_minute = datetime.timedelta(minutes=1)
    l1=l2=hr1=hr2=dl1=dl2 = 0
    #ttl1=ttl2=0
    tzz = pytz.timezone(tz)
    #print('DDDDDDDDD',data)
    xxx = -1
    if os.path.exists(fn):
        with open(fn, 'r') as file:
            # Read the lines from the file and remove newline characters
            lines = [line.strip() for line in file.readlines()]
        return [-1, fn, dtStart, lines]

    for val in data:
        xxx += 1
        #print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!', val is None)
        if val is None:
            #print('DDDDDDDDD',data)
            continue
        if val.startswith('Unit'):
            continue

        val = val.replace('"','')
        z = val.split(',')
        if key is None:
            key = z[0]
        ll1 = decimal.Decimal(z[2])
        ll2 = decimal.Decimal(z[3])
        h1 = decimal.Decimal(z[4])
        h2 = decimal.Decimal(z[5])
        d = z[1]
        dt = d[0:4]+'-'+d[5:7]+'-'+d[8:10]+'T'+d[11:16]+':00'
        # 15 minute change
        epoch = ts2Epoch(dt) - 15 * 60
        utc_datetime = datetime.datetime.utcfromtimestamp(epoch)
        # Set the timezone to UTC
        ###utc_datetime = datetime_obj.astimezone(tzz)#utc_timezone)
        # Print the datetime object in a specific format
        formatted_datetime = utc_datetime.strftime('%Y-%m-%dT%H:%M:%S')+':00'
        formatted_datetime_end = (utc_datetime + datetime.timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%S')+':00'
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

        if len(liters) % 30 == 0:
            print('x', len(liters))
        tdelta = utc_datetime - lastTs
        ####print('TTTTTTT',tdelta, tdelta == t_delta_minute, tdelta  < datetime.timedelta(minutes=2) )
        if l1-ll1 < 0:
            l1=ll1
        if l2-ll2 < 0:
            l2=ll2
        epoch = str(epoch).split('.')[0]
        if tdelta == t_delta_minute:
            lastData = {"key":key+epoch,"start":formatted_datetime, "end":formatted_datetime_end, "epoch": epoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2,
                            "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
            liters.append(lastData)
        # elif (tdelta  == datetime.timedelta(minutes=2)):
        #     missingTs = utc_datetime - t_delta_minute
        #     missingEpoch = str(int(epoch) + 60).split('.')[0]
        #     formatted_missingTs = missingTs.strftime('%Y-%m-%d %H:%M')+':00'
        #     missingEpoch = str(ts2Epoch(missingTs.strftime('%Y-%m-%dT%H:%M')+':00') - 15 * 60).split('.')[0]
        #     formatted_missingTs_end = (missingTs + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M')+':00'
        #     liters.append({"key":key+missingEpoch,"start":formatted_missingTs, "end":formatted_missingTs_end, "epoch": missingEpoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2,
        #                     "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2})
        #     lastData = {"key":key+missingEpoch,"start":formatted_datetime,  "end":formatted_datetime_end,
        #                 "epoch": missingEpoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2,
        #                     "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
        #     liters.append(lastData)
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
                formatted_missingTs = missingTs.strftime('%Y-%m-%dT%H:%M:%S')+':00'
                formatted_missingTs_end = (missingTs + datetime.timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%S')+':00'
                
                missingEpoch = str(int(liters[len(liters)-1]['epoch']) +60).split('.')[0]
                print('MMMMMMMM',missingEpoch, epoch, int(epoch) - int(missingEpoch), formatted_missingTs, missingTs, lastTs)
                if int(missingEpoch) <= int(epoch):
                    lastData = {"key":key+missingEpoch,"start":formatted_missingTs, "end":formatted_missingTs_end, "epoch": missingEpoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2, 
                                "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                    liters.append(lastData)
                    lastTs = missingTs
                    l1 = ll1
                    l2 = ll2
                    continue

                #lastData.update({'ts':formatted_missingTs})
                #lastData.update({'epoch': missingEpoch})
                missingData = lastData = {"key":key+missingEpoch,"start":formatted_missingTs, "end":formatted_missingTs_end, "epoch": missingEpoch,"l1":ll1, "l2":ll2, "dl1": l1-ll1, "dl2": l2-ll2, 
                            "hr1": h1, "hr2": h2, "dhr1": hr1-h1,  "dhr2": hr2-h2}
                print('LLLLL', missingData)
                #liters.append(missingData)
                lastTs = missingTs
                lastData = missingData
                break
        lastTs = utc_datetime
        l1 = ll1
        l2 = ll2
        #print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    if len(liters) == 0:
        return [0, fn, dtStart, liters]
    print(len(liters),'############',liters[0])
    print(len(liters),'############',liters[10])
    print(len(liters),'############',liters[385])
    print(len(liters),'############',liters[386])
    print('fn',fn)


    with open(fn, 'w', newline='') as f:
        writer = csv.writer(f)
        # write header
        # if bulk:
        #     writer.writerow(["key", "start", "end", "epoch", "tankl1"])
        #     for x in liters:
        #         data = [x["key"], x["start"],x["end"],x['epoch'],x["l1"]]
        #         writer.writerow(data)
        # else:
        writer.writerow(["key", "start", "end", "epoch", "tankl1", "tankl2", "deltal1", "deltal2", "hrs1", "hrs2", "deltahrs1", "deltahrs2"])
        for x in liters:
            data = [x["key"], x["start"],x["end"],x['epoch'],x["l1"], x["l2"], x["dl1"], x["dl2"], x["hr1"], x["hr2"], x["dhr1"], x["dhr2"]]
            writer.writerow(data)
    with open(fn, 'r') as file:
        # Read the lines from the file and remove newline characters
        lines = [line.strip() for line in file.readlines()]
    return [-1, fn, dtStart, lines]


    #{"Serial":"00980AA1","Label":"Nigeria - FO Takum","LastCommSecUtc":1710942887}
    #{"Serial":"00980B77","Label":"Nigeria - FO Takum (Office) Gen 2 (B)","LastCommSecUtc":1710942886} 100kVa
    #{"Serial":"00980B7A","Label":"Nigeria - FO Takum (Office) Gen 2","LastCommSecUtc":1710942890}  80kVa

fuel_kwh_header = 'key,start,end,epoch,tankl1,tankl2,deltal1,deltal2,hrs1,hrs2,deltahrs1,deltahrs2'
# example with multiple GBs
calabar_gbs = [{"label": "GEN", "id": "00980B76"}, {"label": "GRID", "id": "00980A9C"}]
takum_gbs = [{"label": "GEN1", "id": "00980B7A"}, {"label": "GEN2", "id": "00980B77"}, {"label": "GRID", "id": "00980AA1"}]
report_data = [
    ####{"site": "CALABAR", "meters": calabar_gbs, "key":"CALABAR_BULK_TANK_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214078&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    ####{"site": "CALABAR", "meters": calabar_gbs, "key":"CALABAR_DG1_And_DG2_", "url": "https://space-fleet.galooli.com/Fleet/GetReportData?objId=7214680&objType=u&startTime=%s&endTime=%s&favoriteId=10588&reportType=Favorite_1"},
    #{"site": "ABUJA", "meters": [{"label": "OFFICE", "id": "00980785"}], "key":"ABUJA_OFFICE_DG1_and_DG2_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214084&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    #####{"site": "ADIKPO", "meters": [{"label": "OFFICE", "id": "00980AAF"}], "key":"ADIKPO_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214687&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    ####{"site": "LAGOS", "meters": [{"label": "OFFICE", "id": "00980A9E"}], "key":"UNHCR_LAGOS_OFFICE_DG1_and_DG2_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214694&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    #{"site": "OGOJA", "meters": [{"label": "HOUSE", "id": "00980AA3"}], "key":"OGOJA_GUEST_HOUSE_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214015&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    #{"site": "OGOJA", "meters": [{"label": "OFFICE", "id": "00980AA5"}], "key":"UNHCR_OGOJA_OFFICE_DG1_and_DG2_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214695&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    {"site": "TARABA", "meters": takum_gbs, "key":"TARABA_DG1_And_DG2_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214697&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
    ####{"site": "TARABA-OLD", "meters": [{"label": "OFFICE", "id": "00980AA1"}], "key":"TARABA_DG1_And_DG2_", "url": "https://space-fleet.galooli.com/Fleet/ExecuteFavoriteReport?objId=7214697&objType=u&startTime=%s&endTime=%s&favoriteId=10588"},
]
 # https://login.galooli.com
# ID: hermes@unhcr.org
# pw: Unhcr.0077

# ID: unhcrbgl@hotmail.com
# pw: Unhcrbgl2009@4

# set these before calling getData()
year = 2024
month = 6
day = 22
date = datetime.datetime(year, month, day)
offset_hrs = 1
tz = 'Africa/Algiers'
days = 13

cnt_processed = 0
site_idx = 0

# Create a parser object
parser = argparse.ArgumentParser(description='To archive or not.')

# Add arguments to the parser
parser.add_argument('archive', help='archive_files or archive_data')

args = parser.parse_args()
print('AAAAAAAA', args)

if args.archive == 'archive_files':
    source_folder = 'NIGERIA_FUEL_BIOHENRY\\data\\galooli'
    destination_folder = 'NIGERIA_FUEL_BIOHENRY\\data\\archive\\galooli'
    match = 'CSV_NEW.csv'
    archive_folders(source_folder, destination_folder, match)

    source_folder = 'NIGERIA_FUEL_BIOHENRY\\data\\combined'
    destination_folder = 'NIGERIA_FUEL_BIOHENRY\\data\\archive\\combined'
    match = '.csv'
    archive_folders(source_folder, destination_folder, match)
    exit()


# set to 1 to just do the first site in the report_data list
end_idx = 1 #####len(report_data)
cursor, conn = create_mysql_connection()              ##############None, None 

if args.archive == 'archive_data':
    res = archive_data(cursor, conn)
    if res != True:
        exit(99)
    exit()


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
        dtStart = res[2][0:10] + '_'
        liters = res[3]
        #####print('LLLLLLLLLLLLLLLL',liters)
        if len(liters) == 0:
            exit()
        #liters[0] += ',123456'
        #print(len(liters), liters[0],'\n', liters[0].split(','))
        if '_bulk_' in fn:
            try:
                sql = 'REPLACE INTO unhcr_fuel.unhcr_fuel_bulk (start, end, epoch, bulk, site_key) VALUES (%s, %s, %s, %s, %s)'
                vals = []
                with open(fn, 'r') as f:
                    f.readline() # read the header
                    for l in f.readlines():
                        print(l)
                        data_list = l.split(',')
                        #print(list[12][:-1],list)
                        key = data_list[0] + '_' +meter_ids
                        values = [data_list[1].replace('\n',''), 
                                  data_list[2].replace('\n',''), 
                                  data_list[3].split('.')[0].replace('\n',''), 
                                  data_list[4].replace('\n',''),
                                  key]
                        vals.append(values)
                    cursor.executemany(sql, vals)
                    print('YYYYYYYYYYYYY\n', "%s record inserted." % cursor.rowcount, '\nYYYYYYYY')
                    conn.commit()
            except Exception as e:
                print('EEEEEEE',e, traceback.print_exc(file=sys.stdout))

            loop -= 1
            date += datetime.timedelta(days=1)
            continue

        fn = fn.replace('/galooli/', '/combined/'+ date.isoformat()[0:10]+ '_').replace('_liters_', '%s_' % meter_ids).replace('.csv','_combined.csv')
        ######################## insert to regenerate combined file
        # if os.path.exists(fn):
        #     os.remove(fn)
        ########################
        if os.path.exists(fn) and cursor is not None:
            print('File exists, delete it to get new data %s' % fn)
            ######loop = 0
            #with open(fn, 'r') as file:

            # not checking table size right now
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
                        key = data_list[0]
                        values = [data_list[1], data_list[2], data_list[4], data_list[5],data_list[6],data_list[7],key,None,None,None,None,None,None]
                        
                        x = 7
                        #print('SSSSSSSSS', vals, len(values),len(data_list),'\n\n', data_list)
                        for y in range(12,len(data_list)):
                            values[x] = data_list[y].replace('\n','')
                            x += 1
                            #print(x,y,data_list[y])
                        #print(values)
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
                # fields = ['ts', 'kwh', 'Label', 'ID']
                # with open('data%s.csv' %m['label'], 'w') as f:
                #     csv_writer = csv.writer(f)
                #     # Write the field names (column headers) to the first row of the CSV file
                #     csv_writer.writerow(fields)
                #     # Write all of the rows of data to the CSV file
                #     csv_writer.writerows(meterDataWh)
                    
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

                while kwh[0][x][0] > fuel_epoch and fuel_idx < len_fuel:
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
                    # if wh < 100:
                    #     dl1_sum = 0
                    #     dl2_sum = 0
                 
                    fuel[dl1_idx] = str(dl1_sum)[:4]
                    fuel[dl2_idx] = str(dl2_sum)[:4]
                    dt = datetime.datetime.utcfromtimestamp(fuel_epoch)
                    fuel[1] = (dt - datetime.timedelta(minutes=60)).isoformat().replace('T', ' ')
                    fuel[2] = (dt - datetime.timedelta(minutes=45)).isoformat().replace('T', ' ')
                    fuel[3] = str(fuel_epoch)
                    fuel[0] = report_data[site_idx]["key"] + fuel[3] + meter_ids
                    fuel_str = ','.join(fuel)
                    #print('FFF',fuel_str)
                    for k in list(range(0,len(kwh))):
                        fuel_str += ',' + str(kwh[k][x][1]/1000) #kwh
                        #print('FFF',fuel_str)
                    fuel_kwh_combo.append(fuel_str)
                    print(x, fuel_idx,'YYYYYYY', fuel_kwh_combo[0], 'ZZZZZ\n',fuel,'\n',fuel_str)
                    dl1_sum = 0
                    dl2_sum = 0

            if len(fuel_kwh_combo) >= 10:
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
    input_file_path = './NIGERIA_FUEL_BIOHENRY/data/eyedro/one-minute-data_Nigeria-SO-Ogoja-Office_8-22_9-02-2023.csv'
    galooli_file_path = './NIGERIA_FUEL_BIOHENRY/data/galooli/liters_2023-08-%s.csv' % ('0%s' % ddd)[-2:]
    output_file_path = './NIGERIA_FUEL_BIOHENRY/data/output4.csv'

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