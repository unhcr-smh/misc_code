import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime as dt
print('Libraries imported')

# Create connection string to SQL DB with meter readings

#######ENGINE = create_engine('postgresql://postgres:4raxeGo5xgB$@localhost:5432/eyedro_meters')
ENGINE = create_engine('mysql://lor__eor:lor__eor@db4free.net/eyedro_meters') # connect to server
# table DDL -- should have matching entry in generator_info.xlsx file

# -- eyedro_meters.`009-XXXXX` definition

# CREATE TABLE `009-XXXXX` (
#   `index` int DEFAULT NULL,
#   `DeviceSerial` int DEFAULT NULL,
#   `Timestamp` int NOT NULL,
#   `Wh` int DEFAULT NULL,
#   `kWH` double DEFAULT NULL,
#   `KW` double DEFAULT NULL,
#   `gmt_timestamp` varchar(50) DEFAULT NULL,
#   `month` int DEFAULT NULL,
#   `week` int DEFAULT NULL,
#   `day_of_month` int DEFAULT NULL,
#   `day_of_week` varchar(50) DEFAULT NULL,
#   `time` varchar(50) DEFAULT NULL,
#   `timeslot_mean` double DEFAULT NULL,
#   `timeslot_median` double DEFAULT NULL,
#   `Wh_Outlier` tinyint(1) DEFAULT NULL,
#   `KVA Rating` double DEFAULT NULL,
#   `EOR` double DEFAULT NULL,
#   `LOR` double DEFAULT NULL,
#   PRIMARY KEY (`Timestamp`),
#   KEY `LOR_IDX` (`LOR`) USING BTREE
#) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

print('SQL Connection String Created')

# Set power factor and load assumptions
FIXED_PF = 0.8
FIXED_LOAD = 0.75
MIN_FIXED_LOAD_FACTOR = .3
print(f'Power factor and load assumptions set at PF = {FIXED_PF}; Load = {FIXED_LOAD}; Minimum Fixed Load Factor = {MIN_FIXED_LOAD_FACTOR}')

# Pull list of generator meter info from Excel
gen_info = 'LOR-EOR\generator_info.xlsx'
df_gen_info = pd.read_excel(gen_info)

# Add cleaned, non-dash serial number to pair with meter readings later
df_gen_info.insert(df_gen_info.columns.get_loc('Meter Serial No.') + 1, 'DeviceSerial_NoDash', df_gen_info['Meter Serial No.'].str.replace('-', ''))
print('Meter-Generator info file imported and cleaned')

# Create functions for use in the script

def calc_eor(kva,pf,load,kw):
    eor = ((kva*pf*load)-kw)/kw
    return eor

print('Function created: calc_eor')

# ((60 * PF)* MIN_FIXED_LOAD_FACTOR)/kw
def calc_lor(kva,pf,load,kw):
    #if kva*pf <= MIN_FIXED_LOAD_FACTOR:
    lor = (kva*pf*MIN_FIXED_LOAD_FACTOR)/kw
    return lor
    #return ((kva*pf*load)-kw)/kw
    #else:
    #    pass

print('Function created: calc_lor')

meter_serials = df_gen_info[['Meter Serial No.', 'KVA Rating']].values.tolist()
print('XXXXXXXXX',meter_serials)
for serial in    meter_serials:
    if serial[0] != '009-80B1E': continue
    
    rt_st = dt.now()
    
    try:

        # Call PG database to pull in meter readings for a sample 
        print('AAAAAAAA', serial)
        df_meter_readings = pd.read_sql_query(f'select * from `{serial[0]}` WHERE LOR is null limit 10000',con=ENGINE)
        print("df_meter_readings",serial)
        #### continue

        # Create a new "kWH" column by dividing "Wh" by 1000
        # Already exists
        ######df_meter_readings.insert(df_meter_readings.columns.get_loc("Wh") + 1, "kWH", df_meter_readings['Wh'] / 1000)

        # Create a new "KW" column by parsing from the "kWH" column
        # Already exists
        ######df_meter_readings.insert(df_meter_readings.columns.get_loc("kWH") + 1, "KW", df_meter_readings['kWH'] * 4)

        # Remove 0-value readings from the dataset
        # Already exists
        ######df_meter_readings = df_meter_readings[df_meter_readings['Wh'] != 0]

        # Create a boolean column 'Wh_Outlier' based being 2 std dev above or below the timeslot mean
        # Already exists
        ######df_meter_readings['Wh_Std'] = df_meter_readings.groupby(['day_of_week', 'time'])['Wh'].transform('std')
        # Already exists
        ######df_meter_readings['Wh_Outlier'] = ((df_meter_readings['Wh'] > df_meter_readings['timeslot_mean'] + 2 * df_meter_readings['Wh_Std']) | (df_meter_readings['Wh'] < df_meter_readings['timeslot_mean'] - 2 * df_meter_readings['Wh_Std']))

        # Pull in the KVA Rating for the given meter/generator
        # Already exists
        #df_meter_readings = df_meter_readings.merge(df_gen_info[['DeviceSerial_NoDash', 'KVA Rating']], 
        #                                            left_on='DeviceSerial', 
        #                                            right_on='DeviceSerial_NoDash', 
        #                                            how='left')

        # Drop the extra "DeviceSerial_NoDash" column if needed
        # Does not  exists
        #df_meter_readings = df_meter_readings.drop(columns=['DeviceSerial_NoDash'])

        # Calculate EOR
        eor = df_meter_readings.apply(lambda row: calc_eor(row['KVA Rating'], FIXED_PF, FIXED_LOAD, row['KW']), axis=1)
        df_meter_readings['EOR'] = eor
        # Calculate LOR
        lor = df_meter_readings.apply(lambda row: calc_lor(row['KVA Rating'], FIXED_PF, FIXED_LOAD, row['KW']), axis=1)
        df_meter_readings['LOR'] = lor
        # Trim off columns
        cols_to_keep = ['index',
                         'DeviceSerial',
                         'Timestamp',
                         'Wh',
                         'kWH',
                         'KW',
                         'gmt_timestamp',
                         'month',
                         'week',
                         'day_of_month',
                         'day_of_week',
                         'time',
                         'timeslot_mean',
                         'timeslot_median',
                         'Wh_Outlier',
                         'KVA Rating',
                         'EOR',
                         'LOR']

        df_meter_readings = df_meter_readings[cols_to_keep]

        # Dump the result to an excel file named for the serial number
        x = df_meter_readings.values.tolist()
       
        print(x[0])
        from sqlalchemy import update, text
        conn = ENGINE.connect()
        cnt = 0
        res = None
        sql = ''
        executed = False
        for z in x:
            if z[-1] is None: z[-1] = 'null'
            print('XXXXXXXX\n',z[-3:])
            sql += 'UPDATE `%s` SET EOR = %s, LOR = %s WHERE `Timestamp` = %s;' %(serial[0],z[-2],z[-1],z[2])
            cnt += 1
            executed = False
            if cnt % 500 == 0: 
                res = conn.execute(text(sql))
                print('222222',res)
                executed = True
                #####print(res, conn.commit())
            print(cnt)
        if not executed: res = conn.execute(text(sql))
        print('33333',res)
        print(conn.commit())
        df_meter_readings.to_csv(f'LOR-EOR/results/{serial[0]}.csv')
    
        # Print status message
        rt_et = dt.now()
        print(f"{serial} | {rt_et-rt_st} elapsed | success")
        
    except Exception as e:
        rt_et = dt.now()
        
        # Capture the exception and print the error message
        print(f"{serial} | {rt_et-rt_st} elapsed | failure | error: {e}")
        