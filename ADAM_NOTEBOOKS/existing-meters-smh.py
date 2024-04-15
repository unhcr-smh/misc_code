import time
import pandas as pd
import json as js
import requests
from sqlalchemy import create_engine
from datetime import datetime as dt, timedelta, timezone
import pytz  # for timezone handling
print('Libraries Imported')

# Set Global Variables
S_IDX = 0
E_IDX = 100
TS_DIFF = 1#24 * 60*60  # 1 day

print(f'Starting and ending indices set, processing meters {S_IDX} to {E_IDX -1}')

### ENGINE = create_engine('postgresql://postgres:4raxeGo5xgB@localhost:5432/eyedro_meters')
ENGINE = create_engine('postgresql://avnadmin:AVNS_zSrniBsHGVQSqhqunlJ@pg-unhcr-unhcr-007.a.aivencloud.com:15602/defaultdb', 
                       connect_args={'options': '-csearch_path=gb_2024'})

print('SQL Connection String Created')

API_BASE_URL = "https://api.eyedro.com/customcmd"
USER_KEY = "UNHCRMHiYgbHda9cRv4DuPp28DnAnfeV8s6umP5R"
USER_KEY_GET_DATA = "UNHCRp28DnAV8s6uHdMHiYgba95RcRv4DnfeuPmP"
print('EyeDro Endpoint and Key Set')


# Reference the below view which calls the SQL database to list the table names for the meter tables
# This is our list of meters which already have data which we will then update

# -- gb_2024.vw_table_list source


# CREATE OR REPLACE VIEW gb_2024.vw_table_list
# AS SELECT table_name 
# FROM information_schema.tables 
# WHERE table

###L_SQL_SERIALS = pd.read_sql_query("select * from gb_2024.vw_table_list;",con=ENGINE).serial_num.to_list()
L_SQL_SERIALS = set()
try:
    L_SQL_SERIALS = set(pd.read_sql_query("SELECT table_name FROM gb_2024.vw_table_list;",con=ENGINE).table_name.values)
    #s_sql_serials = set(pd.read_sql_query("SELECT table_name FROM gb_2024.vw_table_list;",con=ENGINE).serial_num.to_list())
except Exception as e:
    print('EEEEEEEEEEEEEE',e)
    pass
print('List of existing meter serials in SQL database gathered')

# Define functions for various operations in the script

def dt2utc(dtt):
    if dtt.tzinfo is None or dtt.tzinfo.utcoffset(dtt) is None:
        dtt = dtt.replace(tzinfo=timezone.utc)
    else:
        dtt = dtt.astimezone(timezone.utc)
    return dtt.replace(hour=0, minute=0, second=0, microsecond=0)

# Function to convert Timestamp to epoch time
def pd_timestamp_to_epoch(timestamp):
    return timestamp.timestamp()

def eyedro_getdata(serial, timestamp):
    
    '''
    This function takes as its input a meter serial number and an epoch timestamp and calls the GetData API to 
    retrieve the prior day's readings (96 steps at 15-minute intervals). It returns the response as JSON text
    '''
    meter_url = "https://api.eyedro.com/customcmd?Cmd=Unhcr.GetData&DeviceSerial=" + str(serial) + "&DateStartSecUtc=" + str(timestamp) + f"&DateNumSteps=96&UserKey={USER_KEY_GET_DATA}"
    response = requests.get(meter_url, timeout=600)
    return js.loads(response.text)

print('function created: eyedro_getdata')

def parse_timestamp(timestamp):
    '''
    Function to parse out date and time information from timestamp for later use (feature engineering)
    '''
    ts = dt.utcfromtimestamp(timestamp).replace(tzinfo=pytz.utc)

    # Extract various components
    gmt_timestamp = ts.isoformat()
    year = ts.year
    month = ts.month
    week = ts.isocalendar()[1]  # Week number of the year
    day_of_month = ts.day
    day_of_week = ts.strftime('%A').lower()  # Full weekday name in lowercase
    hour = ts.hour
    minute = ts.minute
    time = ts.strftime('%H:%M')

    return {
        'gmt_timestamp': gmt_timestamp,
        'year': year,
        'month': month,
        'week': week,
        'day_of_month': day_of_month,
        'day_of_week': day_of_week,
        'hour': hour,
        'minute': minute,
        'time': time
    }

print('function created: parse_timestamp')

def fill_missing_timestamps(df):
    '''
    Function to scan for missing timestamps and synthetically create 0-value Wh readings to fill these gaps
    '''
    # Convert the "Timestamp" column to a datetime object
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')

    # Find the minimum and maximum timestamps in the dataframe
    min_timestamp = df['Timestamp'].min()
    max_timestamp = df['Timestamp'].max()

    # Generate a list of expected timestamps at 15-minute intervals
    expected_timestamps = pd.date_range(start=min_timestamp, end=max_timestamp, freq='15min')

    # Identify missing timestamps
    missing_timestamps = expected_timestamps[~expected_timestamps.isin(df['Timestamp'])]

    # Create new rows for missing timestamps with 0 in the "Wh" column
    missing_data = pd.DataFrame({
        'Timestamp': missing_timestamps,
        'DeviceSerial': df['DeviceSerial'].iloc[0],  # Assuming all rows have the same serial number
        'Wh': 0
    })

    # Concatenate the missing data with the original dataframe
    df = pd.concat([df, missing_data])

    # Sort the dataframe by timestamp
    df.sort_values(by='Timestamp', inplace=True)

    # Convert the timestamp back to epoch format
    df['Timestamp'] = df['Timestamp'].apply(pd_timestamp_to_epoch).astype('int32') #####.astype('int64').astype('int32') // 10**9  ###astype('int64').astype(dtype)  ###

    return df

print('function created: fill_missing_timestamps')

def impute_and_summarize(df):
    # Calculate the mean Wh value for each timeslot of each day of the week (only non-zero values)
    df['timeslot_mean'] = df.groupby(['day_of_week', 'time'])['Wh'].transform(lambda x: x[x > 0].mean())

    # Calculate the median Wh value for each timeslot of each day of the week (only non-zero values)
    df['timeslot_median'] = df.groupby(['day_of_week', 'time'])['Wh'].transform(lambda x: x[x > 0].median())

    # Create "imputed_mean" column based on conditions
    df['imputed_mean'] = df.apply(lambda row: row['timeslot_mean'] if row['Wh'] == 0 else row['Wh'], axis=1)

    # Create "imputed_median" column based on conditions
    df['imputed_median'] = df.apply(lambda row: row['timeslot_median'] if row['Wh'] == 0 else row['Wh'], axis=1)

    # Create a boolean column to indicate when the calculated value was used
    df['calculated_used'] = df['Wh'] == 0

    return df

print('function created: impute_and_summarize')

#===============================================
#===============================================
#===============================================
idx = len(L_SQL_SERIALS)
#for serial in L_SQ(L_SERIALS[S_IDX:E_IDX]:
print('AAAAAAA meters', len(L_SQL_SERIALS))
perf_st = time.perf_counter()
for serial in sorted(L_SQL_SERIALS):
    
    rt_st = dt.now()
    serial_st = time.perf_counter()
    # if idx < 560:
    #     break
    
    try:
        # Load the data from the existing table
        df_temp = pd.read_sql_query(f'select * from "{serial}"',con=ENGINE)

        # Trim off columns in preparation to append new data
        cols_to_keep = ['DeviceSerial','Timestamp','Wh']
        df_temp = df_temp[cols_to_keep]

        # Find the maximum timestamp in the "Timestamp" column
        max_timestamp = df_temp['Timestamp'].max()

        # Calculate the current timestamp in UTC for the time when the script is run
        current_timestamp = int(dt.now(timezone.utc).timestamp())

        # Create a list of midnight timestamps between max_timestamp and current_timestamp to pass to the API call
        midnight_timestamps = []
        current_date = dt2utc(dt.now()) ###dt.utcfromtimestamp(max_timestamp).date()
        midnight = current_date ###dt(current_date.year, current_date.month, current_date.day, 0, 0, 0, tzinfo=timezone.utc)

        while midnight.timestamp() <= current_timestamp:
            midnight_timestamps.append(int(midnight.timestamp()))
            midnight += timedelta(days=1)

        if current_timestamp - max_timestamp < TS_DIFF:
            serial_et = time.perf_counter()
            idx -= 1
            print(idx, "{:.2f}".format(serial_et - serial_st))
            continue
        # Create list to hold responses to the API calls, storing each response as an element in a list
        li_responses = []

        # Call the API to fetch data, skipping if a fatal error is encountered
        for timestamp in midnight_timestamps:
            try:
                li_responses.append(eyedro_getdata(serial, timestamp))
            except:
                pass

        # Prepare an empty list to hold all rows of the final DataFrame
        all_rows = []

        # Iterate over each response in the list of responses and format into a dataframe
        for data in li_responses:
            header_info = {
                'DeviceSerial': data['DeviceSerial']
            }

            for reading in data['Data']['Wh'][0]:
                timestamp, meter_reading = reading
                row = {**header_info, 'Timestamp': timestamp, 'Wh': meter_reading}

                # Add the combined information to the list
                all_rows.append(row)

        # Create a DataFrame from the API response which we will add to the existing data
        df_new_data = pd.DataFrame(all_rows)

        # Add the newly-fetched meter readings to the existing data
        df_updated = pd.concat([df_temp, df_new_data], ignore_index=True)

        # Reset the index to create a new sequential index
        df_updated = df_updated.reset_index(drop=True)

        # Scan for missing 15-minute increments and fill gaps in the data with 0-Wh readings
        df_updated = fill_missing_timestamps(df_updated)

        # Parse date and time info out of the timestamps
        parsed_timestamps = df_updated['Timestamp'].apply(parse_timestamp)

        # Add the parsed data back into the DataFrame
        df_updated = df_updated.join(pd.json_normalize(parsed_timestamps))

        # Sort the DataFrame by 'Wh' in descending order to put non-zero Wh values first (for use later in dropping duplicates)
        df_updated = df_updated.sort_values(by=['Wh'], ascending=False)

        # Drop duplicates based on 'Timestamp' and keep the first occurrence
        df_updated = df_updated.drop_duplicates(subset=['Timestamp'], keep='first')

        # Re-sort the dataframe by timestamp
        df_updated = df_updated.sort_values(by=['Timestamp'], ascending=True)

        # Impute means and medians and create imputed value columns for later use
        df_updated = impute_and_summarize(df_updated)

        sql_st = time.perf_counter()
        # Load resulting update to SQL
        df_updated.to_sql(f"{serial}", ENGINE, if_exists='replace')
        et = time.perf_counter()
        # Print status message
        rt_et = dt.now()
        sqlet = "{:.2f}".format(et-sql_st)
        serialet = "{:.2f}".format(et-serial_st)
        print(f"{serial} | {rt_et-rt_st} elapsed | success | Rows Loaded: {len(df_updated)} cnt: {idx} sql time: {sqlet} serial time: {serialet}")
        idx-=1

    except Exception as e:
        rt_et = dt.now()
        et = time.perf_counter()
        sqlet = "{:.2f}".format(et-sql_st)
        serialet = "{:.2f}".format(et-serial_st)
        # Capture the exception and print the error message
        print(f"{serial} | {rt_et-rt_st} elapsed | failure | error: {e}   sql time: {sqlet} serial time: {serialet}")

ttlet = "{:.2f}".format((time.perf_counter() - perf_st)/60)
print(f"total time | {ttlet} elapsed mins | cnt: {len(L_SQL_SERIALS) - idx} ")
