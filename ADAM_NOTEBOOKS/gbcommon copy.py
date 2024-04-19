import configparser
from datetime import datetime as dt, timedelta, timezone
import time
import pandas as pd
import pytz  # for timezone handling
import json as js
import requests
from sqlalchemy import create_engine

config = configparser.ConfigParser()
config.read('config.ini')

API_BASE_URL = config['GB_API_V1']['API_BASE_URL'] #"https://api.eyedro.com/customcmd"
USER_KEY = config['GB_API_V1']['USER_KEY'] #"UNHCRMHiYgbHda9cRv4DuPp28DnAnfeV8s6umP5R"
USER_KEY_GET_DATA = config['GB_API_V1']['USER_KEY_GET_DATA'] #"UNHCRp28DnAV8s6uHdMHiYgba95RcRv4DnfeuPmP"
GET_DATA_URL = f"{API_BASE_URL}{config['GB_API_V1']['GET_DATA']}"
GET_DEVICE_LIST_URL = f"{API_BASE_URL}{config['GB_API_V1']['GET_DEVICE_LIST']}{USER_KEY}"

print('EyeDro Endpoint and Key Set')
schema = 'public'
def create_db_engine(yr = 2023):
    global schema
    if yr != 2023:
        schema = 'gb_2024'
        return create_engine(config['GB_2024']['CREATE_ENGINE'], connect_args={'options': '-csearch_path=gb_2024'}), schema
    else:
        schema = 'public'
        return create_engine(config['GB_2023']['CREATE_ENGINE'], connect_args={'options': '-csearch_path=public'}), schema


def dt2utc(dtt):
    if dtt.tzinfo is None or dtt.tzinfo.utcoffset(dtt) is None:
        dtt = dtt.replace(tzinfo=timezone.utc)
    else:
        dtt = dtt.astimezone(timezone.utc)
    return dtt.replace(hour=0, minute=0, second=0, microsecond=0)


# Function to convert Timestamp to epoch time
def pd_timestamp_to_epoch(timestamp):
    return timestamp.timestamp()

# Function to call meter inventory API method
def get_device_inventory_list(test=False):
    url = GET_DEVICE_LIST_URL
    response = requests.get(url)
    data = response.json()
    serials = [(item['Serial']) for item in data['List']]
    if test:
        for x in serials:
            url = f"{API_BASE_URL}?Cmd=Unhcr.GetDeviceInventoryList&DeviceSerial={x}&UserKey={USER_KEY}"
            response = requests.get(url)
            data = response.json()
            print('XXXXXXXXXXX', x)
            if data['Errors'] == []:
                #print(data['List'])
                #print('YYYYYYYYYY', x)
                pass
            else:
                print('EEEEEE',data['Errors'])
    return serials

print('function created: get_device_inventory_list')


# Function to call meter GetData API method
def eyedro_getdata(serial, timestamp):
    
    '''
    This function takes as its input a meter serial number and an epoch timestamp and calls the GetData API to 
    retrieve the prior day's readings (96 steps at 15-minute intervals). It returns the response as JSON text
    '''
    meter_url = f"{GET_DATA_URL}{str(serial)}&DateStartSecUtc={str(timestamp)}&DateNumSteps=96&UserKey={USER_KEY_GET_DATA}"
    response = requests.get(meter_url, timeout=600)
    return js.loads(response.text)

print('function created: eyedro_getdata')

def get_midnight_epoch_timestamps(sd):
    try:
        # Get the current date
        current_date = dt2utc(dt.now())

        # Calculate the start date, which is 'past_months' months before the current date
        # Assuming 30 days per month for simplicity
        ###start_date = current_date - timedelta(days=30.437 * past_months) # average 30.437 days per month

        start_date =  dt2utc(sd)

        # List to store the midnight epoch timestamps
        midnight_timestamps = []

        # Loop over each day from the start date to the current date
        while start_date <= current_date:
            # Create a datetime object for midnight of the current date
            midnight = dt2utc(dt(start_date.year, start_date.month, start_date.day))

            # Convert the datetime object to an epoch timestamp and add it to the list
            epoch_timestamp = int(midnight.timestamp()) ###int(time.mktime(midnight.timetuple()))
            midnight_timestamps.append(epoch_timestamp)

            # Move to the next day
            start_date += timedelta(days=1)
        print('ZZZZZZ')
        return midnight_timestamps
    except Exception as e:
        print("MD EEEEE")
        traceback.print_exc()
        exit(123)

print('function created: get_midnight_epoch_timestamps')

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
    #smh df['Timestamp'] = df['Timestamp'].astype('Int64') // 10**9
    df['Timestamp'] = df['Timestamp'].apply(pd_timestamp_to_epoch).astype('int32')

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

def trim_dataframe(input_df):
    # Sort the DataFrame by the "Timestamp" column
    input_df.sort_values(by='Timestamp', inplace=True)

    # Reset the index
    input_df.reset_index(drop=True, inplace=True)

    # Find the index of the first non-zero value in the "Wh" column
    first_non_zero_index = input_df['Wh'].gt(0).idxmax()

    # Extract the trimmed DataFrame
    trimmed_df = input_df.loc[first_non_zero_index:]

    return trimmed_df

print('function created: trim_dataframe')


#=============================================================
#=============================================================
#=============================================================

def process_new_gb(all_rows, serial, ENGINE):
    # Create a DataFrame from the API response which we will add to the existing data
    df_new_data = pd.DataFrame(all_rows)
    print('DDDDD')
    # Scan for missing 15-minute increments and fill gaps in the data with 0-Wh readings
    df_new_data = fill_missing_timestamps(df_new_data)
    print('EEEEE')
    # Parse date and time info out of the timestamps
    parsed_timestamps = df_new_data['Timestamp'].apply(parse_timestamp)
    print('FFFF')
    # Add the parsed data back into the DataFrame
    df_new_data = df_new_data.join(pd.json_normalize(parsed_timestamps))

    # Sort the DataFrame by 'Wh' in descending order to put non-zero Wh values first (for use later in dropping duplicates)
    df_new_data = df_new_data.sort_values(by=['Wh'], ascending=False)

    # Drop duplicates based on 'Timestamp' and keep the first occurrence
    df_new_data = df_new_data.drop_duplicates(subset=['Timestamp'], keep='first')

    # Re-sort the dataframe by timestamp
    df_new_data = df_new_data.sort_values(by=['Timestamp'], ascending=True)

    parsed_timestamps = df_new_data['Timestamp'].apply(parse_timestamp)
    df_new_data = df_new_data.join(pd.json_normalize(parsed_timestamps))

    # Impute means and medians and create imputed value columns for later use
    df_new_data = impute_and_summarize(df_new_data)

    # Trim resulting dataframe such that any unnecessary 0's at beginning of dataset are removed 
    # This is necessary since not all meters have been online for full 12 months of data period covered by update
    df_new_data = trim_dataframe(df_new_data)

    # Load resulting update to SQL
    res = df_new_data.to_sql(serial, ENGINE, schema=schema, if_exists='replace')
    return df_new_data

def process_existing_gb(all_rows, df_temp, serial, ENGINE):
    st = time.perf_counter()
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
    df_updated.to_sql(serial, ENGINE, schema=schema, if_exists='replace')
    print(f'process_existing_gb: {serial} process secs: {sql_st - st} sql secs: {time.perf_counter() - sql_st}')
    return df_updated


"""
009004E2
00980824
009004E6
00980891
00980A03
00980A04
00980B2E


"""
