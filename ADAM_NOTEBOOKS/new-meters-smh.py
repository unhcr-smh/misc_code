from numpy import dtype
import pandas as pd
import json as js
import requests
from sqlalchemy import create_engine
import datetime
from datetime import datetime as dt, timedelta, timezone
import time
import pytz  # for timezone handling
import traceback
print('Libraries Imported')


def dt2utc(dtt):
    if dtt.tzinfo is None or dtt.tzinfo.utcoffset(dtt) is None:
        dtt = dtt.replace(tzinfo=timezone.utc)
    else:
        dtt = dtt.astimezone(timezone.utc)
    return dtt.replace(hour=0, minute=0, second=0, microsecond=0)

# Get UTC midnight date
utc_midnight_date = dt2utc(dt(year=2024, month=1, day=1, hour=0, minute=0, second=0))

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

# Function to convert Timestamp to epoch time
def pd_timestamp_to_epoch(timestamp):
    return timestamp.timestamp()

# Function to call meter inventory API method
def get_device_inventory_list():
    url = f"{API_BASE_URL}?&Cmd=Unhcr.GetDeviceInventoryList&UserKey={USER_KEY}"
    response = requests.get(url)
    data = response.json()
    serials = [(item['Serial']) for item in data['List']]
    return serials

print('function created: get_device_inventory_list')

# Function to call meter GetData API method
def eyedro_getdata(serial, timestamp):
    
    '''
    This function takes as its input a meter serial number and an epoch timestamp and calls the GetData API to 
    retrieve the prior day's readings (96 steps at 15-minute intervals). It returns the response as JSON text
    '''
    meter_url = "https://api.eyedro.com/customcmd?Cmd=Unhcr.GetData&DeviceSerial=" + str(serial) + "&DateStartSecUtc=" + str(timestamp) + f"&DateNumSteps=96&UserKey={USER_KEY_GET_DATA}"
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


# Gather list of existing meters in SQL database
s_sql_serials = set()
try:
    s_sql_serials = set(pd.read_sql_query("SELECT table_name FROM gb_2024.vw_table_list;",con=ENGINE).table_name.values)
    #s_sql_serials = set(pd.read_sql_query("SELECT table_name FROM gb_2024.vw_table_list;",con=ENGINE).serial_num.to_list())
except Exception as e:
    print('EEEEEEEEEEEEEE',e)
    pass

# Gather list of meters from inventory API
s_api_serials = set(get_device_inventory_list())
print('A')
# Create set of meters to be called which are not already in SQL database
serials_to_call = s_api_serials - s_sql_serials
print('B')
# Convert the set back to a list in case we need to slice it
SERIALS_TO_CALL = sorted(list(serials_to_call))

id = len(SERIALS_TO_CALL)
print(SERIALS_TO_CALL,'!!!!!!!!', id)

for serial in SERIALS_TO_CALL:

    rt_st = dt.now()

    try:

        # Generate the list of midnight epoch timestamps for the past 12 months
        ###midnight_timestamps = get_midnight_epoch_timestamps(12)

        midnight_timestamps = get_midnight_epoch_timestamps(utc_midnight_date)
        print('AAAAAAA', len(midnight_timestamps))

        # Create list to hold responses to the API calls, storing each response as an element in a list
        li_responses = []
        idx = 1
        # Call the API to fetch data, skipping if a fatal error is encountered
        for timestamp in midnight_timestamps:
            try:
                li_responses.append(eyedro_getdata(serial, timestamp))
                idx += 1
                if idx % 10 == 0:
                    print(idx, serial)
            except:
                pass

        # Prepare an empty list to hold all rows of the final DataFrame
        all_rows = []
        for x in li_responses:
            print(li_responses)
        print('BBBB')
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

        print('CCCCCC')
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

        # Impute means and medians and create imputed value columns for later use
        df_new_data = impute_and_summarize(df_new_data)

        # Trim resulting dataframe such that any unnecessary 0's at beginning of dataset are removed 
        # This is necessary since not all meters have been online for full 12 months of data period covered by update
        df_new_data = trim_dataframe(df_new_data)

        # Load resulting update to SQL
        df_new_data.to_sql(f"{serial}", ENGINE, if_exists='replace')

        # Print status message
        rt_et = dt.now()
        print(f"{serial} | {rt_et-rt_st} elapsed | success | Rows Loaded: {len(df_new_data)}")
        id -= 1
        print('records', id)
    except Exception as e:
        rt_et = dt.now()

        # Capture the exception and print the error message
        print(f"{serial} | {rt_et-rt_st} elapsed | failure | error: {e}")
        print("EEEEE")
        traceback.print_exc()
