import gbcommon as gb
import time
import pandas as pd
import datetime
from datetime import datetime as dt, timedelta, timezone
print('Libraries Imported')

# Set Global Variables
TS_DIFF = 24 * 60 * 60  # only update if more than 1 day of data missing

# currently not used
S_IDX = 0
E_IDX = 100
print(f'Starting and ending indices set, processing meters {S_IDX} to {E_IDX -1}')

ENGINE, schema = gb.create_db_engine(yr=2024)


print('SQL Connection String Created')

# Reference the below view which calls the SQL database to list the table names for the meter tables
# This is our list of meters which already have data which we will then update

# -- vw_table_list source
# CREATE OR REPLACE VIEW vw_table_list
# AS SELECT table_name
# FROM information_schema.tables
# WHERE table

L_SQL_SERIALS = set()
try:
    L_SQL_SERIALS = set(pd.read_sql_query(f"SELECT table_name FROM {schema}.vw_table_list;",con=ENGINE).table_name.values)
except Exception as e:
    print('EEEEEEEEEEEEEE',e)
    pass
print('List of existing meter serials in SQL database gathered')

idx = len(L_SQL_SERIALS)
#for serial in L_SQ(L_SERIALS[S_IDX:E_IDX]:
print('AAAAAAA meters', len(L_SQL_SERIALS))
perf_st = time.perf_counter()
for serial in sorted(L_SQL_SERIALS):

    # if idx <= 130:     # to pickup if there's an interruption or failure
    #     idx -=1
    #     continue


    rt_st = dt.now()
    serial_st = time.perf_counter()
    # if idx < 560:
    #     break

    try:
        # Load the data from the existing table
        df_temp = pd.read_sql_query(f'select * from {schema}."{serial}"',con=ENGINE)

        # Trim off columns in preparation to append new data
        cols_to_keep = ['DeviceSerial','Timestamp','Wh']
        df_temp = df_temp[cols_to_keep]

        # Find the maximum timestamp in the "Timestamp" column
        max_timestamp = df_temp['Timestamp'].max()

        # Calculate the current timestamp in UTC for the time when the script is run
        current_timestamp = int(dt.now(timezone.utc).timestamp())

        if current_timestamp - max_timestamp < TS_DIFF:
            serial_et = time.perf_counter()
            idx -= 1
            print(idx, "{:.2f}".format(serial_et - serial_st), serial)
            continue

        # Create a list of midnight timestamps between max_timestamp and current_timestamp to pass to the API call
        midnight_timestamps = []
        max_date = dt.fromtimestamp(max_timestamp, timezone.utc) #
        midnight = dt(max_date.year, max_date.month, max_date.day, 0, 0, 0, tzinfo=timezone.utc)

        ts = max_timestamp
        midnight_timestamps.append(ts)
        ts = midnight.timestamp()
        ts += 24 * 60 * 60
        while ts <= current_timestamp:
            midnight_timestamps.append(int(ts))
            ts += 24 * 60 * 60

        # while midnight.timestamp() <= current_timestamp:
        #     midnight_timestamps.append(int(midnight.timestamp()))
        #     midnight += timedelta(days=1)

        # Create list to hold responses to the API calls, storing each response as an element in a list
        li_responses = []

        # Call the API to fetch data, skipping if a fatal error is encountered
        for timestamp in midnight_timestamps:
            try:
                li_responses.append(gb.eyedro_getdata(serial, timestamp))
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
        sql_st = time.perf_counter()
        df_updated = gb.process_existing_gb(all_rows, df_temp, serial, ENGINE)
        et = time.perf_counter()
        # Print status message
        rt_et = dt.now()
        sqlet = "{:.2f}".format(et-sql_st)
        serialet = "{:.2f}".format(et-serial_st)
        print(f"{serial} | {rt_et-rt_st} elapsed | success | Rows Loaded: {len(df_updated)} cnt: {idx} sql time: {sqlet} serial time: {serialet}")
        idx-=1

    except Exception as e:
        idx-=1
        rt_et = dt.now()
        et = time.perf_counter()
        #sqlet = "{:.2f}".format(et-sql_st)
        #serialet = "{:.2f}".format(et-serial_st)
        # Capture the exception and print the error message
        print(f"{serial} | {rt_et-rt_st} elapsed | failure | error: {e} ")

ttlet = "{:.2f}".format((time.perf_counter() - perf_st)/60)
print(f"total time | {ttlet} elapsed mins | cnt: {len(L_SQL_SERIALS) - idx} ")
