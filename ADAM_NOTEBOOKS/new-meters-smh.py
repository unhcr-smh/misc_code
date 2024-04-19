
import gbcommon as gb
import pandas as pd
from datetime import datetime as dt
import traceback
print('Libraries Imported')

try:
    # Get UTC midnight date
    utc_midnight_date = gb.dt2utc(dt(year=2024, month=4, day=11, hour=0, minute=0, second=0))

    ENGINE, schema = gb.create_db_engine(yr=2024)

    print('SQL Connection String Created')

    # Reference the below view which calls the SQL database to list the table names for the meter tables
    # This is our list of meters which already have data which we will then update

    # -- vw_table_list source
    # CREATE OR REPLACE VIEW vw_table_list
    # AS SELECT table_name 
    # FROM information_schema.tables 
    # WHERE table


    # Gather list of existing meters in SQL database
    s_sql_serials = set()
    s_sql_serials = set(pd.read_sql_query(f"SELECT table_name FROM {schema}.vw_table_list;",con=ENGINE).table_name.values)

    # Gather list of meters from inventory API
    s_api_serials = set(gb.get_device_inventory_list())
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
        # if serial != '00980DDE':
        #     continue

        # these are bad GBs, can not get data
        if serial in ['009004E2',
                '00980824',
                '009004E6',
                '00980891',
                '00980A03',
                '00980A04',
                '00980B2E']:
            continue
        try:
            # Generate the list of midnight epoch timestamps for the past 12 months
            ###midnight_timestamps = get_midnight_epoch_timestamps(12)

            midnight_timestamps = gb.get_midnight_epoch_timestamps(utc_midnight_date)
            print('AAAAAAA', len(midnight_timestamps))

            # Create list to hold responses to the API calls, storing each response as an element in a list
            li_responses = []
            idx = 1
            # Call the API to fetch data, skipping if a fatal error is encountered
            for timestamp in midnight_timestamps:
                try:
                    li_responses.append(gb.eyedro_getdata(serial, timestamp))
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
            df_new_data = gb.process_new_gb(all_rows, serial, ENGINE)

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
except Exception as e:
    print('EEEEEEEEEEEEEE',e)
    traceback.print_exc()
    raise SystemExit(0)
