import pandas as pd
import os



def load_and_concat_citibike_data(start_year, end_year, root_path):
    dtypes_new = {
        'ride_id': str, 'rideable_type': str, 'start_station_name': str, 
        'start_station_id': str, 'end_station_name': str, 'end_station_id': str, 
        'start_lat': float, 'start_lng': float, 'end_lat': float, 'end_lng': float, 
        'member_casual': str
    }
    dtypes_old = {
        'tripduration': 'int', 
        'start station id': str, 'start station name': str, 'start station latitude': float,
        'start station longitude': float, 'end station id': str, 'end station name': str,
        'end station latitude': float, 'end station longitude': float, 'bikeid': str,
        'usertype': str, 'birth year': 'float', 'gender': int
    }
    parse_dates = ['starttime', 'stoptime']
    all_dataframes = []
    months = ["January", "February", "March", "April", "May", "June",
              "July", "August", "September", "October", "November", "December"]

    for year in range(start_year, end_year + 1):
        year_path = f"{root_path}/{year}-citibike-tripdata"
        year_dataframes = []
        for month_index, month_name in enumerate(months, start=1):
            print(f"Loading data for {month_name} {year}...")
            month_path = f"{year_path}/{month_index}_{month_name}"
            file_index = 1
            while True:
                file_path = f"{month_path}/{year}{month_index:02d}-citibike-tripdata_{file_index}.csv"
                if os.path.exists(file_path):
                    if year < 2021 or (year == 2021 and month_index == 1):
                        df = pd.read_csv(file_path, dtype=dtypes_old, parse_dates=parse_dates)
                        # rename the columns to match the new schema
                        df.rename(columns={
                            'start station id': 'start_station_id',
                            'start station name': 'start_station_name', 'start station latitude': 'start_lat',
                            'start station longitude': 'start_lng', 'end station id': 'end_station_id',
                            'end station name': 'end_station_name', 'end station latitude': 'end_lat',
                            'end station longitude': 'end_lng', 
                            'usertype': 'member_casual', 'starttime': 'started_at', 'stoptime': 'ended_at'
                        }, inplace=True)
                        # remove columns that are not in the new schema
                        df.drop(columns=['tripduration', 'bikeid', 'birth year', 'gender'], inplace=True)
                        df['member_casual'] = df['member_casual'].map({'Subscriber': 'member', 'Customer': 'casual'})
                        #df = df[[col for col in dtypes_new.keys() if col in df.columns]]  # Filter for common columns
                    else:
                        df = pd.read_csv(file_path, dtype=dtypes_new, parse_dates=['started_at', 'ended_at'])
                    year_dataframes.append(df)
                    file_index += 1
                else:
                    break
        # save the combined data for the year
        year_df = pd.concat(year_dataframes, ignore_index=True)
        year_df = year_df.dropna()
        year_df.to_csv(f"{year_path}/combined-citibike-tripdata.csv")

    combined_df = pd.concat(all_dataframes, ignore_index=True)
    combined_df = combined_df.dropna()
    print(f"Combined data has {combined_df.shape[0]} rows and {combined_df.shape[1]} columns.")
    # Calculate daily ride counts
 
    # create total_rides column
    total_rides = combined_df.groupby(combined_df['started_at'].dt.date).size()
    total_rides = total_rides.reset_index()
    total_rides.columns = ['date', 'total_rides']
    total_rides.to_csv(f"{root_path}/combined-daily-ride-counts.csv")

    # Sample 20% of the data (adjust the fraction as needed)
    sampled_df = combined_df.sample(frac=0.2)
    sampled_df.to_csv(f"{root_path}/combined-citibike-tripdata.csv")



# make this file executable from the command line
if __name__ == "__main__":
    import sys
    if len(sys.argv) == 4:
        start_year = int(sys.argv[1])  # Convert start_year to int
        end_year = int(sys.argv[2])    # Convert end_year to int
        root_path = sys.argv[3]        # Keep root_path as string
        load_and_concat_citibike_data(start_year, end_year, root_path)
    else:
        print("Usage: python load_full.py <start_year> <end_year> <root_path>")
# example usage from the command line:
# python load_full.py 2020 2024 "data/citibike-tripdata"

load_and_concat_citibike_data(2020, 2020, "data/citibike-tripdata")