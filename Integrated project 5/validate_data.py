#!pip install pytest

weather_df.to_csv('sampled_weather_df.csv', index=False)
field_df.to_csv('sampled_field_df.csv', index=False)

!pytest validate_data.py -v

import os# Define the file paths
weather_csv_path = 'sampled_weather_df.csv'
field_csv_path = 'sampled_field_df.csv'

# Delete sampled_weather_df.csv if it exists
if os.path.exists(weather_csv_path):
    os.remove(weather_csv_path)
    print(f"Deleted {weather_csv_path}")
else:
    print(f"{weather_csv_path} does not exist.")

# Delete sampled_field_df.csv if it exists
if os.path.exists(field_csv_path):
    os.remove(field_csv_path)
    print(f"Deleted {field_csv_path}")
else:
    print(f"{field_csv_path} does not exist.")