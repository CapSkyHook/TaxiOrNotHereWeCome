# Script to download the Yellow Taxi Data From NYU Open Data
# Script must be in same folder as "raw_raw_green_taxi_data_urls.txt"
# Script and url list was based on script and file from https://github.com/toddwschneider/nyc-taxi-data with slight modifications on which data was taken and where the data is saved

cat raw_green_taxi_data_urls.txt | xargs -n 1 -P 6 wget -c -P data/green-taxi/