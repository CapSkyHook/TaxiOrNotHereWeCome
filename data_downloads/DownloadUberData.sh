# Script to download the Uber Data
# Script must be in same folder as "raw_data_urls.txt"
# Script and url list was taken from https://github.com/toddwschneider/nyc-taxi-data

cat raw_uber_data_urls.txt | xargs -n 1 -P 6 wget -c -P data/uber/