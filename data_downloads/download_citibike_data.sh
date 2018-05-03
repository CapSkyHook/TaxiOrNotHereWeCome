cat citibike_urls.txt | xargs -n 1 -P 6 wget -c -P data/citibike_data/
unzip ./data/citibike_data/* -d ./data/citibike_data/