#!/bin/bash

for CITY in pgh sf ny houston detroit chicago cleveland seattle miami london minneapolis austin sanantonio dallas
do
    echo "Starting city: $CITY"
    ./flickr_in_city.py --yfcc_file=../yfcc100m_dataset --city=${CITY} --output_file=temp_yfcc100m_${CITY}.csv
    ./add_autotags.py --city_csv_file=temp_yfcc100m_${CITY}.csv --autotags_file=../yfcc100m_autotags --output_file=yfcc100m_${CITY}_autotagged.csv
    rm temp_yfcc100m_${CITY}.csv
    echo "Done with $CITY"
done
