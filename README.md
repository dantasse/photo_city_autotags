Turning the YFCC100M dataset into small files representing a city area. For example, you might want all the yfcc100m Flickr photos in Pittsburgh plus their autotags.

Usage:

    ./flickr_in_city.py --yfcc_file=../yfcc100m_dataset --city=pgh --output_file=flickr_pgh.csv

format of `flickr_pgh.csv`:

    photo_id,username,lat,lon
Then:
    ./add_autotags.py --city_csv_file=flickr_pgh.csv --autotags_file=../yfcc100m_autotags --output_file=flickr_pgh_autotags.csv

format of `flickr_pgh_autotags.csv`:

    photo_id,username,lat,lon,autotags_90plus,autotags
