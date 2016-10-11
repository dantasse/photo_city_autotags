# Edit: this is obsolete
It's now part of the https://github.com/dantasse/neighborhood_guides repository.

Turning the YFCC100M dataset into small files representing a city area. For example, you might want all the yfcc100m Flickr photos in Pittsburgh plus their autotags.

## Input/output
Input of this whole directory: `yfcc100m_dataset` and `yfcc100m_autotags` and a city name (e.g. `pgh`)  
Output: `yfcc100m_pgh_autotagged.csv` with the following columns:

    photo_id,nsid,lat,lon,autotags_90plus,autotags


## More details:
Usage:

    ./flickr_in_city.py --yfcc_file=../yfcc100m_dataset --city=pgh --output_file=yfcc100m_pgh.csv

format of `yfcc100m_pgh.csv`:

    photo_id,nsid,lat,lon
(nsid is a flickr user ID that looks like 12345678@N00). 

Then:
    ./add_autotags.py --city_csv_file=yfcc100m_pgh.csv --autotags_file=../yfcc100m_autotags --output_file=yfcc100m_pgh_autotags.csv

format of `yfcc100m_pgh_autotags.csv`:

    photo_id,nsid,lat,lon,autotags_90plus,autotags

You can also run `make_all_yfcc100m_city_dumps.sh` to just generate all the CSV files for each city.
