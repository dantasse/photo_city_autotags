#!/usr/bin/env python

# Inputs: a YFCC100M file and a city name.
# Output: a csv with photo_id,username,lat,lon

import argparse, csv, multiprocessing, os, signal, util
parser = argparse.ArgumentParser()
parser.add_argument('--yfcc_file', default='yfcc100m_1k.tsv')
parser.add_argument('--city', default='pgh',choices=util.CITY_LOCATIONS.keys())
#['pgh','ny','sf','houston', 'detroit', 'chicago', ...
parser.add_argument('--output_file', default='yfcc100m_in_city.csv')
args = parser.parse_args()

csv.field_size_limit(200*1000) # There are some big fields in YFCC100M.

output_file = csv.writer(open(args.output_file, 'w'))
lines_skipped = lines_written = 0

infile = open(args.yfcc_file)
while True:
    row = infile.readline().split('\t')
    if row == ['']:
        break # Past the end of the file.
    if row[12] == '' or row[13] == '':
        lines_skipped += 1
        continue # No geotagging on this photo.
    photo_id = row[1]
    username = row[4]
    lat = float(row[12])
    lon = float(row[13])
    output_file.writerow([photo_id, username, lat, lon])
    lines_written += 1

