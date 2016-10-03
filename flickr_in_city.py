#!/usr/bin/env python

# Inputs: a YFCC100M file and a city name.
# Output: a csv with photo_id,username,lat,lon

import argparse, csv, multiprocessing, os, signal, util
parser = argparse.ArgumentParser()
parser.add_argument('--yfcc_file', default='yfcc100m_1k.tsv')
parser.add_argument('--city', default='pgh',choices=util.CITY_LOCATIONS.keys())
#['pgh','ny','sf','houston', 'detroit', 'chicago', ...
parser.add_argument('--num_processes', type=int, default=multiprocessing.cpu_count())
args = parser.parse_args()

csv.field_size_limit(200*1000) # There are some big fields in YFCC100M.

def process_some_rows(start_point, end_point):
    infile = open(args.input_file)
    infile.seek(start_point)
    
    # This block is to clear out whatever partial line you're on.
    if start_point == 0:
        pass
    else:
        infile.seek(-1, 1) # , 1 means "relative to current location."
        if infile.read(1) != '\n':
            infile.readline() 

    while True:
        row = infile.readline().split('\t')
        if row == ['']:
            break # Past the end of the file.
        if row[12] == '' or row[13] == '':
            continue # No geotagging on this photo.
        username = row[4]
        lat = float(row[12])
        lon = float(row[13])
        
        # print username, lat, lon
        if infile.tell() > end_point:
            break
 
def main():
    file_size = os.path.getsize(args.input_file)

    start_indices = [i * file_size / args.num_processes for i in range(args.num_processes)]
    end_indices = start_indices[1:] + [file_size]

    # worker_pool = multiprocessing.Pool(args.num_processes, init_worker)
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    worker_pool = multiprocessing.Pool(args.num_processes)
    signal.signal(signal.SIGINT, original_sigint_handler)

    try:
        results = []
        for i in range(args.num_processes):
            res = worker_pool.apply_async(process_some_rows, (start_indices[i], end_indices[i]))
            results.append(res)
        for res in results:
            res.get()

    except KeyboardInterrupt:
        print "Caught KeyboardInterrupt, terminating workers."
        worker_pool.terminate()
        worker_pool.join()
    else:
        print "Quitting normally."
        worker_pool.close()
        worker_pool.join()

if __name__ == '__main__':
    main()

