#!/usr/bin/env python

# Inputs: a YFCC100M file and a city name.
# Output: a csv with photo_id,nsid,lat,lon
# (nsid = a user ID, it usually looks like 12345678@N00)

import argparse, csv, multiprocessing, os, signal, time, util, json
parser = argparse.ArgumentParser()
parser.add_argument('--yfcc_file', default='../yfcc100m_1k.tsv')
parser.add_argument('--city', default='pgh',choices=util.CITY_LOCATIONS.keys())
#['pgh','ny','sf','houston', 'detroit', 'chicago', ...
parser.add_argument('--num_processes', type=int, default=multiprocessing.cpu_count())
parser.add_argument('--output_file', default='yfcc100m_in_city.csv')
args = parser.parse_args()

csv.field_size_limit(200*1000) # There are some big fields in YFCC100M.

def init_process(q):
    # Ignore SIGINT so we can easily ctrl-c.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    process_some_rows.output_queue = q # basically global, whatever
    
def process_some_rows(start_point, end_point):
    infile = open(args.yfcc_file)
    infile.seek(start_point)
    city_bounds = [float(x) for x in util.CITY_LOCATIONS[args.city]['locations'].split(',')]
    
    # This block is to clear out whatever partial line you're on.
    if start_point != 0:
        infile.seek(-1, 1) # , 1 means "relative to current location."
        if infile.read(1) != '\n':
            infile.readline() # and just throw it away; it's a partial line.

    lines_skipped = 0
    while True:
        row = infile.readline().split('\t')
        if row == ['']:
            process_some_rows.output_queue.put(None)
            break # Past the end of the file.

        if row[12] == '' or row[13] == '':
            lines_skipped += 1 # No geotagging on this photo.
        else:
            photo_id = row[1]
            nsid = row[3] # user ID
            lon = float(row[12])
            lat = float(row[13])
            if lat > city_bounds[1] and lat < city_bounds[3] and \
                    lon > city_bounds[0] and lon < city_bounds[2]:
                process_some_rows.output_queue.put([photo_id, nsid, lat, lon])

        if infile.tell() > end_point:
            process_some_rows.output_queue.put(None)
            break
    return lines_skipped
 
if __name__ == '__main__':
    file_size = os.path.getsize(args.yfcc_file)
    start_indices = [i * file_size / args.num_processes for i in range(args.num_processes)]
    end_indices = start_indices[1:] + [file_size]

    output_queue = multiprocessing.Queue()
    print "%s\tStarting processing with %s processes" % (time.asctime(), args.num_processes)
    worker_pool = multiprocessing.Pool(args.num_processes, init_process, (output_queue,))

    try:
        results = []
        lines_skipped = 0
        for i in range(args.num_processes):
            res = worker_pool.apply_async(process_some_rows, (start_indices[i], end_indices[i]))
            results.append(res)
        for res in results:
            lines_skipped += res.get()

        print "%s\tStarting the writer now" % time.asctime()
        output_file = csv.writer(open(args.output_file, 'w'))
        nones = 0 # Listen for N Nones so the processes all close
        writes = 0
        while True:
            line = output_queue.get()
            if line == None:
                nones += 1
                if nones >= args.num_processes:
                    break
            else:
                writes += 1
                output_file.writerow(line)
        
    except KeyboardInterrupt:
        print "Caught KeyboardInterrupt, terminating workers."
        worker_pool.terminate()
        worker_pool.join()
    else:
        print "%s\tQuitting normally." % time.asctime()
        worker_pool.close()
        worker_pool.join()

