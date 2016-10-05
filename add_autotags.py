#!/usr/bin/env python

# TODO

import argparse, csv, multiprocessing, os, signal, time
parser = argparse.ArgumentParser()
parser.add_argument('--city_csv_file', default='yfcc100m_in_city.csv')
parser.add_argument('--autotags_file', default='../yfcc100m_autotags')
parser.add_argument('--output_file', default='yfcc100m_in_city_autotagged.csv')
parser.add_argument('--num_processes', type=int, default=multiprocessing.cpu_count())
args = parser.parse_args()

csv.field_size_limit(200*1000) # There are some big fields in YFCC100M.

def init_process(q):
    signal.signal(signal.SIGINT, signal.SIG_IGN) # Ignore SIGINT for ctrl-c.
    process_some_rows.output_queue = q

# This time we're processing rows of the Autotags file.
def process_some_rows(start_point, end_point, photos):
    autotags_file = open(args.autotags_file)
    autotags_file.seek(start_point)
    
    # This block is to clear out whatever partial line you're on.
    if start_point != 0:
        autotags_file.seek(-1, 1) # , 1 means "relative to current location."
        if autotags_file.read(1) != '\n':
            autotags_file.readline() # and ignore it.

    while True:
        row = autotags_file.readline().split('\t')
        if row == ['']:
            process_some_rows.output_queue.put(None)
            break # Past the end of the file.
        photo_id = int(row[0])
        if photo_id in photos:
            photo = photos[photo_id]
            autotags_str = row[1].strip()
            autotags_list = autotags_str.split(',')
            if autotags_list == ['']:
                autotags_list = [] # Avoid out-of-range bugs
            autotags_90plus = [tagprob.split(':')[0] for tagprob in autotags_list \
                    if float(tagprob.split(':')[1]) > 0.9]
            photo.append(autotags_90plus)
            photo.append(autotags_str)
            process_some_rows.output_queue.put(photo)
        
        if autotags_file.tell() > end_point:
            process_some_rows.output_queue.put(None)
            break
 
if __name__ == '__main__':
    print "%s\tStarting." % time.asctime()
    city_csv_reader = csv.reader(open(args.city_csv_file))
    photos = {int(row[0]): row for row in city_csv_reader}

    file_size = os.path.getsize(args.autotags_file)
    start_indices = [i * file_size / args.num_processes for i in range(args.num_processes)]
    end_indices = start_indices[1:] + [file_size]

    output_queue = multiprocessing.Queue()
    worker_pool = multiprocessing.Pool(args.num_processes, init_process, (output_queue,))

    print "%s\tDone reading in CSV, starting processing autotags." % time.asctime()
    try:
        results = []
        for i in range(args.num_processes):
            res = worker_pool.apply_async(process_some_rows, (start_indices[i], end_indices[i], photos))
            results.append(res)
        for res in results:
            res.get()

        print "%s\tDone reading autotags, writing output." % time.asctime()
        out_writer = csv.writer(open(args.output_file, 'w'))
        nones = 0
        while nones < args.num_processes:
            line = output_queue.get()
            if line == None:
                nones += 1
            else:
                out_writer.writerow(line)

    except KeyboardInterrupt:
        print "Caught KeyboardInterrupt, terminating workers."
        worker_pool.terminate()
        worker_pool.join()
    else:
        print "Quitting normally."
        worker_pool.close()
        worker_pool.join()

