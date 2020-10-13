from queue import Queue
import threading
from threading import Thread
from time import time
from datetime import datetime, timedelta
import psycopg2
import mmh3 as mmh3
import statistics
import logging
import os
from dotenv import load_dotenv
import argparse

# store processing times of each query in a list
query_ptimes = []

# define logger object to print query results to a file
logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('output.log', 'w+')
logger.addHandler(file_handler)

# load env variables
load_dotenv()

# define new class which is descendent of Thread class
class Query(Thread):
    # initialize empty queue and setup database connection
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        self.db_conn()
    
    # runs the worker
    def run(self):
        while True:
            # worker fetches data from its queue
            host, start_time, end_time = self.queue.get()
            try:
                # worker runs the database query
                run_query(self.cursor, host, start_time, end_time) 
            finally:
                # worker signals the queue about task completion
                self.queue.task_done()

    # establish database connection
    def db_conn(self):
        try:
            self.connection = psycopg2.connect(user = os.getenv('db_user'), host = os.getenv('db_host'), database = os.getenv('db_name'))
            self.cursor = self.connection.cursor()
        except (Exception, psycopg2.Error) as error :
            print ("Error while connecting to PostgreSQL", error)

    # close the database connection at the end
    def __del__(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()


# runs database queries to get max and min cpu usage for a host every min in a given time period
def run_query(cursor, host, start_time, end_time):
    # convert string to datetime objects
    st = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

    print(threading.current_thread().getName()+" is processing "+host)
    
    # runs every min in a time period specified by start time and end time
    while st + timedelta(seconds=60) <= end_time:
        et = st + timedelta(seconds=60)
        
        # executing query and recording processing time
        curr = time()
        cursor.execute("SELECT MAX(usage), MIN(usage) FROM cpu_usage WHERE ts >= %s and ts <= %s and host = %s;", (st, et, host))
        cpu_usage = cursor.fetchone()
        query_ptimes.append(time()-curr)
        
        # storing query results in a dictionary and printing to the output file
        res = {}
        res["worker"] = threading.current_thread().getName()
        res["host"] = host
        res["start_time"] = st
        res["end_time"] = et
        res["max_cpu_usage"] = cpu_usage[0]
        res["min_cpu_usage"] = cpu_usage[1]
        logger.info(res)

        st = et


# hash function to generate host to worker mappings with below constraints: 
# each host is executed by same worker everytime and 
# each worker can execute multiple hosts
def assignHostToWorker(hostname, num_workers):
    return mmh3.hash(hostname) % num_workers


# Get user inputs (query parameters in a csv file, number of concurrent workers) from command line
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query_inp_file", "-q", required=True, help="query params csv file")
    parser.add_argument("--num_workers", "-n", required=True, help="number of concurrent workers")
    return parser.parse_args()


# Main function starts here
def main():
    args = get_args()
    query_inp_file = args.query_inp_file
    num_workers = int(args.num_workers)

    # prepare the data needed for processing
    with open(query_inp_file) as f:
        li = [line.strip() for line in f]
        query_li = [line.split(',') for line in li]

    # save host to worker mappings in a dicitionary
    takenLi = []
    mappings = {}
    for q in query_li[1:]:
        if q[0] not in takenLi:
            mappings[q[0]] = assignHostToWorker(q[0], num_workers)
            takenLi.append(q[0])

    # create queue for each worker and start the workers
    queueLi = []
    for i in range(num_workers):
        queue = Queue()
        worker = Query(queue)
        worker.daemon = True
        worker.start()
        queueLi.append(queue) 

    # add tasks in the respective queues of workers 
    for q in query_li[1:]:
        queue_num = mappings[q[0]]
        queueLi[queue_num].put([q[0], q[1], q[2]])

    # main thread waiting until all queues finish processing
    for i in range(num_workers):
        queueLi[i].join()

    # query performance statistics
    print("==========================================================")
    print("Total number of queries", len(query_ptimes))
    print("Total processing time of all queries", sum(query_ptimes))
    print("Minimum query time", min(query_ptimes))
    print("Maximum query time", max(query_ptimes))
    print("Average query time", sum(query_ptimes)/len(query_ptimes))
    print("Median query time", statistics.median(query_ptimes))
    print("==========================================================")
    print("Please refer `output.log` file for more details on query results")
    

# run main program
if __name__ == '__main__':
    main()