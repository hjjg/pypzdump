#!/usr/bin/env python
import os
import sys
import time
from Queue import Queue
from thread import allocate_lock
from threading import Thread
import MySQLdb

default_file="./my.cnf"
backuphost="10.39.0.2"
num_threads = 4

def exitfail(message="an error occured", code=255):
    sys.stderr.write(message)
    sys.exit(code)

def log(message):
    print(message)

def dump_table(table):
    pass

def dump_table_worker(i, q, db):
    while True:
        table = q.get()
        
        checksumcursor = db.cursor()
        checksumcursor.execute("CHECKSUM TABLE `%s`.`%s`" % (table[0], table[1]))
        checksum = checksumcursor.fetchone()
        lock.acquire()
        print("worker: ", i+1, table, checksum[1])
        lock.release()
        
        dump_table(table)
        
        q.task_done()


if not os.path.isfile(default_file):
    exitfail("default_file not found: %s" % default_file, 1)

log("Connecting to the source database")
try:
    db_main=MySQLdb.connect(host=backuphost,read_default_file=default_file)
    c=db_main.cursor()
except:
    exitfail("Could not connect to backuphost '%s'" % backuphost)

log("Get a global lock on")
c.execute("FLUSH TABLES WITH READ LOCK")

# this gets me something like this if binlog is enabled:
#    logfile, position, binlog_do_db, binlog_ignore_db
# ('mysql-bin.000001', 107L, '', '')
c.execute("SHOW MASTER STATUS")
master_info=c.fetchone()
if master_info == None:
    exitfail("binlog is not enabled")

# we need to get a lock to print threadinfo nicely
lock = allocate_lock()
tables_queue = Queue()

# set up worker threads
log("Initializing %d worker threads" % num_threads)
for i in range(num_threads):
    # num_threads also determines the number of db connections - 1
    db=MySQLdb.connect(host=backuphost,read_default_file=default_file)
    worker = Thread(target=dump_table_worker, args=(i, tables_queue, db,))
    worker.setDaemon(True)
    worker.start()

log("Get the list of tables")
# Get all the tables from information_schema
c.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE ENGINE IN('MyISAM', 'InnoDB') AND TABLE_SCHEMA NOT IN('information_schema', 'performance_schema') LIMIT 50")
row=c.fetchone()

log("Filling the queue")
# fill up the queue
while row is not None:
    tables_queue.put([row[0], row[1]])
    row = c.fetchone()


# wait till all items in the queue are done
tables_queue.join()
log("All tasks in the queue are done")

# clean up environment
log("Cleaning up")
c.execute("UNLOCK TABLES")
sys.exit(0)
#EOF
