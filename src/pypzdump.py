#!/usr/bin/env python
import os
import sys
import time
import signal
import MySQLdb
import subprocess
import ConfigParser
from Queue import Queue
from threading import Thread,RLock

backup_path = "/tmp/backups"
num_threads = 6
try:
    default_file = sys.argv[1]
except:
    default_file = "./my.cnf"

Config = ConfigParser.ConfigParser()
Config.read(default_file)

def exitfail(message="an error occured and someone was too lazy to catch it", code=127):
    sys.stderr.write(message)
    sys.exit(code)

def log(message):
    print(message)

def signal_handler(signum, frame):
    log("Signal %s received. Exiting.")
    sys.exit()

def dump_table(table):
    global default_file
    cmd = [
        "mysqldump", 
        "--defaults-file=%s" % default_file,
        table[0],
        table[1],
        ]

    filename = "/tmp/backups/%s.%s" % (table[0], table[1])
    with open(filename, "w") as outfile:
        outfile.flush()
        return_code = subprocess.call(cmd, stdout=outfile)
    return return_code

def dump_table_worker(i, q, db):
    while True:
        table = q.get()
        
        checksumcursor = db.cursor()
        checksumcursor.execute("CHECKSUM TABLE `%s`.`%s`" % (table[0], table[1]))
        checksum = checksumcursor.fetchone()
        
        with lock:
            # TODO: check if we already have the table with this checksum
            print("worker: ", i+1, table, checksum[1])
        status = dump_table(table)
        
        # TODO: log status and checksum etc. here
        
        q.task_done()


if not os.path.isfile(default_file):
    exitfail("default_file not found: %s" % default_file, 1)

signal.signal(signal.SIGINT, signal_handler)

log("Connecting to the source database")
try:
    db_main=MySQLdb.connect(read_default_file=default_file)
    c=db_main.cursor()
except:
    exitfail("Could not connect to backuphost")

log("Get a global lock on")
c.execute("FLUSH TABLES WITH READ LOCK")

# this gets me something like this if binlog is enabled:
#    logfile, position, binlog_do_db, binlog_ignore_db
# ('mysql-bin.000001', 107L, '', '')
c.execute("SHOW MASTER STATUS")
master_info=c.fetchone()
c.execute("SHOW SLAVE STATUS")
slave_info=c.fetchone()
if master_info == None and slave_info == None:
    exitfail("binlog is not enabled")

statfile_name = os.path.join(backup_path, "master_slave_status.info")
with open(statfile_name, "w") as statfile:
    statfile.write(str(master_info) + "\n")
    statfile.write(str(slave_info))

# we need to get a lock to print threadinfo nicely
lock = RLock()
tables_queue = Queue()

# set up worker threads
log("Initializing %d worker threads" % num_threads)
for i in range(num_threads):
    # num_threads also determines the number of db connections - 1
    db=MySQLdb.connect(read_default_file=default_file)
    worker = Thread(target=dump_table_worker, args=(i, tables_queue, db,))
    worker.setDaemon(True)
    worker.start()

log("Get the list of tables")
# Get all the tables from information_schema
c.execute(Config.get("pypzdbdump", "select_tables_statement"))
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
