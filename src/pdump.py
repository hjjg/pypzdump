#!/usr/bin/env python

import MySQLdb

# 1. Try to connect to the database
# 2. get a global read lock for all databases
# 3. read master info
# 4. build an save CHANGE MASTER TO
# 5. read a list of all database tables
# 6. generate checksums for all tables
# 7. check if we have to dump any tables to disk
#    -> use a database to save checksums from the previous run?
#    -> also save the last dump date and write code to dump at least
#       once a week?
# 8. dump any tables to disk with filename dbname.tablename format
# 9. clean up old tables (diff tables on disk)

