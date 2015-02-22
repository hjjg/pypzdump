# pypzdump - dump MySQL databases in parallel and use ZFS #

## DISCLAIMER ##
You will lose data if you rely on this script for backing up your
databases. For me, this is just a complement to existing mechanisms.

This code is far from being stable.

## The idea ##
I have backup about 400 databases, each holding about 300
tables. Most of the tables are MyISAM and don't change that often.
Not only do I want to make parallel dumps of tables to speed things up,
I'd also like to dump changed tables only and make use of ZFS snapshots.

1. Try to connect to the database
2. get a global read lock for all databases
3. read master/slave info
4. build an save CHANGE MASTER TO
5. read a list of all database tables
6. generate checksums for all tables
7. check if we have to dump any tables to disk
   -> use a database to save checksums from the previous run?
   -> also save the last dump date and write code to dump at least
      once a week?
8. dump any tables to disk with filename dbname.tablename format
9. clean up old tables (diff tables on disk)
10. take a ZFS snapshot

## TODO ##
 - everything from the above list
 - Clean up logging
 - python3 testing
