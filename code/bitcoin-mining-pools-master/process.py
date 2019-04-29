#!/usr/bin/python

from __future__ import print_function
import blocktrail
import sqlite3 as sql
import time


client = blocktrail.APIClient(
             api_key="f618f7367b4d424d2c9af6babb399216cce6254a", 
             api_secret="e36fd54c12acc23067c0627f750d53c768b8f667", 
             network="BTC", testnet=False)

sql_conn = sql.connect('test.sqlite')
sql_cur = sql_conn.cursor()

#sql_cur.execute("DROP TABLE IF EXISTS blocks")
#sql_cur.execute(
#    "CREATE TABLE blocks( "
#    "height INT, hash CHAR(64), nonce INT8, merkleroot CHAR(64), prev_block  CHAR(64)," 
#    "version CHAR(64), difficulty INT8, byte_size INT," 
#    "value INT8, miningpool_name CHAR(128),"
#    "arrival_time CHAR(128), block_time  CHAR(128), confirmations INT,"
#    "miningpool_slug CHAR(128), is_orphan CHAR(16)," 
#    "transactions INT, miningpool_url CHAR(128), next_block CHAR(64))" )
#sql_conn.commit()

def process_blk(blk_id):
    
    sql_cur.execute(  "SELECT * FROM blocks WHERE height = %d "%blk_id )
    
    if sql_cur.fetchone() is not None:
        print( "skipping %.7d..."%blk_id  )
        return False
    
    print("processing %.7d..."%blk_id)
    blk = client.block(blk_id)
    keys = blk.keys()
    values = [ blk[k]  for k in keys ]

    sql_cur.execute( """INSERT INTO blocks( %s ) VALUES ( %s )"""%( ",".join(keys), ",".join(len(keys)*["?"]) ), values )    
    sql_conn.commit()
    return True

    
BLK_OBJECTIVE = 540000
max_blk = 440000

while max_blk <= BLK_OBJECTIVE:
    while True:
        try:
            process_blk(max_blk)
        except:
            print( "sleeping" )
            time.sleep( 15 )
            continue
        break
    max_blk += 1


