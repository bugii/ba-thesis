
import blocktrail
import pickle
import csv
from datetime import datetime
import time
import sys, os
import optparse

'''

Class to get redeeming transactions

Input:
- dictionary of txs of which you want to find the redemption tx 

Output:
- redemption dictionaries stored in block logic

Used to create the pickled-bitcoin-redeem-2_tx, which are used by miner-identification.py

'''

# TODO: process_rest

class RedemptionParser:
    def __init__(self, start, end):
        self.start = start
        self.current = start
        self.end = end
        self.api = blocktrail.APIClient(
            api_key="f618f7367b4d424d2c9af6babb399216cce6254a",
            api_secret="e36fd54c12acc23067c0627f750d53c768b8f667",
            network="BTC", testnet=False, api_version="v1")

        self.redemption = {}

        self.origin_txs = pickle.load(open("Dictionaries/redemption-tx-1-500000.pickle")) # Chose whatever dictionary should be checked
        # for redeems


    def process_all(self):
        while self.current < self.end:
            x = 0
            while x <= 10:
                try:
                    self.process(self.current)
                except:
                    print("sleeping, error: ", sys.exc_info())
                    time.sleep(15)
                    if x == 10:
                        print("couldn't get block %d" % self.current)
                    x += 1
                    continue
                break

            self.current += 1


    def process(self, id):
        self.redemption = {} # Init an empty dict for every block
        print("block %d" %id)
        blk = self.api.block(id)
        total_tx = blk[u'transactions']
        time = datetime.strptime(blk[u'block_time'], "%Y-%m-%dT%H:%M:%S+0000")
        pages_needed = total_tx / 200 + 1
        print (total_tx, pages_needed)

        for page in range(1, pages_needed + 1):
            print("on page %d" %page)
            block = self.api.block_transactions(id, page=page, limit=200)
            for transaction in block[u'data']:
                hash = transaction[u'hash']
                transaction_inputs = transaction[u'inputs']

                for transaction_input in transaction_inputs:
                    previous_hash = transaction_input[u'output_hash'] #get the output hash (referencing output in the input -> origin tx) of every input of a block
                    if previous_hash is not None:
                        if self.origin_txs.has_key(previous_hash): #check origin tx pickle for the previous hash
                            if not hash in self.redemption:
                                self.redemption[hash] = (id, time, [(self.origin_txs[previous_hash][0], previous_hash)]) #to allow for multiple previous hashes / id's for the same redemption tx
                            #found redemption, add to dict with key of the tx hash where it was redeemed
                                print("redeem tx found in blk %d with hash %s" % (id, hash))
                            else:
                                self.redemption[hash][2].append((self.origin_txs[previous_hash][0], previous_hash)) #only appending if redeem tx is already in dict

        if not self.redemption:
            print("no redeeming txs found in blk %d" % id)
            return

        self.save(id)


    def process_rest(self):
        for block in range(self.start, self.end):
            if not os.path.exists("pickled-bitcoin-redeem-2_tx/%.3d/%d.pickle" % (block / 1000, block)):
                x=0
                while x <= 5:
                    try:
                        self.process(block)
                    except:
                        print("sleeping, error: ", sys.exc_info())
                        time.sleep(15)
                        if x == 5:
                            print("still couldn't get block %d" % block)
                        x += 1
                        continue
                    break



    def save(self, id):
        outdir_name = "pickled-bitcoin-redeem-2_tx/%.3d" % (id / 1000)

        if not os.path.exists(outdir_name):
            os.makedirs(outdir_name)

        pickle.dump(self.redemption, open("%s/%d.pickle" % (outdir_name, id), "w"))


def parse_command_line():
    parser = optparse.OptionParser()

    parser.add_option("--period", action='store', dest="period",
                  default='364250, 500000', help="minimum block number to process")

    options, args = parser.parse_args()

    options.period = map(int, options.period.split(","))
    assert len(options.period) == 2

    return options

if __name__ == "__main__":
    options = parse_command_line()
    print options.period
    proc = RedemptionParser(options.period[0], options.period[1])
    #proc.process_all()

    proc.process_rest()

