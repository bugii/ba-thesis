import pickle
from datetime import datetime
import csv
import os

'''

class GenesisStats:
creates genesis tx dictionary

class RedemptionStats:
input: genesis tx dictionary and pickled-bitcoin-redeem_tx 
output: 2 csv's: redemption-stats, mining-redemption-stats (redemption + corresponding genesis)

'''

class GenesisStats:
    def __init__(self, start, end):
        self.block_dir = "UH-bitcoin-heur_0" #using also addresses not users
        self.start = start
        self.current = start
        self.end = end
        self.genesis_tx = {}


    def load_id(self, id):
        outdir_name = "%s/%.3d"%(self.block_dir, id/1000)
        return pickle.load(open("%s/%d.pickle"%(outdir_name, id), "r"))


    def process_all(self):
        while self.current < self.end:
            self.process(self.current)
            self.current += 1
        self.save()


    def process(self, id):
        block = self.load_id(id)
        self.genesis_tx[block.transactions[0][0]] = [block.block_id, block.transactions[0][4], block.time]
        print ("Genesis tx of block %d added" % block.block_id)


    def save(self):
        with open("genesis-tx-%d-%d.pickle" %(self.start, self.end), "wb") as file:
            pickle.dump(self.genesis_tx, file)

        with open("genesis-tx-%d-%d.csv" % (self.start, self.end), "wb") as file:
            out_file = csv.writer(file)
            out_file.writerow(["Block", "Outputs", "Block Time"])
            for hash in self.genesis_tx:
                out_file.writerow([self.genesis_tx[hash][0], len(self.genesis_tx[hash][1]), self.genesis_tx[hash][2]])


class RedemptionStats(GenesisStats):
    def __init__(self, start, end):
        GenesisStats.__init__(self, start, end)
        self.redeemed_dir = "pickled-bitcoin-redeem_tx"

        self.mining_and_redemption = [["Genesis block", "Genesis outputs", "Genesis block time", "Redeem block", "Redeem block inputs",
                                       "Redeem block outputs", "Redeem block time", "Days to redeem"]]

        self.redemption = [["Redeem block", "Inputs", "Outputs", "Time"]]

        self.genesis_tx = pickle.load(open("genesis-tx-%d-%d.pickle" % (self.start, self.end), "rb"))


    def load_redeem(self, id):
        outdir_name = "%s/%.3d"%(self.redeemed_dir, id/1000)
        if os.path.exists("%s/%d.pickle"%(outdir_name, id)):
            foo, ret, foo = pickle.load(open("%s/%d.pickle"%(outdir_name, id), "r"))
            return ret
        else:
            return None


    def process(self, id):
        redeem_blk = self.load_redeem(id)

        if redeem_blk is None:
            return

        blk = self.load_id(id) #load original block only when a redeem_blk exists -> faster

        for (transaction_hash, transaction_fee, transaction_size, transaction_input,
             transaction_output) in blk.transactions:

            if not redeem_blk.has_key(transaction_hash):
                continue

            self.redemption.append([blk.block_id, len(transaction_input), len(transaction_output), blk.time])

            this_redeem_tx = redeem_blk[transaction_hash]

            for (origin_tx, tx_pos, position_in_input) in this_redeem_tx[2]:
                gen_tx = self.genesis_tx[origin_tx]
                #calculate time to redeem
                time_origin = datetime.strptime(gen_tx[2], '%Y-%m-%d %H:%M:%S')
                time_redeem = datetime.strptime(blk.time, '%Y-%m-%d %H:%M:%S')
                time_to_redeem = (time_redeem - time_origin).days
                self.mining_and_redemption.append([gen_tx[0], len(gen_tx[1]), time_origin, blk.block_id, len(transaction_input),
                                                   len(transaction_output), time_redeem, time_to_redeem])

                print("blk: %d" %id)

    def save(self):
        with open('mining-redemption-stats-%d-%d.csv' %(self.start, self.end), 'wb') as file:
            out_file = csv.writer(file)
            for row in self.mining_and_redemption:
                out_file.writerow(row)

        with open('redemption-stats-%d-%d.csv' %(self.start, self.end), 'wb') as file:
            out_file = csv.writer(file)
            for row in self.redemption:
                out_file.writerow(row)



#proc = GenesisStats(1, 500000)
#proc.process_all()
proc = RedemptionStats(1, 500000)
proc.process_all()
