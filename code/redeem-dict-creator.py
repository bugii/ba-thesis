import pickle
import os
import csv

'''

Input:
- pickled-bitcoin-redeem_tx

Output:
- redeem tx dictionary (note: Input is already a dictionary, but not stored in one file)

Will be used with genesis-redemption-stats.py

'''


class DictCreator:
    def __init__(self, start, end):
        self.start = start
        self.current = start
        self.end = end
        self.redeemed_dir = "pickled-bitcoin-redeem_tx"
        self.block_dir = "UH-bitcoin-heur_0"
        self.genesis_txs = pickle.load(open('genesis-tx-1-500000.pickle', "r"))

        self.redeem_txs = {}

    def load_redeem(self, id):
        outdir_name = "%s/%.3d" % (self.redeemed_dir, id/1000)
        if os.path.exists("%s/%d.pickle" % (outdir_name, id)):
            foo, ret, foo = pickle.load(open("%s/%d.pickle" % (outdir_name, id), "r"))
            return ret
        else:
            return None


    def load_id(self, id):
        outdir_name = "%s/%.3d"%(self.block_dir, id/1000)
        return pickle.load(open("%s/%d.pickle"%(outdir_name, id), "r"))


    def process_all(self):
        while self.current < self.end:
            self.process(self.current)
            self.current += 1
        self.save()


    def process(self, id):
        redeem_blk = self.load_redeem(id)
        if redeem_blk is None:
            return

        original_blk = self.load_id(id) # load original block for output len

        for (transaction_hash, transaction_fee, transaction_size, transaction_input,
             transaction_output) in original_blk.transactions:

            if not redeem_blk.has_key(transaction_hash):
                continue

            for (origin_hash, foo, bar) in redeem_blk[transaction_hash][2]:
                if not transaction_hash in self.redeem_txs: # Do not overwrite previous dict entries
                    self.redeem_txs[transaction_hash] = [id, transaction_output, [(self.genesis_txs[origin_hash][0], origin_hash)]]
                    print("tx in block %d added" % id)
                else:
                    self.redeem_txs[transaction_hash][2].append((self.genesis_txs[origin_hash][0], origin_hash))


    def save(self):
        with open('redemption-tx-%d-%d.pickle' % (self.start, self.end), 'wb') as file:
            pickle.dump(self.redeem_txs, file)

        with open('redemption-tx-%d-%d.csv' % (self.start, self.end), 'wb') as file:
            outfile = csv.writer(file)
            for key in self.redeem_txs:
                outfile.writerow([key, self.redeem_txs[key]])


proc = DictCreator(1, 500000)
proc.process_all()




