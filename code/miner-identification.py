import pickle
import csv
import os
import numpy as np
from datetime import datetime, timedelta

'''

Class to detect the miners.

Input:
- genesis pickle dictionary (created with genesis-redemption-stats.py Genesis class)
- redemption pickle dictionary (created with redeem-dict-creator) 
- pickled-bitcoin-redeem_tx data set (dictionary stored in block logic)
- pickled-bitcoin-redeem-2_tx data set (created with redemption-parser.py) (dictionary stored in block logic)

Output:
- Miner csv overview of each case (1-4)
- Miner vector with 1's for miner and 0's for non-miner

'''


class MinerIdentifier:
    def __init__(self, start, end, case, genesis_txs, miningpools, minimise_user_heur_1, minimise_user_heur_2s, minervector, redemption_txs=None):
        self.block_dir = "UH-bitcoin-heur_0"
        self.redeemed_dir = "pickled-bitcoin-redeem_tx"
        self.redeemed_2_dir = "pickled-bitcoin-redeem-2_tx"

        self.case = case
        self.genesis_txs = genesis_txs
        self.miningpools = miningpools
        self.minimise_user_heur_1 = minimise_user_heur_1
        self.minimise_user_heur_2s = minimise_user_heur_2s
        self.minervector = minervector
        self.miners = [["Mined Block", "Time", "Address, User, BTC in", "BTC total", "Redemption Block", "Redemption transaction Hash",
                        "Total Miners", "Mining Pool", "Days to redeem", "Heuristic"]]
        self.redemption_txs = redemption_txs

        self.start = start
        self.current = start
        self.end = end
        self.genesis_req = 10
        self.redeemed_req = 20
        self.redeemed_2_req = 20
        self.redeem_time = timedelta(days=5)


    def load_id(self, id):
        outdir_name = "%s/%.3d"%(self.block_dir, id/1000)
        return pickle.load(open("%s/%d.pickle" % (outdir_name, id), "r"))


    def load_redeem(self, id):
        outdir_name = "%s/%.3d" % (self.redeemed_dir, id/1000)
        if os.path.exists("%s/%d.pickle" % (outdir_name, id)):
            foo, ret, foo = pickle.load(open("%s/%d.pickle" % (outdir_name, id), "r"))
            return ret
        else:
            return None


    def load_redeem_2(self, id):
        outdir_name = "%s/%.3d" % (self.redeemed_2_dir, id / 1000)
        if os.path.exists("%s/%d.pickle" % (outdir_name, id)):
            ret = pickle.load(open("%s/%d.pickle" % (outdir_name, id), "r"))
            return ret
        else:
            return None


    def get_user(self, address): # returns user to fit the heur_2s data
        user1 = self.minimise_user_heur_1[address]
        user2 = self.minimise_user_heur_2s[user1]
        return user2


    def get_miners(self, tx_out):
        tx_total_output = 0
        total_miners = 0
        miners = []

        for recipient in tx_out:
            address, user, tx_output = recipient[0], self.get_user(recipient[0]), recipient[1]
            miners.append((address, user, tx_output))
            self.minervector[user] = 1
            tx_total_output += tx_output
            total_miners += 1

        return miners, tx_total_output, total_miners


    def process_all(self):
        if self.case == 1:
            print("processing case 1")
            self.process_case_1()
            self.save()

        if self.case == 2:
            print("processing case 2")
            while self.current < self.end:
                self.process_case_2(self.current)
                self.current += 1
            self.save()

        if self.case == 3:
            print("processing case 3")
            while self.current < self.end:
                self.process_case_3(self.current)
                self.current += 1
            self.save()

        if self.case == 4:
            print("processing case 4")
            self.process_case_4()
            self.save()

        if self.case == 5:
            print("processing case 5")
            self.process_case_5()
            self.save()

            '''
            while self.current < self.end:
                self.process_case_5(self.current)
                self.current += 1
            self.save()
            '''

    def process_case_1(self):
        txs_redeemed = []

        for hash in self.genesis_txs:
            genesis_tx = self.genesis_txs[hash]
            recipients = genesis_tx[1]
            block = genesis_tx[0]
            time = genesis_tx[2]
            if len(recipients) > self.genesis_req:

                miners, tx_total_output, total_miners = self.get_miners(recipients)

                self.miners.append([block, time, miners, tx_total_output, block, hash, total_miners,
                                    self.miningpools[str(block)], 0, 1])

                txs_redeemed.append(hash)
                print("Miners of blk %d added" % block)

        for origin_tx in txs_redeemed:
            del self.genesis_txs[origin_tx]



    def process_case_2(self, id):
        redeem_txs = self.load_redeem(id) # load the redeeming txs in the blk
        if redeem_txs is None:
            return

        blk = self.load_id(id)

        for (transaction_hash, transaction_fee, transaction_size, transaction_input,
             transaction_output) in blk.transactions:

            if not redeem_txs.has_key(transaction_hash):
                continue

            if len(transaction_output) >= self.redeemed_req:
                redeem_tx = redeem_txs[transaction_hash]
                for (origin_tx, foo, bar) in redeem_tx[2]:
                    if not self.genesis_txs.has_key(origin_tx):
                        continue

                    genesis_tx = self.genesis_txs[origin_tx]
                    time_genesis = datetime.strptime(genesis_tx[2], "%Y-%m-%d %H:%M:%S")
                    time_redeem = datetime.strptime(blk.time, "%Y-%m-%d %H:%M:%S")
                    time_to_redeem = time_redeem - time_genesis

                    if len(genesis_tx[1]) <= 2 and time_to_redeem <= self.redeem_time: # changed to <= 2 to include those special cases in year 2017

                        miners, tx_total_output, total_miners = self.get_miners(transaction_output)

                        self.miners.append([genesis_tx[0], genesis_tx[2], miners, tx_total_output, id, transaction_hash,
                                            total_miners, self.miningpools[str(genesis_tx[0])], time_to_redeem, 2])

                        print("Miners of blk %d added" % genesis_tx[0])
                        del self.genesis_txs[origin_tx]



    def process_case_3(self, id):
        redeem_2_txs = self.load_redeem_2(id)

        if redeem_2_txs is None:
            return

        blk = self.load_id(id)

        for (transaction_hash, transaction_fee, transaction_size, transaction_input,
             transaction_output) in blk.transactions:

            if not redeem_2_txs.has_key(transaction_hash):
                continue

            if len(transaction_output) > self.redeemed_2_req:
                (redeem_2_blk, redeem_2_time, redeem_2_origin) = redeem_2_txs[transaction_hash]

                for (redeem_1_block, redeem_1_hash) in redeem_2_origin:
                    redeem_1_tx = self.redemption_txs[redeem_1_hash]

                    (redeem_1_blk, redeem_1_output, redeem_1_origin) = redeem_1_tx
                    if len(redeem_1_output) < self.redeemed_req:
                        for (genesis_blk, genesis_hash) in redeem_1_origin:
                            if not self.genesis_txs.has_key(genesis_hash):
                                continue

                            genesis_tx = self.genesis_txs[genesis_hash]
                            time_genesis = datetime.strptime(genesis_tx[2], "%Y-%m-%d %H:%M:%S")
                            time_redeem = datetime.strptime(blk.time, "%Y-%m-%d %H:%M:%S")
                            time_to_redeem = time_redeem - time_genesis

                            if len(genesis_tx[1]) <= 2 and time_to_redeem <= self.redeem_time: # changed to <= 2 to include those special cases in year 2017

                                miners, tx_total_output, total_miners = self.get_miners(transaction_output)

                                self.miners.append([genesis_blk, genesis_tx[2], miners, tx_total_output, id, transaction_hash, total_miners,
                                                    self.miningpools[str(genesis_blk)], time_to_redeem, 3])

                                print("Miners of blk %d added" % genesis_blk)
                                del self.genesis_txs[genesis_hash]


    def process_case_4(self):
        txs_redeemed = []

        for genesis_hash in self.genesis_txs: # loop through not redeemed genesis txs with case 1-3
            genesis_tx = self.genesis_txs[genesis_hash]
            recipients = genesis_tx[1]
            block = genesis_tx[0]
            time = genesis_tx[2]
            miners, tx_total_output, total_miners = self.get_miners(recipients)

            if not self.miningpools[str(block)]:  # check if dict value is empty ('') -> no mining pool was involved in the first place
                self.miners.append([block, time, miners, tx_total_output, block, genesis_hash, total_miners,
                                    'no pool', 0, 4])

                txs_redeemed.append(genesis_hash)
                print("Miners of blk %d added" % block)

        for origin_tx in txs_redeemed:
            del self.genesis_txs[origin_tx]


    def process_case_5(self):
       for genesis_hash in self.genesis_txs:
           genesis_tx = self.genesis_txs[genesis_hash]
           time = genesis_tx[2]
           self.miners.append([genesis_tx[0], time, "NA", "NA", "NA", "NA", "NA", self.miningpools[str(genesis_tx[0])], "NA", 5])


    '''
    def process_case_5(self, id):
        
        
        redeem_txs = self.load_redeem(id)  # load the redeeming txs in the blk
        if redeem_txs is None:
            return

        blk = self.load_id(id)

        for (transaction_hash, transaction_fee, transaction_size, transaction_input,
             transaction_output) in blk.transactions:

            if not redeem_txs.has_key(transaction_hash):
                continue

            redeem_tx = redeem_txs[transaction_hash]
            for (origin_tx, foo, bar) in redeem_tx[2]:
                if not self.genesis_txs.has_key(origin_tx):
                    continue

                miners, tx_total_output, total_miners = self.get_miners(transaction_output)
                genesis_tx = self.genesis_txs[origin_tx]
                time_genesis = datetime.strptime(genesis_tx[2], "%Y-%m-%d %H:%M:%S")
                time_redeem = datetime.strptime(blk.time, "%Y-%m-%d %H:%M:%S")
                time_to_redeem = time_redeem - time_genesis

                self.miners.append([genesis_tx[0], miners, tx_total_output, id, transaction_hash, total_miners,
                                    self.miningpools[str(genesis_tx[0])], time_to_redeem, 2])

                print("Miners of blk %d added" % genesis_tx[0])
        '''


    def save(self):
        with open('Miner-csv/case-%s-%d-%d.csv' % (str(self.case), self.start, self.end), 'w') as file:
            outfile = csv.writer(file)
            for row in self.miners:
                outfile.writerow(row)

        with open('Miner-vector/miner-vector-after-case-%s-%d-%d.npy' % (str(self.case), self.start, self.end), 'w') as file:
            np.save(file, self.minervector)

        if not self.case == 5: # not needed for case 5 anymore, because it is empty
            with open('Dictionaries/genesis-tx-after-case-%s-%d-%d.pickle' % (str(self.case), self.start, self.end), 'w') as file:
                pickle.dump(self.genesis_txs, file)

        

# LOADING INITIAL FILES
genesis_txs = pickle.load(open('Dictionaries/genesis-tx-1-500000.pickle'))
miningpools = pickle.load(open('Dictionaries/miningpools-1-500000.pickle'))
minimise_user_heur_1 = np.load(open('UH-bitcoin-heur_1/minimise_user-1_500000.npy'))
minimise_user_heur_2s = np.load(open('UH-bitcoin-heur_2s/minimise_user-1_500000.npy'))
cfg = pickle.load(open('UH-bitcoin-heur_2s/config-1_500000.pickle')) # LOAD FILE FROM USER HEUR_2s TO GET USERS
no_users = cfg["minimised_n_users"] #HAS TO BE THE SAME SIZE AS THE WEALTH VECTORS)
minervector = np.zeros(no_users)


# UPDATING FILES AND EXECUTING CASE 1
#proc1 = MinerIdentifier(1, 500000, 1, genesis_txs, miningpools, minimise_user_heur_1, minimise_user_heur_2s, minervector)
#proc1.process_all()


# UPDATING FILES AND EXECUTING CASE 2
#minervector = np.load('Miner-vector/miner-vector-after-case-1-1-500000.npy')
#genesis_txs = pickle.load(open("Dictionaries/genesis-tx-after-case-1-1-500000.pickle"))
#proc2 = MinerIdentifier(1, 500000, 2, genesis_txs, miningpools, minimise_user_heur_1, minimise_user_heur_2s, minervector)
#proc2.process_all()


# UPDATING FILES AND EXECUTING CASE 3
#minervector = np.load('Miner-vector/miner-vector-after-case-2-1-500000.npy')
#genesis_txs = pickle.load(open("Dictionaries/genesis-tx-after-case-2-1-500000.pickle"))
#redemption_txs = pickle.load(open("Dictionaries/redemption-tx-1-500000.pickle"))
#proc3 = MinerIdentifier(1, 500000, 3, genesis_txs, miningpools, minimise_user_heur_1, minimise_user_heur_2s, minervector, redemption_txs=redemption_txs)
#proc3.process_all()


# UPDATING FILES AND EXECUTING CASE 4
minervector = np.load(open('Miner-vector/miner-vector-after-case-3-1-500000.npy'))
genesis_txs = pickle.load(open("Dictionaries/genesis-tx-after-case-3-1-500000.pickle"))
proc4 = MinerIdentifier(1, 500000, 4, genesis_txs, miningpools, minimise_user_heur_1, minimise_user_heur_2s, minervector)
proc4.process_all()


# UPDATING FILES AND EXECUTING CASE 5
minervector = np.load(open('Miner-vector/miner-vector-after-case-4-1-500000.npy'))
genesis_txs = pickle.load(open("Dictionaries/genesis-tx-after-case-4-1-500000.pickle"))
proc5 = MinerIdentifier(1, 500000, 5, genesis_txs, miningpools, minimise_user_heur_1, minimise_user_heur_2s, minervector)
proc5.process_all()
