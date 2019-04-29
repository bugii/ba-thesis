import csv
import pickle

'''

creates mining pool dictionary with block as key and value = mining pool

Input:
- bitcoin blocks csv

Output:
- pickle dictionary of mining pools

'''


with open('bitcoin-mining-pools-master/bitcoin_blocks.csv', 'r') as file:
    mining = csv.reader(file)
    mining_pools = {}
    for row in mining:
        if row[9] == 'Unknown Entity':
            mining_pools[row[0]] = '' # some blocks without pool have a 'Unknown Entity' value stored instead of the normal ''
        else:
            mining_pools[row[0]] = row[9]


    with open('Dictionaries/miningpools-1-500000.pickle', 'w') as file2:
        pickle.dump(mining_pools, file2)