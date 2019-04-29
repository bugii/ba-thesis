from stats import *
import numpy as np
import csv
import glob
from datetime import *
from dateutil.relativedelta import*

'''

Input:
- balance, inflow and outflow vectors (created with build-2-wealth.py), reset monthly in this case
- Miner vector (same length as the above) (created with miner-identification.py)

Output:
- files for plotting monthly median/gini/top1%/top5%/top10%/top20%/top50% of the miner only

'''

# Parameters
min_wealth = 0
case = 4

# read files in directory and create array which helps with loading files
files = glob.glob("DISTR-bitcoin-heur_2s-month/balance-reset-*.npy")
blocks = []
for path in files:
    blocks.append([path[42:50]])

blocks=sorted(blocks) # sort to add the correct month in the next step

month = datetime.strptime("2009-01", "%Y-%m")
for seq in blocks:
    seq.append(month)
    month += relativedelta(months=1)

header = ["Time", "Heur", "Result"]

output = {'balance': [header],
          'inflow': [header],
          'outflow': [header]}

miner = np.load("Miner-vector/miner-vector-after-case-%d-1-500000.npy" % case)


def load_files(seq):
    balance = np.load("DISTR-bitcoin-heur_2s-month/balance-reset-%s.npy" % seq)
    inflow = np.load("DISTR-bitcoin-heur_2s-month/inflow-reset-%s.npy" % seq)
    outflow = np.load("DISTR-bitcoin-heur_2s-month/outflow-reset-%s.npy" % seq)
    return balance, inflow, outflow


def get_percentiles(level, array):
    total = np.sum(array)
    n = len(array)
    array[::-1].sort() # sort array in descending order (in place to save mem)
    return np.sum(array[:int(n * level + 1)]) / float(total)


for seq in blocks:
    print("processing seq: %s" % seq[0])
    balance, inflow, outflow = load_files(seq[0])

    # All calculations in place to safe memory
    # multiply with miner vector to only get miner's values
    balance *= miner
    inflow *= miner
    outflow *= miner

    # remove all values which don't suit min_wealth level (non-miners will be removed automatically, because they have value == 0
    balance = balance[np.where(balance > min_wealth)]
    inflow = inflow[np.where(inflow > min_wealth)]
    outflow = outflow[np.where(outflow > min_wealth)]

    # write block
    results_balance, results_inflow, results_outflow = [], [], []

    # calculate median
    output['balance'].append([seq[1], "Median", np.median(balance)])
    output['inflow'].append([seq[1], "Median", np.median(inflow)])
    output['outflow'].append([seq[1], "Median", np.median(outflow)])

    # calculate gini
    output['balance'].append([seq[1], "Gini", gini_index(balance)])
    output['inflow'].append([seq[1], "Gini", gini_index(inflow)])
    output['outflow'].append([seq[1], "Gini", gini_index(outflow)])

    # calculate percentiles
    levels = [0.01, 0.05, 0.1, 0.2, 0.5]
    for level in levels:
        output['balance'].append([seq[1], "Top %d" % (level * 100) + "%", get_percentiles(level, balance)])
        output['inflow'].append([seq[1], "Top %d" % (level * 100) + "%", get_percentiles(level, inflow)])
        output['outflow'].append([seq[1], "Top %d" % (level * 100) + "%", get_percentiles(level, outflow)])


# write to files
with open("Miner-csv/balance-%d-%.6f.csv" % (case, min_wealth), 'wb') as file:
    outfile = csv.writer(file)
    for row in output['balance']:
        outfile.writerow(row)

with open("Miner-csv/inflow-%d-%.6f.csv" % (case, min_wealth), 'wb') as file:
    outfile = csv.writer(file)
    for row in output['inflow']:
        outfile.writerow(row)

with open("Miner-csv/outflow-%d-%.6f.csv" % (case, min_wealth), 'wb') as file:
    outfile = csv.writer(file)
    for row in output['outflow']:
        outfile.writerow(row)
