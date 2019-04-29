# -*- coding: utf-8 -*-
'''
Created on 11.06.2013
@author: tessonec@ethz.ch
@version: 1.1.0 (29.08.13)
'''
from heuristics import SequenceCalculator
from utils import newline_msg

import time, os.path, os, datetime
import optparse
import dill
import numpy as np


'''

creates balance, inflow, outlfow vectors each month, reset = False

Used by miner-flow.py

'''


class UserDistributionCalculator(SequenceCalculator):

    def __init__(self, options):
        SequenceCalculator.__init__(self, options)

        self.minimum_wealth = options.minimum_wealth

        self.storage_dir = "DISTR-%s-%s-%s" % (self.currency, self.heuristic, self.epoch)
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir )

        self.tx_income = np.zeros(self.minimised_n_users, dtype=np.double)
        self.tx_outcome = np.zeros(self.minimised_n_users, dtype=np.double)
        # self.no_transactions = np.zeros(self.minimised_n_users, dtype=np.int32)

        self.last_time = None
        self.current_time = None

        self.reset = options.reset

    def process(self, blk):
        self.current_time = self.get_time(blk)

        if self.last_time is None:
            self.last_time = self.current_time

        all_transactions = blk.short_transactions

        for (n_tx, (
                transaction_hash, transaction_fee, transaction_size, transaction_input,
                transaction_output)) in enumerate(all_transactions):

            if len(transaction_input) == 0:
                continue

            input_users = set()

            for (user_in, qty) in transaction_input:
                input_users.add(user_in)
                if user_in == 0:
                    continue
                self.tx_outcome[user_in] += qty
                # self.no_transactions[user_in] += 1

            assert len(input_users) == 1, "%s - %s" % (blk.block_id, input_users)

            input_user = input_users.pop()

            for (user_out, qty) in transaction_output:
                self.tx_income[user_out] += qty


    def update_intermediate(self, **kwargs):

        self.last_time = self.current_time

        self.balance = self.tx_income - self.tx_outcome
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        interfix = ""
        if self.reset:
            interfix = "-reset"

        fdistr_name = "%s/balance%s-%.8d.npy" % (self.storage_dir, interfix, kwargs['block_id'])
        newline_msg("OUT", "saving wealth '%s' (sz = %d)" % ( fdistr_name, len(self.balance) ))
        np.save(open(fdistr_name, "w"), self.balance)

        fdistr_name = "%s/inflow%s-%.8d.npy" % (self.storage_dir, interfix, kwargs['block_id'])
        newline_msg("OUT", "saving inflow '%s' (sz = %d)" % (fdistr_name, len(self.balance)))
        np.save(open(fdistr_name, "w"), self.tx_income)

        fdistr_name = "%s/outflow%s-%.8d.npy" % (self.storage_dir, interfix, kwargs['block_id'])
        newline_msg("OUT", "saving outflow '%s' (sz = %d)" % (fdistr_name, len(self.balance)))
        np.save(open(fdistr_name, "w"), self.tx_outcome)

        #fdistr_name = "%s/no_transactions%s-%.8d.npy" % (self.storage_dir,interfix,  kwargs['block_id'])
        #newline_msg("OUT", "saving no_transactions '%s' (sz = %d)" % (fdistr_name, len(self.no_transactions)))
        #np.save(open(fdistr_name, "w"), self.no_transactions)

        if self.reset:
            #self.no_transactions.fill(0)
            self.tx_outcome.fill(0.)
            self.tx_income.fill(0.)


#    , self.no_transactions, self.distance_to_miner


#########################################################################################
#########################################################################################


def parse_command_line():
    parser = optparse.OptionParser()

    parser.add_option("--period", action='store', dest="period",
                      default='1, 500000', help="minimum block number to process")

    parser.add_option("--load-period", action='store', dest="load_period",
                      default=None, help="minimum block number to process")

    parser.add_option("--save-each", type="int", action='store', dest="save_each",
                      default=1, help="saves and updates status every SAVE_EACH")

    parser.add_option("--epoch", action='store', dest="epoch",
                      default='month', help="type of time-aggregation")

    parser.add_option("--heur", action='store', dest='heuristic', default="2s", help="run for given heuristics")

    parser.add_option("--curr", action='store', dest="currency", default='bitcoin',
                      help="which currency is being analysed")

    parser.add_option("--minimum-wealth", type="float", action='store', dest="minimum_wealth",
                      default=0, help="minimum wealth non trimmed")

    parser.add_option("--reset", action='store_true', dest="reset",
                        default=False, help="which currency is being analysed")


    options, args = parser.parse_args()

    options.heuristic = "heur_%s" % options.heuristic

    options.period = map(int, options.period.split(","))
    assert len(options.period) == 2
    [options.min_blk, options.max_blk] = options.period

    if options.load_period is not None:
        options.load_period = map(int, options.load_period.split(","))
    else:
        options.load_period = options.period

    assert len(options.load_period) == 2

    options.log_filename = "LOG-%s-user_distribution-%s.%s.log" % (
    options.currency, options.heuristic, options.epoch)
    return options, args


if __name__ == "__main__":
    options, args = parse_command_line()
    proc = UserDistributionCalculator(options)
    newline_msg("MSG", "processing")
    proc.process_all()

    # proc.update_intermediate(force=True)






