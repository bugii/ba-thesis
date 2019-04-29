'''
Created on 27 Aug 2013

@author: tessonec 
'''


import os, os.path, pickle, sys, time
import gzip
import numpy as np

import string

from collections import defaultdict

from block import BlockParser
from utils import newline_msg


MIN_BLOCK=1
MAX_BLOCK=420000

 


class SequentialAnaliser:

    float_digits = string.digits + "."

    extension = ".pickle.gz"        

    def __init__(self, save_each = 1000, file_log = None, ecur = "bitcoin", pickled_dir = "pickled", output_dir = "output"):

        self.pickled_directory = pickled_dir
        self.output_directory = output_dir

        self.currency = ecur
        
        self.save_each = save_each
        
        self.times_processed = 0
        self.total_counter = 0
        
        if file_log is not None:
            self.file_log = open( file_log, "aw" )
        else:
            self.file_log = None        

    def __reduce(self, qty):
        ret = ""
        for ch in qty:
            if ch in self.float_digits:
                ret += ch 
        return ret
        
    def load_id(self, id):
        """Returns a BlockParser object"""
        outdir_name = "%s/%.3d"%(self.pickled_directory, id/1000)
        return pickle.load(open("%s/%d.pickle"%(outdir_name, id), "r"))

    def save_blk(self, blk):
        id_str = str( blk.block_id )
        outdir_name = "%s/%.3d"%(self.output_directory, int(id_str)/1000)
  
        if not os.path.exists(outdir_name):
            os.makedirs(outdir_name)
            
        pickle.dump(blk,open("%s/%s.pickle"%(outdir_name, id_str), "w"))
    
#    def amend_blk(self, blk):
#        """Returns a BlockParser object"""
#        blk.block_id = int(blk.block_id)
#        blk.difficulty = float(blk.difficulty)
#        try:
#            blk.generation_amount = float(blk.generation_amount)
#        except:
#            blk.generation_amount = 0.
#        try:
#            blk.generation_fees = float(blk.generation_fees)
#        except:
#            blk.generation_fees = 0.
#
#        blk.no_transactions = int(blk.no_transactions)
#        blk.total_btc = float(blk.total_btc)
        
#        new_transactions = []
        
#        for ( transaction_hash, transaction_fee, transaction_size, transaction_input,transaction_output ) in blk.transactions:
#            transaction_fee = float(transaction_fee)
#            try:
#                transaction_input = [(addr, float(qty)) for (addr, qty) in transaction_input]
#            except:
#                transaction_input = [(addr, float(self.__reduce(qty))) for (addr, qty) in transaction_input]
#
#            transaction_output = [(addr, float(qty)) for (addr, qty) in transaction_output]
            
#            new_transactions.append( ( transaction_hash, float(transaction_fee), float(transaction_size),transaction_input,transaction_output ) )
        
#        blk.transactions = new_transactions
        
#        self.save_blk(blk)


    def process_all(self, id_min, id_max):
        self.min_blk = id_min
        self.max_blk = id_max

        for id in range(id_min, id_max):
            #try:
            blk = self.load_id(id) # :::~ This loads the first block
            #except:
            #    newline_msg("ERR", "failure loading %d "%id)
            #    continue
          
            self.process(blk)
            
            if self.update_condition(block_id = blk.block_id):
                self.update_intermediate(block_id = blk.block_id, proc = "process_all")
        self.update_intermediate(block_id = id_max)

    def process(self, blk):
    # :::~ function to process a single block 
    # :::~  receives blk block
    # :::~
        pass
    
    
    def update_condition(self, **kwargs):
        return kwargs['block_id'] % self.save_each == 0
        
    def update_intermediate(self, **kwargs):
        additional = ""
        if kwargs.has_key("proc"):
            additional = kwargs["proc"]
    # :::~ function called every save_each
        newline_msg("MSG", "%s block_id = %s .... %d "%(additional, kwargs["block_id"], self.total_counter) )
        if self.file_log:
            newline_msg("MSG", "%s block_id = %s .... %d "%(additional, kwargs["block_id"], self.total_counter), stream = self.file_log )
            self.file_log.flush()
            
        self.times_processed = 0




#########################################################################################################
#########################################################################################################
#########################################################################################################

############### :::~ CT: The SequentialProcessor class

#########################################################################################################
#########################################################################################################
#########################################################################################################



class SequentialProcessor(SequentialAnaliser):
    '''
        Class that allows for sequential processi
    '''
    
        
    def __init__(self, save_each = 1000, file_log = None, ecur = None, pickled_dir = "pickled", output_dir = "output", max_correspondences = 50000):
        
        SequentialAnaliser.__init__(self, save_each, file_log , ecur = ecur,
                                    pickled_dir=pickled_dir, output_dir=output_dir)



    def process_all(self, id_min, id_max):
        self.max_blk = id_max
        self.min_blk = id_min

        for id in range(id_min, id_max):

            blk = self.load_id(id)  # :::~ This loads the first block
            self.process(blk)

            if self.update_condition(block_id=blk.block_id):
                self.update_intermediate(block_id=blk.block_id, proc="process_all")

#            if len(self.dict_of_correspondences) > self.max_correspondences:
#                self.rebase_users()

        self.update_intermediate(block_id=id_max)
        
    def amend_all(self, id_min, id_max):
        for id in range(id_min, id_max):
            blk = self.load_id(id) # :::~ This loads the first block

            amended = self.amend_transactions(blk)
            blk.transactions = amended['transactions']
            blk.short_transactions = amended['short_transactions']
            blk.new_elements = amended['new_elements']

            self.save_blk(blk)

            if self.update_condition(block_id = blk.block_id):
                self.update_intermediate(block_id = blk.block_id, proc="saving")

        cfg = {}
        cfg["n_users"] = self.n_users
        cfg["min_blk"] = self.min_blk
        cfg["max_blk"] = self.max_blk
        cfg["heuristics"] = self.heuristic

        pickle.dump(cfg,open("%s/config-%d_%d.pickle"%(self.output_directory, self.min_blk, self.max_blk),"w"))
        pickle.dump(cfg, open("%s/config.pickle" % (self.output_directory), "w"))



 #   def rebase_users(self):
 #       # :::~ to limit the time, correspondences are rebased
#
 #       reb_us = {}
#        #  print "doc",self.dict_of_correspondences
 #       for user in self.dict_of_correspondences:
 #           if user in reb_us:
 #               continue
 #           sou = self.dict_of_correspondences[user]
 #           rebu = min(sou)
 #           for us2 in sou:
 #               #  reb_us[user] = rebu
 #               reb_us[us2] = rebu
 ##   #    print self.naddr_to_users[:100]
#
#        for naddr, user in enumerate(self.naddr_to_users):
#            if user in reb_us:
#                self.naddr_to_users[naddr] = reb_us[user]
#        # print "reb_us", reb_us, [ (pos, val) for (pos, val) in enumerate(self.naddr_to_users) if pos != val]
#        del reb_us
#    #    print self.naddr_to_users[:100]
##
#
#        self.dict_of_correspondences.clear()

 #   def minimise_representation(self):
##        print self.n_users
 #       min_dict = -1 * np.ones(self.n_users, dtype=np.int) # -1: idiom for unitialised values
 #       accumulator = 0

 #       for naddr, user in enumerate(self.naddr_to_users):
 #           if min_dict[user] == -1:  # It has not been initilalised
 #               min_dict[user] = accumulator
 #               accumulator += 1
 #           self.naddr_to_users[naddr] = min_dict[user]
            #        indices = np.where(self.naddr_to_users[:, None] == values[None, :])[0]
#        self.naddr_to_users[ indices ] = min(values)
 #       self.n_users = accumulator

#
# def save_appearances(self, fname_appearance):
#     newline_msg("SAVE", "appearances into: %s" % fname_appearance)
#
#     if fname_appearance[-len(self.extension):] != self.extension:
#         fname_appearance = "%s%s" % (fname_appearance, self.extension)
#
#     pickle.dump(self.naddr_to_appearanceblk, gzip.open(fname_appearance, "wb"))

class SequentialPrecCalc(SequentialProcessor):
    '''
        Class that allows for sequential processi
    '''


    def __init__(self, save_each = 1000, file_log = None, skip_if_ud = None, strict = True , ecur = None, pickled_dir = "", output_dir = ""):
        SequentialProcessor.__init__(self, save_each, file_log  , ecur = ecur, pickled_dir = pickled_dir , output_dir = output_dir)
        self.total_attempts = 0.
        self.correct_attempts = 0.
        self.strict = strict
        self.skip_if_ud = skip_if_ud
        
        print >> self.file_log, "block_id total_counter correct_attempts total_attempts"
        

    def update_intermediate(self, **kwargs):
        if self.total_attempts > 0:
            log_str = "processed block_id = %d // %d --- prec: %f / %f ((%f))"%(kwargs["block_id"], self.n_users, self.correct_attempts, self.total_attempts,self.correct_attempts/self.total_attempts )
        else:    
            log_str = "processed block_id = %d // %d --- prec: %f / %f "%(kwargs["block_id"], self.n_users, self.correct_attempts, self.total_attempts)
        newline_msg("MSG", log_str )
        if self.file_log:
            print >> self.file_log, kwargs["block_id"], self.total_counter, self.correct_attempts, self.total_attempts
            self.file_log.flush()
            
        self.times_processed = 0
   #     self.rebase_users()

