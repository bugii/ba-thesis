


import graph_tool as gt
import graph_tool.topology as gtt


#import bidict
from collections import defaultdict
import numpy as np
import os.path

import shutil
class Componentiser():
    '''

    '''


    def __init__(self):
        self.graph_of_correspondences = gt.Graph( directed=False )
        self.nuser_vertex = {} #bidict.bidict()
        self.correspondences = None

        self.counter = 0
        self.n_users = 0


    def reset(self):
        del self.graph_of_correspondences
        del self.nuser_vertex

        self.graph_of_correspondences = gt.Graph( directed=False )
        self.nuser_vertex = {} #bidict.bidict()

        self.correspondences = None

        self.counter = 0
        self.n_users = 0

    def add_correspondence(self, ls, arg2 = None):

        if type(ls) == type(1) or type(ls) == np.int32:
            if ls == arg2:
               return
            ls = set( [ls, arg2] )
        elif type(ls) == type([]):
            if arg2 is not None:
                ls.append( arg2 )
            ls = set(ls)

        if len(ls) <= 1:
            return

        ls = sorted( ls )
        for nu in ls:
            if self.nuser_vertex.has_key(nu):
                continue
            v = self.graph_of_correspondences.add_vertex()
            idx = self.graph_of_correspondences.vertex_index[v]
            self.nuser_vertex[nu] = idx

        v0 = self.nuser_vertex[ ls[0] ]

        for nu in ls[1:]:
            v1 = self.nuser_vertex[nu]
            if self.graph_of_correspondences.edge(v0,v1) is None:
                self.graph_of_correspondences.add_edge(v0,v1)
                self.counter += 1


    def update_n_users(self, ls):
        if len(ls) == 0:
             return

        max_ls = max(ls) + 1
        if max_ls > self.n_users:
            self.n_users = max_ls
      #  print self.n_users


    def compute_correspondences(self, no_users = None):
        if no_users is None:
            no_users = self.n_users

        self.correspondences = np.arange(0,no_users, dtype = np.int32)
        comps, hist = gtt.label_components( self.graph_of_correspondences )

        d_comps = {}

        for u in range(no_users):
            if not self.nuser_vertex.has_key(u):
                continue
            v = self.nuser_vertex[u]
            comp = comps[v]

            if not comp in d_comps:
                d_comps[comp] = u

            self.correspondences[u] = d_comps[comp]

#            print u, d_comps[comp]
#        print self.nuser_vertex
#        print [int(i) for i in self.graph_of_correspondences.vertices()]
#        print [str(i) for i in self.graph_of_correspondences.edges()]
#        print self.correspondences


    def save(self, heuristic, min_blk, max_blk):
        if not os.path.exists(heuristic):
            os.makedirs(heuristic)

        fname = "%s/correspondences-%d_%d.npy"%(heuristic,min_blk,max_blk)
        np.save(fname, self.correspondences)

    def load(self, heuristic, min_blk, max_blk, move = False):
        fname = "%s/correspondences-%d_%d.npy" % (heuristic, min_blk, max_blk)
        self.correspondences = np.load(fname)
        self.n_users = len(self.correspondences)
        if move:
            shutil.move("%s/correspondences-%d_%d.npy" % (heuristic, min_blk, max_blk),"%s/correspondences_processed-%d_%d.npy" % (heuristic, min_blk, max_blk))

        for (i,ci) in enumerate(self.correspondences):
            if i != ci:
                self.add_correspondence(i,ci)


    def minimise_representation(self):
         min_dict =  np.full(self.n_users, -1, dtype=np.int32) # -1: idiom for unitialised values
         self.minimised_correspondences = np.full(self.n_users, -1, dtype=np.int32)
         accumulator = 0

         for naddr, user in enumerate(self.correspondences):
             if min_dict[user] == -1:  # It has not been initilalised
                 min_dict[user] = accumulator
                 accumulator += 1
             self.minimised_correspondences[naddr] = min_dict[user]
         self.minimised_n_users = accumulator+1

    def __getitem__(self, item):
        return self.correspondences[ item ]


    def save_minimised(self, heuristic, min_blk, max_blk):
        if not os.path.exists(heuristic):
            os.makedirs(heuristic)

        fname = "%s/minimised_correspondences-%d_%d.npy"%(heuristic,min_blk,max_blk)
        np.save(fname, self.minimised_correspondences)

    def get_report(self):
        return  self.graph_of_correspondences.num_vertices(), self.graph_of_correspondences.num_edges()
            #compo = Componentiser()

#compo.add_nuser_correspondence([0,1,2,3,4])
#compo.add_nuser_correspondence([8,10])
#compo.add_nuser_correspondence([6,14])
#compo.add_nuser_correspondence([2,16])
#compo.add_nuser_correspondence([10,5])

#ret = compo.compute_correspondences(20)

#compo2 = Componentiser()
#for i in range(20):
#    compo2.add_nuser_correspondence([i,ret[i]])
#    print "r1::: %3d => %3d"%(i,ret[i])

#ret2 = compo2.compute_correspondences(20)
#for i in range(20):
#    print "r2::: %3d => %3d, %3d"%(i,ret2[i],ret[i])









    # def update_user_correspondences(self, ls_users):
    #
    #     if len(ls_users) <= 1:
    #         return
    #
    #     self.total_counter += 1
    #     max_l = 0
    #     max_set = set()
    #     for i in ls_users:
    #         if self.dict_of_correspondences.has_key(i):
    #
    #             if len(self.dict_of_correspondences[i]) > max_l:
    #                 max_set = self.dict_of_correspondences[i]
    #                 max_l = len(max_set)
    #
    #
    #     for i in ls_users:
    #         if self.dict_of_correspondences.has_key(i) and self.dict_of_correspondences[i] != max_set:
    #             max_set.update(self.dict_of_correspondences[i])
    #         else:
    #             max_set.add(i)
    #     for i in max_set:
    #         self.dict_of_correspondences[i] = max_set
    #
    # def rebase_users(self):
    #     # :::~ to limit the time, correspondences are rebased
    #     reb_us = {}
    #     #  print "doc",self.dict_of_correspondences
    #     for user in self.dict_of_correspondences:
    #         if user in reb_us:
    #             continue
    #         sou = self.dict_of_correspondences[user]
    #         rebu = min(sou)
    #         for us2 in sou:
    #             #  reb_us[user] = rebu
    #             reb_us[us2] = rebu
    #
    #     for naddr, user in enumerate(self.naddr_to_users):
    #         if user in reb_us:
    #             self.naddr_to_users[naddr] = reb_us[user]
    #     del reb_us
    #
    #
    #     self.dict_of_correspondences.clear()
    #
    # def minimise_representation(self):
    #     min_dict = -1 * np.ones(self.n_users, dtype=np.int) # -1: idiom for unitialised values
    #     accumulator = 0
    #
    #     for naddr, user in enumerate(self.naddr_to_users):
    #         if min_dict[user] == -1:  # It has not been initilalised
    #             min_dict[user] = accumulator
    #             accumulator += 1
    #         self.naddr_to_users[naddr] = min_dict[user]
    #     self.n_users = accumulator