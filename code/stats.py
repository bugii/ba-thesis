#!/usr/bin/python

import numpy as np
import math as m


def theil_index(y):
    if len(y) == 0:
        return 0

    n = len(y)
        
    yt = y.sum()
    s = y/(yt*1.0)
    lns = np.log(n*s)
    slns = s*lns
    return np.sum(slns)/m.log(n)
    


def gini_index(x, must_sort = True):
    if len(x) == 0:
        return 0
    if must_sort:
        x = sorted(x) # increasing order

    y = np.cumsum(x)
    
    B = np.sum(y) / (y[-1] * len(x))
    return 1 + 1./len(x) - 2*B

    


# r = np.random.lognormal(1,1,10000)
# 
# print theil_index(r), gini_index(r) 
# 
# r = np.ones(10000)
# 
# print theil_index(r), gini_index(r) 
# 
# r = np.ones(100000)
# r[0] = 1e10
# print theil_index(r), gini_index(r) 

