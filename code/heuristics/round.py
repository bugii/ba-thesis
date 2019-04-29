

import pandas as pd
import numpy as np
import math as m
import decimal as d
import datetime as dt
import dateutil.parser as du

class RoundEstimation:

    def __init__(self, delta, epsilon = 1e-8):
        self.delta = delta
        self.epsilon = d.Decimal(epsilon)


    def is_round(self, value):
        a = d.Decimal(value )
        resto = abs( a - d.Decimal( round(a,self.delta) ) )
        if resto  < self.epsilon:
            return value, True
        return  value, False






class ForexRoundEstimator:

    def __init__(self, delta, epsilon = 1e-8):
        self.delta = delta
        self.epsilon = d.Decimal(epsilon)
        self.__load_prices()
#        self.__prices = {}


    def __load_prices(self):
        self.__prices = {}
        for l in open("market-price.csv"):
            dat, val = l.split(",")
            val = float(val)
            dat = du.parse(dat)
            self.__prices[dat] = val
        dat = du.parse("2008-06-06")
        self.__prices[dat] = 0.

    def get_price(self,date):

        if type(date) == type(""):
            date = du.parse(date)
 #       print date

        date = dt.datetime(year=date.year, month=date.month, day = date.day)
        date0 = date

        while not self.__prices.has_key(date) :

            date = date - dt.timedelta(days=1)

 #       print date0, date, len(self.__prices), self.__prices[date]
        return self.__prices[date]


    def is_round(self,value,date = None):
        price = self.get_price(date)
        a = d.Decimal(value)
        if price < 10:
            resto = abs(10*a - int(10*a))
        else:
            loc_delta = - int(m.log10(self.delta / price))
            resto = abs(a - d.Decimal(round(a, loc_delta)))


        ret = resto  < self.epsilon
#        print resto

        return  ret






#fre = ForexRoundEstimator(0.1)

#print fre.is_round(0.05, "2011-01-28")
