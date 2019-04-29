#!/usr/bin/python
'''
Created on Apr 12, 2013

@author: tessonec
'''

import sys, re, pickle
from lxml import html



def stderr_message(msg_type, string, indent = 0):
    print >> sys.stderr, " "*indent+"[ %s ] %s"%( msg_type, string)


class BlockParser:

  def __init__(self, id, fname = None):

    self.block_id = id
    if fname is not None:
      self.doc = html.fromstring( "".join(open(fname).readlines() ) )

      self.parse_block()
    
      self.parse_transactions()
      del self.doc 


  def parse_block(self):
  #  self.info = {}

    xpval = self.doc.xpath("//ul[@class='infoList']/li")
    for row in xpval:
      
      if row.text == None: continue
      row_ls = [i.lower() for i in row.itertext()]
      key = row_ls[0] 
      if key in ["hash", "previous block", "next block", "time", "total btc", "merkle root", "nonce"]:
    
          self.__dict__[key.replace(" ","_")] = row_ls[-1].strip(": ")
      if key  == "transactions":
          self.__dict__["no_transactions"] = row_ls[-1].strip(": ")
      if key == "difficulty":
          foo = row_ls[-3].strip(":")
          foo = foo[:foo.find("(")].replace(" ","")
          self.__dict__[key] = foo
      if "generation" in key:
          self.__dict__["generation_amount"] = key.split()[1]
          self.__dict__["generation_fees"] =key.split()[3]
    
      
  def parse_transactions(self):
    
    self.transactions = []
    
    table = self.doc.xpath("//table[@class='txtable']")
    
    for row in table[0].xpath(".//tr"):
      transaction_fee = row.getchildren()[1].text
      if transaction_fee == "Fee": continue
      transaction_id = row.getchildren()[0].getchildren()[0].get('href')
      transaction_id = transaction_id.split("/")[-1]
 
      transaction_size = row.getchildren()[2].text
      
      transaction_from = []
      xpfrom = row.getchildren()[3].getchildren()[0].xpath(".//li")

      for i in xpfrom:
          itext = i.text
          if itext is None:
              lstxt = [j.strip(": ") for j in i.itertext()]
             # print lstxt
              transaction_from.append((lstxt[0], lstxt[1]))
           
          else:
              if  "Generation" in itext:
                 transaction_from.append(("generation",itext.split()[1]))
      
      transaction_to = []
      xpto = row.getchildren()[4].getchildren()[0].xpath(".//li")
      for i in xpto:
          itext = i.text
    
          if itext is None:
              lstxt = [j.strip(": ") for j in i.itertext()]
              transaction_to.append((lstxt[0], lstxt[1]))
           


      
      self.transactions.append( ( transaction_id, transaction_fee, transaction_size, transaction_from, transaction_to ) )
    
def load_id(curr, id, method = None):
        """Returns a BlockParser object"""
        if method is None:
            pickled_directory = "pickled-%s"%curr
        else:
            pickled_directory = method
        outdir_name = "%s/%.3d"%(pickled_directory, id/1000)
        return pickle.load(open("%s/%d.pickle"%(outdir_name, id), "r"))
 