'''
Created on 27 Aug 2013

@author: tessonec
'''



import sys, time

# 
# from collections import defaultdict



def newline_msg( tp, msg, indent = 0 , stream = sys.stderr, colorise = False ):
    try:
         prog_name = sys.argv[0].split("-")[2]
    except:
         prog_name = sys.argv[0].split("-")[1]
    if colorise:
        tp = str_color(tp)
        msg = str_color(msg)
    print >> stream, "%s[ %s - %5s ] (@%s) %s"%(" "*indent, prog_name, tp, time.strftime("%m/%d %H:%M:%S"), msg)
    stream.flush()

def sameline_msg( tp, msg, indent = 0 , stream = sys.stderr, colorise = False ):
    try:
         prog_name = sys.argv[0].split("-")[2]
    except:
         prog_name = sys.argv[0].split("-")[1]
    if colorise:
        tp = str_color(tp)
        msg = str_color(msg)
    print >> stream, "%s[ %s - %5s ] (@%s) %s\r"%(" "*indent, prog_name, tp, time.strftime("%m/%d %H:%M:%S"), msg),
    stream.flush()

def str_color(s):
    "simple colour implementation"
    color_dict = {"red": "\033[31m","green": "\033[32m","yellow": "\033[33m","blue": "\033[34m","magenta": "\033[35m","cyan": "\033[36m", "reset":'\033[0m'}


    for c in color_dict:
        s = s.replace( "@%s"%c, color_dict[c] )
    return s+color_dict['reset']



# class Counter():
#     def __init__(self):
#         self.counter = 0
#         
#     def __call__(self, *args):
#         self.counter += 1
#         #print "Counter::: ", self.counter
#         return self.counter
#     
#     


#def ctdict():
    # :::~ This dictionary (if the item doesn't exist yet, it adds it to the disctionary, with a value larger than a previous one) 
    # :::~  To modify the counter, modify the 'default_factory.counter' member
#    return  dict() # defaultdict( Counter() )

