# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 13:28:14 2019

@author: vvanderwesthuizen
"""


import sys

def hello(a,b):
    print("hello and that's your sum:", a + b)

if __name__ == "__main__":
    a = int(sys.argv[1])
    b = int(sys.argv[2])
    hello(a, b)

