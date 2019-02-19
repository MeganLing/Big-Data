# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 10:12:05 2019

@author: megan
"""

from mrjob.job import MRJob

class Part1(MRJob):

    def mapper(self, _, line):
        (CustomID, ItemID, amountspend) = line.split(',')
        yield CustomID, float(amountspend) #find out each customer each purchase
        
    def reducer(self, CustomID, amountspend):
        yield CustomID, sum(amountspend) #find out each customer total purchase

if __name__ == '__main__':
    Part1.run()
    
#!python Part1.py DataA1.csv > Part1.txt
