# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 10:44:17 2019

@author: megan
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class Part2(MRJob):

    MRJob.SORT_VALUES = True

    def steps(self):
        return [
            MRStep(mapper=self.mapper_find_customer,
                   reducer=self.reducer_count_spending),
            MRStep(mapper=self.mapper_make_customer_want_sort,
                   reducer = self.reducer_output_sorted)
        ]
        
    def mapper_find_customer(self, _, line):
        (CustomID, ItemID, amountspend) = line.split(',')
        yield CustomID, float(amountspend) #find out each customer each purchase
        
    def reducer_count_spending(self, CustomID, amountspend):
        yield CustomID, sum(amountspend) #find out each customer total purchase not sorted

    def mapper_make_customer_want_sort(self, CustomerID, amountspend): #want to sort
        yield None, ('%04.02f'%float(amountspend), CustomerID)
        
    def reducer_output_sorted(self, _, amountspendCustomerID):
        for sort in amountspendCustomerID:
            yield sort[0], sort[1]        #amountspend & CustomerID

if __name__ == '__main__':
    Part2.run()

# !python Part2.py DataA1.csv > Part2.txt