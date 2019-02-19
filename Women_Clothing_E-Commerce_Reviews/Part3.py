# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 18:22:53 2019

@author: megan
"""


from mrjob.job import MRJob
from mrjob.step import MRStep

class Part3(MRJob):

    MRJob.SORT_VALUES = True

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_item,
                   combiner=self.combiner_count_item,
                   reducer=self.reducer_get_item),
            MRStep(mapper=self.mapper_item_sort,
                   reducer = self.reducer_output_sorted)
        ]
        
    def mapper_get_item(self, _, line):
        (transactionID, ItemName, rate) = line.split(',')
        yield int(ItemName), 1
    
    def combiner_count_item(self, ItemName, counts):
        yield ItemName, sum(counts)
    
    def reducer_get_item(self, ItemName, counts):
        yield ItemName, sum(counts) 

    def mapper_item_sort(self, ItemName, counts):
        yield None, ('%05d'%int(counts), ItemName)
        
    def reducer_output_sorted(self, n, countsItemName):
        for sort in sorted(countsItemName, reverse=True): #sort descending
            yield sort[0], sort[1] 
#Sequence: counts, ItemName
#e.g. count: occur"01024"times   ItemName: 1078	
            
if __name__ == '__main__':
    Part3.run()

# !python Part3.py Womens_Clothing_E-Commerce_Reviews.csv > Part3.txt