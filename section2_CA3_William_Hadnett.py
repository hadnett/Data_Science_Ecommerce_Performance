#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: williamhadnett D00223305
"""

import pymongo
import os


os.chdir('/Users/williamhadnett/Documents/Data_Science/Data_Science_CA3_William_Hadnett')
import atlasCredentials
# =============================================================================
# Connect to MongoDB
# =============================================================================

connection = "mongodb+srv://"+atlasCredentials.username+":"+atlasCredentials.password+"@cluster0.gh4kb.mongodb.net/test?retryWrites=true&w=majority"
client = pymongo.MongoClient(connection)

mydb = client['test']

shopcol = mydb['websiteshop']

# =============================================================================
# Product Association
# =============================================================================

#Find top ten products for association analysis based on quantity purchased.
unwind = {'$unwind':'$Basket'}
group = {'$group': {'_id': '$Basket.StockCode', 'count': {'$sum': 1}}}
sort={'$sort':{'count':-1}}
limit={'$limit': 10}
top10 = list(shopcol.aggregate([unwind,group,sort,limit]))
print(top10)
# [{'_id': '85123A', 'count': 320}, {'_id': '22423', 'count': 211}, 
# {'_id': '22469', 'count': 182}, {'_id': '22834', 'count': 162}, 
# {'_id': '22111', 'count': 160}, {'_id': '22961', 'count': 160}, 
# {'_id': '21485', 'count': 155}, {'_id': '22470', 'count': 152}, 
# {'_id': '22113', 'count': 146}, {'_id': '22112', 'count': 143}]

# =============================================================================
# Product Association - Product 85123A - Product 21212 - (Benchmark)
# =============================================================================

group = {'$group': {'_id': 0, 'total': {'$sum': 1}}}
totalDocs = list(shopcol.aggregate([group]))
print(totalDocs)

# Support(x) =  # of transactions in which x appears/total transactions

query =  {'Basket.StockCode': '85123A'}
support85123A = shopcol.count_documents(query) / totalDocs[0]['total']
print(support85123A)
# Support 85123A bought:  0.153 

query =  {'Basket.StockCode': {'$all': ['85123A', '21212']}}
supportBoth = shopcol.count_documents(query) / totalDocs[0]['total']
print(supportBoth)
# Support Both bought: 0.0105

#Confidence that 21212 will be bought when 85123A is bought.
#Conf(85123A -> 21212) = supp(85123A and 21212)/ supp(85123A)

conf = supportBoth / support85123A
print(conf)
# conf: 0.06862745098039216
 
# Lift 
query =  {'Basket.StockCode': '21212'}
support21212 = shopcol.count_documents(query) / totalDocs[0]['total']
print(support21212)
# 0.056

#Life(85123A -> 21212) = supp(85123A and 21212)/ supp(85123A) * supp(21212)
lift = supportBoth / (support85123A * support21212)
print(lift)
# Lift: 1.2254901960784315
# So the support for 21212 is 0.004% more likely to bough if the basket contains
# product 85123A than in general.

# =============================================================================
# Product Association - Generalize formula for top 10
# =============================================================================

# The above support, confidence and lift will act as a bench mark to ensure that the 
# calculates for the top ten are carried out correctly. 
def calculateAssoication(mongoResponse):
    
    pairs = findPairs(mongoResponse)
    
    group = {'$group': {'_id': 0, 'total': {'$sum': 1}}}
    totalDocs = list(shopcol.aggregate([group]))
    
    for i in pairs:
        # Support(x) =  # of transactions in which x appears/total transactions
        query =  {'Basket.StockCode': i[0]['_id']}
        supportItem1 = i[0]['count'] / totalDocs[0]['total']

        query =  {'Basket.StockCode': {'$all': [i[0]['_id'], i[1]['_id']]}}
        supportBoth = shopcol.count_documents(query) / totalDocs[0]['total']
        
        #Confidence that Item 1 will be bought when Item 2 is bought.
        #Conf(Item 1 -> Item 2) = supp(Item 1 and Item 2)/ supp(Item 1)
         
        # Lift 
        query =  {'Basket.StockCode': i[1]['_id']}
        supportItem2 =  i[1]['count'] / totalDocs[0]['total']
        
        conf = supportBoth / supportItem1
        # The only metric that changes in regards to the inverse association 
        # appears to be confidence as number of appearances remains the same for both 
        # items individually and together in the same basket. This metric can be 
        # gathered to display the inverse realtionship to the reader.
        inverseConf = supportBoth /supportItem2
        
        #Lift(Item 1 -> Item 2) = supp(Item 1 and Item 2)/ supp(Item1) * supp(Item2)
        lift = supportBoth / (supportItem1 * supportItem2)
        displayAssoication(supportItem1, supportBoth, supportItem2, conf, inverseConf, lift, i)


def findPairs(mongoResponse):
    
    it = iter(mongoResponse)
    pairs = list(zip(it, it))
    return pairs


def displayAssoication(support1, supportBoth, support2, conf, inverseConf, lift, i):
    
    print('Support for Item ',i[0]['_id'],': ',support1)
    print('Support Both: ',supportBoth)
    print('Support Item ',i[1]['_id'],': ',support2)
    print('Confidence: ',conf)
    print('Lift ',i[0]['_id'],' -> ',i[1]['_id'],': ',lift)
    
    print('\nSupport Item ',i[1]['_id'],': ',support2)
    print('Support Both: ',supportBoth)
    print('Support for Item ',i[0]['_id'],': ',support1)
    print('Confidence: ',inverseConf)
    print('Lift ',i[1]['_id'],' -> ',i[0]['_id'],': ',lift)
    print("\n")

calculateAssoication(top10)

# Output of Assoication Analysis of Top Ten Items
'''
Support for Item  85123A :  0.16
Support Both:  0.016
Support Item  22423 :  0.1055
Confidence:  0.1
Lift  85123A  ->  22423 :  0.9478672985781991

Support Item  22423 :  0.1055
Support Both:  0.016
Support for Item  85123A :  0.16
Confidence:  0.15165876777251186
Lift  22423  ->  85123A :  0.9478672985781991


Support for Item  22469 :  0.091
Support Both:  0.0085
Support Item  22834 :  0.081
Confidence:  0.09340659340659342
Lift  22469  ->  22834 :  1.1531678198344866

Support Item  22834 :  0.081
Support Both:  0.0085
Support for Item  22469 :  0.091
Confidence:  0.10493827160493828
Lift  22834  ->  22469 :  1.1531678198344866


Support for Item  22111 :  0.08
Support Both:  0.0105
Support Item  22961 :  0.08
Confidence:  0.13125
Lift  22111  ->  22961 :  1.640625

Support Item  22961 :  0.08
Support Both:  0.0105
Support for Item  22111 :  0.08
Confidence:  0.13125
Lift  22961  ->  22111 :  1.640625


Support for Item  21485 :  0.0775
Support Both:  0.0095
Support Item  22470 :  0.076
Confidence:  0.12258064516129032
Lift  21485  ->  22470 :  1.6129032258064517

Support Item  22470 :  0.076
Support Both:  0.0095
Support for Item  21485 :  0.0775
Confidence:  0.125
Lift  22470  ->  21485 :  1.6129032258064517


Support for Item  22113 :  0.073
Support Both:  0.017
Support Item  22112 :  0.0715
Confidence:  0.23287671232876717
Lift  22113  ->  22112 :  3.257016955647093

Support Item  22112 :  0.0715
Support Both:  0.017
Support for Item  22113 :  0.073
Confidence:  0.2377622377622378
Lift  22112  ->  22113 :  3.257016955647093
'''





