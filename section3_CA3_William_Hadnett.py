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

amazoncol = mydb['amazonshop']
ebaycol = mydb['ebayshop']

# =============================================================================
# Question 1 - Number of Visits by Gender
# =============================================================================

def mapper(collIn):
    for doc in collIn:
        yield (doc['Customer']['Gender'], 1)
        

def reducer(mapDataIn):
    out = {}
    for word in mapDataIn:
        if word[0] in out.keys():
            out[word[0]] = out[word[0]] + word[1]
        else:
            out[word[0]] = word[1]
            
    return out


def reducerCols(reduceCol1, reduceCol2):
    out = reduceCol1
    for key, value in reduceCol2.items():
        out[key] = out.get(key, 0) + value
    return out


result1 = list(amazoncol.find({}))
result2 = list(ebaycol.find({}))
mapRes1 = mapper(result1)
mapRes2 = mapper(result2)

reduceCol1 = reducer(mapRes1)
reduceCol2 = reducer(mapRes2)

out = reducerCols(reduceCol1, reduceCol2)
# Number of visits across amazon and ebay by gender:
# {'Male': 120, 'Female': 279}

# =============================================================================
# Question 2 - Maximum Total Basket Price Across Both Stores and Customers
# =============================================================================

def mapper(collIn):
    for doc in collIn:
        total = 0
        for x in doc['Basket']:
            total += (x['Quantity']* x['UnitPrice'])
        yield ('total', total)

def reducer(mapDataIn):
    maxBasket = 0
    for word in mapDataIn:
        if word[1] > maxBasket:
            maxBasket = word[1]

    out = {'MaxBasket': maxBasket}
    return out


def reducerCols(reduceCol1, reduceCol2):
    out = {}
    if(reduceCol1['MaxBasket'] > reduceCol2['MaxBasket']):
        out = {'MaxBasket': reduceCol1['MaxBasket']}
    else:
        out = {'MaxBasket': reduceCol2['MaxBasket']}
    return out

result1 = list(amazoncol.find({}))
result2 = list(ebaycol.find({}))
mapRes1 = mapper(result1)
mapRes2 = mapper(result2)

reduceCol1 = reducer(mapRes1)
reduceCol2 = reducer(mapRes2)

out = reducerCols(reduceCol1, reduceCol2)
# Max Total Basket Price Across Both Stores and Customers: 
# {'MaxBasket': 15525.810000000007}

# =============================================================================
# Question 3 - The Average Number of Items Purchased by Nationality of Customers
# =============================================================================

def mapper(collIn):
    for doc in collIn:
        yield (doc['Customer']['Country'], len(doc['Basket']), 1)
        

def reducer(mapDataIn):
    out = {}
    for word in mapDataIn:
        if word[0] in out.keys():
            out[word[0]]=[out[word[0]][0]+ word[1], out[word[0]][1]+ word[2]]
        else:
            out[word[0]]= [word[1],word[2]]
            
    return out


def reducerCols(reduceCol1, reduceCol2):
    out = reduceCol1
    for key, value in reduceCol2.items():
        if key in out.keys():
            out[key]=[out[key][0]+ value[0], out[key][1]+ value[1]]
        else:
            out[key]=[value[0], value[1]]
    return out


result1 = list(amazoncol.find({}))
result2 = list(ebaycol.find({}))
mapRes1 = mapper(result1)
mapRes2 = mapper(result2)

reduceCol1 = reducer(mapRes1)
reduceCol2 = reducer(mapRes2)

out = reducerCols(reduceCol1, reduceCol2)

averages=[{key, value[0]/value[1]} for key,value in out.items()]
print(averages)

# Average Number of Items per Country:
# [{19.89971346704871, 'United Kingdom'}, {35.0, 'Portugal'}, 
# {18.4375, 'Germany'}, {'France', 17.842105263157894}, 
# {4.0, 'Finland'}, {'Spain', 43.333333333333336}, 
# {24.0, 'EIRE'}, {13.333333333333334, 'Australia'}, 
# {'Belgium', 14.0}, {43.0, 'Switzerland'}]

# =============================================================================
# Question 4 - The standard deviation in the basket price by gender of customers.
# =============================================================================

def mapper(collIn):
    for doc in collIn:
        total = 0
        for x in doc['Basket']:
            total += x['Quantity']* x['UnitPrice']
        yield (doc['Customer']['Gender'], total,  1, 0)
        
        
def findMean(dataList):
    maleTotal = 0
    maleCount = 0
    femaleTotal = 0
    femaleCount = 0
    for i in dataList:
        if i[0] == 'Male':
            maleTotal += i[1]
            maleCount += 1
        else:
            femaleTotal += i[1]
            femaleCount += 1
    
    mean = (maleTotal/maleCount, femaleTotal/femaleCount)
    return mean

def reducer(mapDataIn):
    listMapData = list(mapDataIn)   
    mean = findMean(listMapData)  
    out = {}
    for word in listMapData:
        if word[0] == 'Male':
            currentMean = mean[0]
        else:
            currentMean = mean[1]
        
        # Find delta squared which can then be used later to work variance
        # and from there standard deviation. 
        delta = word[1] - currentMean
        deltaSq = delta * delta
            
        if word[0] in out.keys():
            print(deltaSq)
            out[word[0]]=[out[word[0]][0]+ word[1], out[word[0]][1]+ word[2], out[word[0]][2]+ deltaSq]
        else:
            out[word[0]]= [word[1],word[2], 0]
        print(out)
    return out


def reducerCols(reduceCol1, reduceCol2):
    out = reduceCol1
    total = 0
    for key, value in reduceCol2.items():
        if key in out.keys():
            print(out[key][2]+ value[2])
            total = out[key][2]+ value[2]
            print(total)
            out[key]=[out[key][0]+ value[0], out[key][1]+ value[1], out[key][2]+ value[2]]
        else:
            out[key]=[value[0], value[1], value[2]]
        print(out)
    return out


result1 = list(amazoncol.find({}))
result2 = list(ebaycol.find({}))
mapRes1 = mapper(result1)
mapRes2 = mapper(result2)
    
reduceCol1 = reducer(mapRes1)
reduceCol2 = reducer(mapRes2)

out = reducerCols(reduceCol1, reduceCol2)

import math
print("\n")
for key, value in out.items():
    average = value[0]/value[1]
    variance = value[2]/value[1]
    stddev = math.sqrt(variance)
    print(key, "Average: ", average, " Variance: ", variance, " stddev: ", stddev)

# Male Average:  367.6361666666666  Variance:  439.1980948848039  stddev:  20.95705358309712
# Female Average:  459.7186379928316  Variance:  206.28183353419018  stddev:  14.362514874985862

# Female baskets will have less spread than male baskets as the standard deviation is lower.
# Variance shows that the female baskets are closer to the mean than male baskets.

# =============================================================================
# Question 5 - The confidence and lift measures for the association rule Code 22113 => 22112
# =============================================================================

def mapper(collIn, item1, item2):
    for doc in collIn:
        countItem1 = countItem2 = False
        for x in doc['Basket']:
            if x['StockCode'] == item1:
                countItem1 = True
            if x['StockCode'] == item2:
                countItem2 = True
        # Create a 'matrix' that can be processed in the reducer to discover
        # how many times an item appeared by itself or together. 
        if countItem2 and countItem1:
            yield ('items', 1, 1, 1)
        elif countItem1:
            yield ('items', 1, 0, 0)
        elif countItem2:
            yield ('items', 0, 1, 0)
        
            
#{'item': [29, 44, 15]}
def reducer(mapDataIn):            
    out = {}
    for word in mapDataIn:
        if word[0] in out.keys():
            out[word[0]]= [out[word[0]][0]+ word[1], out[word[0]][1]+ word[2], out[word[0]][2]+ word[3]]
        else:
            out[word[0]]= [word[1],word[2],word[3]]
    print(out)
    return out


# Add results from reducer together assuming confidence and lift is 
# wanted across both stores for the association relationship.
def reducerCols(reduceCol1, reduceCol2):
    out = reduceCol1
    for key, value in reduceCol2.items():
        if key in out.keys():
            out[key]=[out[key][0]+ value[0], out[key][1]+ value[1], out[key][2]+ value[2]]
        else:
            out[key]=[value[0], value[1], value[2]]
    return out


result1 = list(amazoncol.find({}))
result2 = list(ebaycol.find({}))
mapRes1 = mapper(result1, '22113', '22112')
mapRes2 = mapper(result2, '22113', '22112')

reduceCol1 = reducer(mapRes1)
reduceCol2 = reducer(mapRes2)

out = reducerCols(reduceCol1, reduceCol2)

group = {'$group': {'_id': 0, 'total': {'$sum': 1}}}
totalAmazon = list(amazoncol.aggregate([group]))

group = {'$group': {'_id': 0, 'total': {'$sum': 1}}}
totalEbay = list(ebaycol.aggregate([group]))

totalDocs = totalEbay[0]['total'] + totalAmazon[0]['total']

supportItem1 = out['items'][0] / totalDocs
supportItem2 = out['items'][1] / totalDocs
supportBoth = out['items'][2] / totalDocs

conf = supportBoth / supportItem1
print("Confidence 22113 => 22112: ",conf)
# Confidence 22113 => 22112:  0.5172413793103449

lift = supportBoth / (supportItem1 * supportItem2)
print("Lift 22113 => 22112: ", lift)
# Lift 22113 => 22112:  4.690438871473354




























