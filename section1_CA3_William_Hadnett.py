#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 22:34:10 2020

@author: williamhadnett
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
# Customer Analysis - Number of Visits by Gender
# =============================================================================

query = { 'Customer.Gender': 'Female' }
result = shopcol.count_documents(query)
print(result)
# Number of Female Visitors: 1391 

query = { 'Customer.Gender': 'Male' }
result1 = shopcol.count_documents(query)
print(result1)
# Number of Male Visitors: 609
# It is clear from the results returned that this ecommerce platform has considerably
# more female visitors than male visitors.

project = {'$project': {'month': {'$month': {'$toDate': '$InvoiceDate'}}}}
match = {'$match':{ 'Customer.Gender': 'Female' }}
group = {'$group': {'_id': '$month', 'count': {'$sum': 1}}}
result = list(shopcol.aggregate([match, project, group]))
print(result)
#[{'_id': 11, 'count': 189}, {'_id': 12, 'count': 744}, {'_id': 1, 'count': 458}]

project = {'$project': {'month': {'$month': {'$toDate': '$InvoiceDate'}}}}
match = {'$match':{ 'Customer.Gender': 'Male' }}
group = {'$group': {'_id': '$month', 'count': {'$sum': 1}}}
result = list(shopcol.aggregate([match, project, group]))
print(result)
#[{'_id': 11, 'count': 57}, {'_id': 12, 'count': 308}, {'_id': 1, 'count': 244}]

# After reviewing the visits by gender grouped by month we can see that both males
# and females visited the site more during the month of December. This makes
# sense as customers were most likely buying Christmas gifts during this month.
# However, overall per month females appear to visit the site more.  

# =============================================================================
# Customer Analysis - Number of Visits by Age
# =============================================================================

# Find Max Age in Collection
group={'$group': {'_id': '$status', 'MaxAge':{'$max':'$Customer.Age'}}}
result = list(shopcol.aggregate([group]))
print(result)
# MaxAge: 80

# Find Min Age in Collection
group={'$group': {'_id': '$status', 'MinAge':{'$min':'$Customer.Age'}}}
result = list(shopcol.aggregate([group]))
print(result)
# MinAge: 18

query = { 'Customer.Age': {'$lt': 40} }
resultAgeLT40 = shopcol.count_documents(query)

query = { 'Customer.Age': {'$gte': 40, '$lte': 60} }
resultAgeGTE40LTE60 = shopcol.count_documents(query)

query = { 'Customer.Age': {'$gt': 60} }
resultAgeGT60 = shopcol.count_documents(query)

print("\nNumber of Customers Under 40: ", resultAgeLT40)
print("Number of Customers between 40 and 60 inclusive: ", resultAgeGTE40LTE60)
print("Number of Customers 60+: ", resultAgeGT60 )
#Number of Customers Under 40:  1670
#Number of Customers between 40 and 60 inclusive:  257
#Number of Customers 60+:  73

# To compare number of visits by age the ages have been seperated into three 
# groups. Customers under 40, customers between the age of 40 and 60 (inclusive)
# and customers over 60. It is clear that the majority of the visits to this website
# are made by visitors under the age of 40.  

# Further Analysis of customer visits under 40. 
query = { 'Customer.Age': {'$gte': 20, '$lte': 30}}
resultAgeGTE20LTE30 = shopcol.count_documents(query)
print("\nNumber of Customers between 20 and 30 inclusive: ", resultAgeGTE20LTE30)

# Find percentage of customer visits between 20 and 30 (inclusive).
totalDocuments = shopcol.count_documents({})
percent20To30 = (resultAgeGTE20LTE30/totalDocuments) * 100
print(percent20To30)
# percent20To30 = 63.6

# It appears that the majority of the visits to this website are made by people
# between the ages of 20 and 30 (inclusive). This age range accounts for 63.6 
# percent of the traffic on the website. Therefore, it is important that this
# website continues to cater for this age range moving forward as it 
# appears to be the age range of the websites target audience. 

# =============================================================================
# Customer Analysis - Number of Items Purchased by Gender
# =============================================================================

unwind = {'$unwind':'$Basket'}
group={'$group': {'_id': '$status', 'total':{'$sum':'$Basket.Quantity'}}}
totalItems = list(shopcol.aggregate([unwind,group]))
print(totalItems)
# [{'_id': None, 'total': 513464}]

unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Female' }}
group={'$group': {'_id': '$status', 'total':{'$sum':'$Basket.Quantity'}}}
totalItemsFemale = list(shopcol.aggregate([match,unwind,group]))
print(totalItemsFemale)
# [{'_id': None, 'total': 360283}]

unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Male' }}
group={'$group': {'_id': '$status', 'total':{'$sum':'$Basket.Quantity'}}}
totalItemsMale = list(shopcol.aggregate([match,unwind,group]))
print(totalItemsMale)
# [{'_id': None, 'total': 153181}]

# The above Analysis shows the total number of items bought by gender. It is 
# clear that Females bought more items than Males (over twice as much). Males
# bought a total of 153181 items and females bought a total of 360283 items. 








