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

# Total items purchased by Females.
unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Female' }}
group={'$group': {'_id': '$status', 'total':{'$sum':'$Basket.Quantity'}}}
totalItemsFemale = list(shopcol.aggregate([match,unwind,group]))
print(totalItemsFemale)
# [{'_id': None, 'total': 360283}]

# Total items purchased by Males.
unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Male' }}
group={'$group': {'_id': '$status', 'total':{'$sum':'$Basket.Quantity'}}}
totalItemsMale = list(shopcol.aggregate([match,unwind,group]))
print(totalItemsMale)
# [{'_id': None, 'total': 153181}]

# The above Analysis shows the total number of items bought by gender. It is 
# clear that Females bought more items than Males (over twice as much). Males
# bought a total of 153181 items and females bought a total of 360283 items.
# Confirming that females appear to visit this website more and purchase a 
# larger quantity of items. 

# =============================================================================
# Customer Analysis - Number of Items Purchased by Age
# =============================================================================

# Total Items purchased by visitors under 40 years of age.
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$lt': 40} }}
group={'$group': {'_id': '$status', 'total':{'$sum':'$Basket.Quantity'}}}
totalItemsUnder40 = list(shopcol.aggregate([match,unwind,group]))
print(totalItemsUnder40)
# [{'_id': None, 'total': 447076}]

# Total Items purchased by visitors between 40 and 60 years of age (inclusive).
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$gte': 40, '$lte': 60} }}
group={'$group': {'_id': '$status', 'total':{'$sum':'$Basket.Quantity'}}}
totalItems40To60 = list(shopcol.aggregate([match,unwind,group]))
print(totalItems40To60)
# [{'_id': None, 'total': 52369}]

# Total Items purchased by visitors over 60.
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$gt': 60} }}
group={'$group': {'_id': '$status', 'total':{'$sum':'$Basket.Quantity'}}}
totalItemsOver60 = list(shopcol.aggregate([match,unwind,group]))
print(totalItemsOver60)
# [{'_id': None, 'total': 14019}]

# Further Analysis of Number of Items bought by Age
percentItemsOver40 = ((totalItems40To60[0]['total'] + totalItemsOver60[0]['total']) / totalItems[0]['total']) *100
print(percentItemsOver40)
# 12.9294361435271

percentItemsUnder40 = (totalItemsUnder40[0]['total'] / totalItems[0]['total']) * 100
print(percentItemsUnder40)
# 87.07056385647289

# Upon further analysis it is clear that the items bought from this website 
# are primarily bought by visitors under the age of 40 as they account for 
# 87.07% of the items purchased. 

# =============================================================================
# Customer Analysis - Value of Items Purchased by Gender
# =============================================================================

# Find the max value of an item in a customers basket.
unwind = {'$unwind':'$Basket'}
group={'$group': {'_id': '$status', 'MaxValue':{'$max':'$Basket.UnitPrice'}}}
maxPrice = list(shopcol.aggregate([unwind,group]))
print(maxPrice)
# [{'_id': None, 'MaxPrice': 295}]

# Find the min value of an item in a customers basket.
unwind = {'$unwind':'$Basket'}
group={'$group': {'_id': '$status', 'MinValue':{'$min':'$Basket.UnitPrice'}}}
minPrice = list(shopcol.aggregate([unwind,group]))
print(minPrice)
# [{'_id': None, 'MinValue': 0.07}]

# Large difference between the most expensive and cheapest item.

# Find the average value for Females
unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Female' }}
group={'$group': {'_id': '$status', 'AverageValue':{'$avg':'$Basket.UnitPrice'}}}
avgPrice = list(shopcol.aggregate([match,unwind,group]))
print(avgPrice)
# 'AverageValue': 3.122939869562101

# Find the average value for Males
unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Male' }}
group={'$group': {'_id': '$status', 'AverageValue':{'$avg':'$Basket.UnitPrice'}}}
avgPrice = list(shopcol.aggregate([match,unwind,group]))
print(avgPrice)
# 'AverageValue': 2.929234227002729

# Females tend to have a larger average value than Males. This would suggest 
# that on average females purchase more expensive items than males.

# Total value of female baskets.
unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Female' }}
group={'$group': {'_id': '$status', 'TotalValue':{'$sum':'$Basket.UnitPrice'}}}
totalValue = list(shopcol.aggregate([match,unwind,group]))
print(totalValue)
# 'TotalValue': 87148.76

# Total value of male baskets.
unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Male' }}
group={'$group': {'_id': '$status', 'TotalValue':{'$sum':'$Basket.UnitPrice'}}}
totalValue = list(shopcol.aggregate([match,unwind,group]))
print(totalValue)
# 'TotalValue': 36492.4

# The total value of the for females is considerably more than the
# total value for males. This could be caused by either females buying more
# items or by females buying more expensive items. However, it is clear that 
# the baskets belonging to females are more valuable. 

# =============================================================================
# Customer Analysis - Value of Items Purchased by Age
# =============================================================================

# Find average value for visitors under 40 years of age.
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$lt': 40} }}
group={'$group': {'_id': '$status', 'AverageValue':{'$avg':'$Basket.UnitPrice'}}}
avgPrice = list(shopcol.aggregate([match,unwind,group]))
print(avgPrice)
# 'AverageValue': 3.0566669615736237

# Find the average value for visitors between 40 and 60 (inclusive).
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$gte': 40, '$lte': 60} }}
group={'$group': {'_id': '$status', 'AverageValue':{'$avg':'$Basket.UnitPrice'}}}
avgPrice = list(shopcol.aggregate([match,unwind,group]))
print(avgPrice)
# 'AverageValue': 3.0666014095536416

# Find average value for visitors over 60 years of age.
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$gt': 60} }}
group={'$group': {'_id': '$status', 'AverageValue':{'$avg':'$Basket.UnitPrice'}}}
avgPrice = list(shopcol.aggregate([match,unwind,group]))
print(avgPrice)
# 'AverageValue': 3.2133927245731253

# The average value is similiar across the three age ranges outlined above. 
# However, it can be noted that visitors in the over 60s age group have a slightly
# higher average value. Visitors under 40 and between 40 and 60 only have a 0.1
# difference in their average value.

unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$lt': 40} }}
group={'$group': {'_id': '$status', 'TotalValue':{'$sum':'$Basket.UnitPrice'}}}
totalValue = list(shopcol.aggregate([match,unwind,group]))
print(totalValue)
# 'TotalValue': 103648.52

unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$gte': 40, '$lte': 60} }}
group={'$group': {'_id': '$status', 'TotalValue':{'$sum':'$Basket.UnitPrice'}}}
totalValue = list(shopcol.aggregate([match,unwind,group]))
print(totalValue)
# 'TotalValue': 15664.2

unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$gt': 60} }}
group={'$group': {'_id': '$status', 'TotalValue':{'$sum':'$Basket.UnitPrice'}}}
totalValue = list(shopcol.aggregate([match,unwind,group]))
print(totalValue)
# 'TotalValue': 4328.44

# Find total number of documents for visitors less than 40 years old.
query = { 'Customer.Age': {'$lt': 40} }
result = shopcol.count_documents(query)
print(result)
# Total Document under 40: 1670

# Find total number of documents for visitors more than 60 years old.
query = { 'Customer.Age': {'$gt': 60} }
result = shopcol.count_documents(query)
print(result)
# Total Documents over 60: 73

# After reviewing the totalValue for each age group it is clear that the under
# 40s have a larger total value. However, this does not match the average 
# value identified ealier in this section. This may be partily due to the sample
# size as we know that when sample size increases the STD decreases and vice versa.
# The average for the under 40s is also more precise due to the 'Law of Large 
# Numbers'. 

# =============================================================================
# Customer Analysis - Total Spend by Gender
# ============================================================================= 

# Find the total spend of female visitors.
unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Female'}}
group={'$group': {'_id': '$status',  'totalSpend': {'$sum': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
totalSpend = list(shopcol.aggregate([match,unwind,group]))
print(totalSpend)
# 'totalSpend': 643346.28

# Find the average female spend.
unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Female'}}
group={'$group': {'_id': '$status',  'avgSpend': {'$avg': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
avgSpend = list(shopcol.aggregate([match,unwind,group]))
print(avgSpend)
# 'avgSpend': 23.05404859170071

# Find the total spend of male visitors.
unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Male'}}
group={'$group': {'_id': '$status',  'totalSpend': {'$sum': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
totalSpend = list(shopcol.aggregate([match,unwind,group]))
print(totalSpend)
# 'totalSpend': 283904.09

# Find the average female spend.
unwind = {'$unwind':'$Basket'}
match = {'$match':{'Customer.Gender': 'Male'}}
group={'$group': {'_id': '$status',  'avgSpend': {'$avg': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
avgSpend = list(shopcol.aggregate([match,unwind,group]))
print(avgSpend)
# 'avgSpend': 22.7888978969337

# Female visitors clearly spent more than male visitors on this website. Females
# spend a total of 643346.28 while males only spend a total of 283904.09. 












