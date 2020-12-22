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

# Check all customers uploaded to altas.
group = {'$group': {'_id': 0, 'Number': {'$sum': 1}}}
result = list(shopcol.aggregate([group]))
print(result)

# =============================================================================
# Customer Analysis - Number of Visits by Gender
# =============================================================================

# Number of shopping trips by gender
group = {'$group': {'_id': '$Customer.Gender', 'count': {'$sum': 1}}}
result = list(shopcol.aggregate([group]))
print(result)
# Number of Female Visitors: 1391 
# Number of Male Visitors: 609

# Find the number of unique customers by gender.
group = {'$group': {'_id': '$Customer.ID', 'Gender': {'$max': "$Customer.Gender"}}}
group2 = {'$group': {'_id': '$Gender', 'Number': {'$sum': 1}}}
result = list(shopcol.aggregate([group, group2]))
print(result)
# Number of Unique Female Visitors: 713
# Number of Unique Male Visitors: 402

# It is clear from the results returned that this ecommerce platform has 
# visited by considerably more females than males.

# Group Female visits by month.
project = {'$project': {'month': {'$month': {'$toDate': '$InvoiceDate'}}}}
match = {'$match':{ 'Customer.Gender': 'Female' }}
group = {'$group': {'_id': '$month', 'count': {'$sum': 1}}}
result = list(shopcol.aggregate([match, project, group]))
print(result)
#[{'_id': 11, 'count': 189}, {'_id': 12, 'count': 744}, {'_id': 1, 'count': 458}]

# Group Male visits by month.
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

ageRanges = {'$group':{'_id':1, 'totalCustomers':{'$sum':1}, 
                    'Under40': {'$sum':{ "$cond": [ { "$lt": [ "$Age", 40 ] }, 1, 0] }},
                    'Between40and60': { "$sum": {"$cond": [ { "$and": [ { "$gte":  ["$Age", 40 ] }, { "$lte": ["$Age", 60] } ]}, 1, 0] }},
                    'Over60': {'$sum': { "$cond": [ { "$gt": [ "$Age", 60 ] }, 1, 0] }}
                    }}

group = {'$group': {'_id': '$Customer.ID', 'Age': {'$max': "$Customer.Age"}}}
group2 = ageRanges
result = list(shopcol.aggregate([group, group2]))
print(result)

# [{'_id': 1, 'totalCustomers': 1115, 'Under40': 900, 'Between40and60': 161, 'Over60': 54}]

# To compare number of visits by age the ages have been seperated into three 
# groups. Customers under 40, customers between the age of 40 and 60 (inclusive)
# and customers over 60. It is clear that the majority of the visits to this website
# are made by visitors under the age of 40.  

# Further Analysis of customer visits under 40. 
group = {'$group': {'_id': '$Customer.ID', 'Age': {'$max': "$Customer.Age"}}}
group2 = {'$group': {'_id': 1, 'Between20and30': { "$sum": {"$cond": [ { "$and": [ { "$gte":  ["$Age", 20 ] }, { "$lte": ["$Age", 30] } ]}, 1, 0] }}}}
result = list(shopcol.aggregate([group, group2]))
print(result)
# [{'_id': 1, 'Between20and30': 685}]
# 685 / 1115 = 61.43%

# It appears that the majority of the visits to this website are made by people
# between the ages of 20 and 30 (inclusive). This age range accounts for 61.43% 
# percent of the traffic on the website. Therefore, it is important that this
# website continues to cater for this age range moving forward as it 
# appears to be the age range of the websites target audience. 

# =============================================================================
# Customer Analysis - Number of Items Purchased by Gender
# =============================================================================

# Find total number of items purchased.
unwind = {'$unwind':'$Basket'}
group={'$group': {'_id': 1, 'total':{'$sum':'$Basket.Quantity'}}}
totalItems = list(shopcol.aggregate([unwind,group]))
print(totalItems)
# [{'_id': 1, 'total': 513464}]

# Total items purchased by Females.
unwind = {'$unwind':'$Basket'}
group = {'$group': {'_id': '$Customer.Gender', 'total':{'$sum':'$Basket.Quantity'}}}
totalItems = list(shopcol.aggregate([unwind, group]))
print(totalItems)
# [{'_id': 'Male', 'total': 153181}, {'_id': 'Female', 'total': 360283}]

# The above Analysis shows the total number of items bought by gender. It is 
# clear that Females bought more items than Males (over twice as much). Males
# bought a total of 153181 items and females bought a total of 360283 items.
# Confirming that females appear to visit this website more and purchase a 
# larger quantity of items. 

# =============================================================================
# Customer Analysis - Number of Items Purchased by Age
# =============================================================================

# Total Items purchased by the three age groups.
unwind = {'$unwind':'$Basket'}
group = {'$group':{'_id': 1,'totalItemsPurchased':{'$sum':'$Basket.Quantity'}, 
                    'Under40': {'$sum':{ "$cond": [ { "$lt": [ "$Customer.Age", 40 ] }, '$Basket.Quantity', 0] }},
                    'Between40and60': { "$sum": {"$cond": [ { "$and": [ { "$gte":  ["$Customer.Age", 40 ] }, { "$lte": ["$Customer.Age", 60] } ]}, '$Basket.Quantity', 0] }},
                    'Over60': {'$sum': { "$cond": [ { "$gt": [ "$Customer.Age", 60 ] }, '$Basket.Quantity', 0] }},
                    }}
result = list(shopcol.aggregate([unwind,group]))
print(result)
# [{'_id': 1, 'totalItemsPurchased': 513464, 'Under40': 447076, 'Between40and60': 52369, 'Over60': 14019}]

# Further Analysis of Number of Items bought by Age
percentItemsOver40 = ((result[0]['Between40and60'] + result[0]['Over60']) / result[0]['totalItemsPurchased']) *100
print(percentItemsOver40)
# 12.9294361435271

percentItemsUnder40 = (result[0]['Under40'] / result[0]['totalItemsPurchased']) * 100
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

# Find the average value for Male and Females
unwind = {'$unwind':'$Basket'}
group={'$group': {'_id': '$Customer.Gender', 'AverageValue':{'$avg':'$Basket.UnitPrice'}}}
avgPrice = list(shopcol.aggregate([unwind,group]))
print(avgPrice)
# [{'_id': 'Female', 'AverageValue': 3.122939869562101}, {'_id': 'Male', 'AverageValue': 2.929234227002729}]


# Females tend to have a larger average value than Males. This would suggest 
# that on average females purchase more expensive items than males.

# Total value of female and male baskets.
unwind = {'$unwind':'$Basket'}
group={'$group': {'_id': '$Customer.Gender', 'TotalValue':{'$sum':'$Basket.UnitPrice'}}}
totalValue = list(shopcol.aggregate([unwind,group]))
print(totalValue)
# [{'_id': 'Female', 'TotalValue': 87148.76}, {'_id': 'Male', 'TotalValue': 36492.4}]

# The total value of the for females is considerably more than the
# total value for males. This could be caused by either females buying more
# items or by females buying more expensive items. However, it is clear that 
# the baskets belonging to females are more valuable. 

# =============================================================================
# Customer Analysis - Value of Items Purchased by Age
# =============================================================================

# Average value and total value across the three age groups.
unwind = {'$unwind':'$Basket'}
group = {'$group':{'_id': 1,'totalItemsPurchased':{'$sum':'$Basket.UnitPrice'}, 
                    'AvgValueUnder40': {'$avg':{ "$cond": [ { "$lt": [ "$Customer.Age", 40 ] }, '$Basket.UnitPrice', 0] }},
                    'TotalValueUnder40': {'$sum':{ "$cond": [ { "$lt": [ "$Customer.Age", 40 ] }, '$Basket.UnitPrice', 0] }},
                    'AvgValueBetween40and60': { "$avg": {"$cond": [ { "$and": [ { "$gte":  ["$Customer.Age", 40 ] }, { "$lte": ["$Customer.Age", 60] } ]}, '$Basket.UnitPrice', 0] }},
                    'TotalValueBetween40and60': { "$sum": {"$cond": [ { "$and": [ { "$gte":  ["$Customer.Age", 40 ] }, { "$lte": ["$Customer.Age", 60] } ]}, '$Basket.UnitPrice', 0] }},
                    'AvgValueOver60': {'$avg': { "$cond": [ { "$gt": [ "$Customer.Age", 60 ] }, '$Basket.UnitPrice', 0] }},
                    'TotalValueOver60': {'$sum': { "$cond": [ { "$gt": [ "$Customer.Age", 60 ] }, '$Basket.UnitPrice', 0] }}
                    }}
result = list(shopcol.aggregate([unwind,group]))
print(result)
# [{'_id': 1, 'totalItemsPurchased': 123641.16, 
# 'AvgValueUnder40': 2.5678456049945497, 'TotalValueUnder40': 103648.52, 
# 'AvgValueBetween40and60': 0.3880735308690913, 'TotalValueBetween40and60': 15664.2, 
#'AvgValueOver60': 0.1072351600436032, 'TotalValueOver60': 4328.44}]

# The average value is similiar across the three age ranges outlined above. 
# However, it can be noted that visitors in the over 60s age group have a slightly
# higher average value. Visitors under 40 and between 40 and 60 only have a 0.1
# difference in their average value.


# Find total number of documents for visitors less than 40 years old or greater than 60 years old.
group = {'$group': {'_id': '$Customer.ID', 'Age': {'$max': "$Customer.Age"}}}
group2 = {'$group': {'_id': 1, 
                     'TotalUnder40': {'$sum':{ "$cond": [ { "$lt": [ "$Age", 40 ] }, 1, 0] }},
                     'TotalOver60': {'$sum':{ "$cond": [ { "$gt": [ "$Age", 60 ] }, 1, 0] }},
                     }}
result = list(shopcol.aggregate([unwind,group,group2]))
print(result)
# [{'_id': 1, 'TotalUnder40': 900, 'TotalOver60': 54}]

# After reviewing the totalValue for each age group it is clear that the under
# 40s have a larger total value. However, this does not match the average 
# value identified ealier in this section. This may be partily due to the sample
# size as we know that when sample size increases the STD decreases and vice versa.
# The average for the under 40s is also more precise due to the 'Law of Large 
# Numbers'. 

# =============================================================================
# Customer Analysis - Total Spend by Gender
# ============================================================================= 

# Find the total spend of female and male visitors.
unwind = {'$unwind':'$Basket'}
match = {'$match':{'': 'Female'}}
group={'$group': {'_id': '$Customer.Gender',  'totalSpend': {'$sum': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
totalSpend = list(shopcol.aggregate([unwind,group]))
print(totalSpend)
# [{'_id': 'Female', 'totalSpend': 643346.28}, {'_id': 'Male', 'totalSpend': 283904.09}]

# Find the average female and male spend.
unwind = {'$unwind':'$Basket'}
group={'$group': {'_id': '$Customer.Gender',  'avgSpend': {'$avg': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
avgSpend = list(shopcol.aggregate([unwind,group]))
print(avgSpend)
# [{'_id': 'Male', 'avgSpend': 22.7888978969337}, {'_id': 'Female', 'avgSpend': 23.05404859170071}]

# Female visitors clearly spent more than male visitors on this website. Females
# spend a total of 643346.28 while males only spend a total of 283904.09. 

# =============================================================================
# Customer Analysis - Total Spend by Age
# ============================================================================= 

# Total spend under 40s
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$lt': 40} }}
group={'$group': {'_id': '$status',  'totalSpend': {'$sum': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
totalSpend = list(shopcol.aggregate([match,unwind,group]))
print(totalSpend)
# 'totalSpend': 809837.76

# Find the average spend under 40
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$lt': 40} }}
group={'$group': {'_id': '$status',  'avgSpend': {'$avg': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
avgSpend = list(shopcol.aggregate([match,unwind,group]))
print(avgSpend)
# 'avgSpend': 23.882678934796072

# Total spend between 40 and 60 inclusive
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$gte': 40, '$lte': 60} }}
group={'$group': {'_id': '$status',  'totalSpend': {'$sum': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
totalSpend = list(shopcol.aggregate([match,unwind,group]))
print(totalSpend)
# 'totalSpend': 94601.73

# Find the average spend between 40 and 60 inclusive
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$gte': 40, '$lte': 60} }}
group={'$group': {'_id': '$status',  'avgSpend': {'$avg': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
avgSpend = list(shopcol.aggregate([match,unwind,group]))
print(avgSpend)
# 'avgSpend': 18.52030736100235

# Total spend over 60
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$gt': 60} }}
group={'$group': {'_id': '$status',  'totalSpend': {'$sum': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
totalSpend = list(shopcol.aggregate([match,unwind,group]))
print(totalSpend)
# 'totalSpend': 22810.88

# Find the average spend over 60
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$gt': 60} }}
group={'$group': {'_id': '$status',  'avgSpend': {'$avg': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
avgSpend = list(shopcol.aggregate([match,unwind,group]))
print(avgSpend)
# 'avgSpend': 16.93458054936897

# The age group that spent the most while visiting this website appears to be 
# the under 40s with a total spend of 809837.76 and an average spend of 23.88.  

# =============================================================================
# Customer Analysis - Summary
# ============================================================================= 

'''
The information above is all extremely useful and tells us a lot about the 
total spend and value by gender and age. However, what may be more useful to a 
business such as the one hosting this website is the customer profile and 
demographic that the above analysis identifies and builds. Through the above analysis 
I could now inform this business that their target audience is females aged 20 to 40
and that over three months this demographic is likely to spend up to 80k via their 
website. This business can then create a strategy based of this information to target 
this demographic. Alternatively a strategy could be created to increase catchment
of males under 40 or both males and females over 40 etc. However, that is a business
decision based on the direction of the company. 
'''









