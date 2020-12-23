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

# Discovered how to produce age bins via research on stack overflow:
# https://stackoverflow.com/questions/42435584/group-by-age-groups-in-mongo-db
# has been adapted to suit my needs.
    
# Find unique customer visits by age, along with max and min age.
ageRanges = {'$group':{'_id':1, 'totalCustomers':{'$sum':1}, 
                    'Under40': {'$sum':{ "$cond": [ { "$lt": [ "$Age", 40 ] }, 1, 0] }},
                    'Between40and60': { "$sum": {"$cond": [ { "$and": [ { "$gte":  ["$Age", 40 ] }, { "$lte": ["$Age", 60] } ]}, 1, 0] }},
                    'Over60': {'$sum': { "$cond": [ { "$gt": [ "$Age", 60 ] }, 1, 0] }},
                    'MaxAge':{'$max':'$Age'},
                    'MinAge':{'$min':'$Age'}
                    }}

group = {'$group': {'_id': '$Customer.ID', 'Age': {'$max': "$Customer.Age"}}}
group2 = ageRanges
result = list(shopcol.aggregate([group, group2]))
print(result)

# [{'_id': 1, 'totalCustomers': 1115, 'Under40': 900, 'Between40and60': 161, 'Over60': 54, 'MaxAge': 80, 'MinAge': 18}]

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
# (685 / 1115) * 100 = 61.43%

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

# Total items purchased by Males and Females.
unwind = {'$unwind':'$Basket'}
group = {'$group': {'_id': '$Customer.Gender', 'total':{'$sum':'$Basket.Quantity'}}}
totalItems = list(shopcol.aggregate([unwind, group]))
print(totalItems)
# [{'_id': 'Female', 'total': 360283}, {'_id': 'Male', 'total': 153181}]

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

# Find the average, max and min value for Male and Females
unwind = {'$unwind':'$Basket'}
group={'$group': {'_id': '$Customer.Gender', 'AverageValue':{'$avg':'$Basket.UnitPrice'}, 'MinValue':{'$min':'$Basket.UnitPrice'},
                  'MaxValue':{'$max':'$Basket.UnitPrice'}
                  }}
avgPrice = list(shopcol.aggregate([unwind,group]))
print(avgPrice)
# [{'_id': 'Male', 'AverageValue': 2.929234227002729, 'MinValue': 0.12, 'MaxValue': 295}, 
# {'_id': 'Female', 'AverageValue': 3.122939869562101, 'MinValue': 0.07, 'MaxValue': 295}]

# Large difference between the most expensive and cheapest item.

# Females tend to have a larger average value than Males. This would suggest 
# that on average females purchase more expensive items than males.

# Average number of different items in a basket by Gender
project = {'$project': {'_id': 0, 'Gender':'$Customer.Gender', 'NumberOfItems': {'$size': '$Basket'}}}
group = {'$group': {'_id': '$Gender', 'Average':{'$avg':'$NumberOfItems'}}}
result = list(shopcol.aggregate([project,group]))
print(result)
# [{'_id': 'Female', 'Average': 20.061826024442848}, 
# {'_id': 'Male', 'Average': 20.45648604269294}]

# The Average number of items per basket are relatively similiar for both Males
# and females, with males having slightly more items on average in their basket.

# =============================================================================
# Customer Analysis - Value of Items Purchased by Age
# =============================================================================

# Average value across the three age groups.
unwind = {'$unwind':'$Basket'}
group = {'$group':{'_id': 1,'totalValue':{'$sum':'$Basket.UnitPrice'}, 
                    'AvgValueUnder40': {'$avg':{ "$cond": [ { "$lt": [ "$Customer.Age", 40 ] }, '$Basket.UnitPrice', '$$REMOVE'] }},
                    'AvgValueBetween40and60': { "$avg": {"$cond": [ { "$and": [ { "$gte":  ["$Customer.Age", 40 ] }, { "$lte": ["$Customer.Age", 60] } ]}, '$Basket.UnitPrice', '$$REMOVE'] }},
                    'AvgValueOver60': {'$avg': { "$cond": [ { "$gt": [ "$Customer.Age", 60 ] }, '$Basket.UnitPrice', '$$REMOVE'] }}
                    }}
result = list(shopcol.aggregate([unwind,group]))
print(result)
# [{'_id': 1, 'totalValue': 123641.16, 'TotalValueUnder40': 3.0566669615736237, 
# 'TotalValueBetween40and60': 3.0666014095536416, 'TotalValueOver60': 3.2133927245731253}]

# After reviewing the average value for each age group it is clear that the over 
# 60's have a large average value. 

unwind = {'$unwind':'$Basket'}
group = {'$group':{'_id': 1,'totalValue':{'$sum':'$Basket.UnitPrice'}, 
                    'maxValueUnder40': {'$max':{ "$cond": [ { "$lt": [ "$Customer.Age", 40 ] }, '$Basket.UnitPrice', '$$REMOVE'] }},
                    'maxValueBetween40and60': { "$max": {"$cond": [ { "$and": [ { "$gte":  ["$Customer.Age", 40 ] }, { "$lte": ["$Customer.Age", 60] } ]}, '$Basket.UnitPrice', '$$REMOVE'] }},
                    'maxValueOver60': {'$max': { "$cond": [ { "$gt": [ "$Customer.Age", 60 ] }, '$Basket.UnitPrice', '$$REMOVE'] }},
                    'minValueUnder40': {'$min':{ "$cond": [ { "$lt": [ "$Customer.Age", 40 ] }, '$Basket.UnitPrice', '$$REMOVE'] }},
                    'minValueBetween40and60': { "$min": {"$cond": [ { "$and": [ { "$gte":  ["$Customer.Age", 40 ] }, { "$lte": ["$Customer.Age", 60] } ]}, '$Basket.UnitPrice', '$$REMOVE'] }},
                    'minValueOver60': {'$min': { "$cond": [ { "$gt": [ "$Customer.Age", 60 ] }, '$Basket.UnitPrice', '$$REMOVE'] }}
                    }}
result = list(shopcol.aggregate([unwind,group]))
print(result)
# [{'_id': 1, 'totalValue': 123641.16, 'maxValueUnder40': 295, 
# 'maxValueBetween40and60': 175, 'maxValueOver60': 165, 'minValueUnder40': 0.07, 
# 'minValueBetween40and60': 0.12, 'minValueOver60': 0.14}]

# Customers under 40 appear to have a higher max value than the other two age groups.
# However, the same age group also have the lowest min value.

# =============================================================================
# Customer Analysis - Total Spend by Gender
# ============================================================================= 

# Find the total spend of female and male visitors.
unwind = {'$unwind':'$Basket'}
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
# spend a total of 643346.28 while males only spent a total of 283904.09. 

# =============================================================================
# Customer Analysis - Total Spend by Age
# ============================================================================= 

# Find the total spend across the three age groups.
unwind = {'$unwind':'$Basket'}
group = {'$group':{'_id': 1,'totalSpend':{'$sum':{ '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}, 
                    'TotalSpendUnder40': {'$sum':{ "$cond": [ { "$lt": [ "$Customer.Age", 40 ] }, { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}, 0] }},
                    'TotalSpendBetween40and60': { "$sum": {"$cond": [ { "$and": [ { "$gte":  ["$Customer.Age", 40 ] }, { "$lte": ["$Customer.Age", 60] } ]}, { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}, 0] }},
                    'TotalSpendOver60': {'$sum': { "$cond": [ { "$gt": [ "$Customer.Age", 60 ] }, { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}, 0] }}
                    }}
result = list(shopcol.aggregate([unwind,group]))
print(result)
# [{'_id': 1, 'totalSpend': 927250.37, 'TotalSpendUnder40': 809837.76, 
# 'TotalSpendBetween40and60': 94601.73, 'TotalSpendOver60': 22810.88}]

# Find the average spend under 40
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$lt': 40} }}
group={'$group': {'_id': '$status',  'avgSpend': {'$avg': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
avgSpend = list(shopcol.aggregate([match,unwind,group]))
print(avgSpend)
# 'avgSpend': 23.882678934796072

# Find the average spend between 40 and 60 inclusive
unwind = {'$unwind':'$Basket'}
match = {'$match':{ 'Customer.Age': {'$gte': 40, '$lte': 60} }}
group={'$group': {'_id': '$status',  'avgSpend': {'$avg': { '$multiply': [ '$Basket.UnitPrice', '$Basket.Quantity' ]}}}}
avgSpend = list(shopcol.aggregate([match,unwind,group]))
print(avgSpend)
# 'avgSpend': 18.52030736100235

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
# Customer Analysis - Additional Analysis
# ============================================================================= 

####### Repeat Customers ########

# Find total number of repeating customers.
group = {'$group': {'_id': '$Customer.ID', 'NumberVisits': {'$sum': 1}}}
group2 = {'$group': {'_id': 1, 'TotalRepeatingCustomers': {'$sum':{ "$cond": [ { "$gt": [ "$NumberVisits", 1 ] }, 1, 0]}}}}
project = {'$project': {'_id': 1, 'TotalRepeatingCustomers': 1, "Percent": {'$multiply': [{'$divide': ['$TotalRepeatingCustomers',2000]}, 100]}}}
result = list(shopcol.aggregate([group,group2,project]))
print(result)
#[{'_id': 1, 'TotalRepeatingCustomers': 389, 'Percent': 19.45}]

# 389 visitors revisited the site this accounts for 19.45%
# these repeating customers account for 19.45% of the sites 
# visits.

# Find repeating customers by gender
group = {'$group': {'_id': '$Customer.ID', 'NumberVisits': {'$sum': 1}, 'Gender': {'$max': "$Customer.Gender"}}}
group2 = {'$group': {'_id': '$Gender', 'TotalRepeatingCustomers': {'$sum':{ "$cond": [ { "$gt": [ "$NumberVisits", 1 ] }, 1, 0]}}}}
result = list(shopcol.aggregate([group,group2]))
print(result)
# [{'_id': 'Female', 'TotalRepeatingCustomers': 217}, 
# {'_id': 'Male', 'TotalRepeatingCustomers': 172}]
# Female customers tend to revisit the site more than male customers.

####### Customer Nationality ########

# Find top 3 countries
group = {'$group': {'_id': '$Customer.ID', 'Country':{'$max':'$Customer.Country'}}}
group2 = {'$group':{ '_id':'$Country','NumberOfCustomers': {'$sum': 1}}}
sort={'$sort':{'NumberOfCustomers':-1}}
limit = {'$limit':3}
result = list(shopcol.aggregate([group,group2,sort, limit]))
print(result)
# [{'_id': 'United Kingdom', 'NumberOfCustomers': 1010}, 
# {'_id': 'France', 'NumberOfCustomers': 27}, 
# {'_id': 'Germany', 'NumberOfCustomers': 27}]

# The majority of the visitors visiting this website are from the United Kingdom 
# followed by France and Germany.

group = {'$group': {'_id': '$Customer.ID', 'Country':{'$max':'$Customer.Country'}}}
group2 = {'$group':{ '_id':'$Country','NumberOfCustomers': {'$sum': 1}}}
sort={'$sort':{'NumberOfCustomers':1}}
limit = {'$limit':3}
result = list(shopcol.aggregate([group,group2,sort, limit]))
print(result)
# [{'_id': 'Cyprus', 'NumberOfCustomers': 1}, 
# {'_id': 'Poland', 'NumberOfCustomers': 1}, 
# {'_id': 'Channel Islands', 'NumberOfCustomers': 1}]

# Cyprus, Poland and the Channel Islands only display 1 visitor to the site
# therefore the business could decided to promote their products in these locations 
# to promote reach and encourage engagement from these countries.


# =============================================================================
# Customer Analysis - Summary
# ============================================================================= 

'''
The information above is all extremely useful and tells us a lot about the 
total spend and value by gender and age. However, what may be more useful to a 
business such as the one hosting this website is the customer profile and 
demographic that the above analysis identifies and builds. Through the above analysis 
I could now inform this business that their target audience is females aged 20 to 40
from the United Kingdom and that visitors under 40 generate appoximately 80k in sales
over the course of three months. 

I can also inform them that the three countries that use their site the least are 
Cyprus, Poland and the Channel Islands. 

The retention/revist rate has also been identified and this can be used to make
assumptions about customer loyalty, satisfaction and the expenditure regarding the
acquisition of new customers.

This business can then create a strategy based of this information to target 
this demographic. Alternatively a strategy could be created to increase catchment
of males under 40 or both males and females over 40 etc. However, that is a business
decision based on the direction of the company. 
'''









