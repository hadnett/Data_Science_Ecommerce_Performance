#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 22:34:10 2020

@author: williamhadnett
"""

import pymongo
import os
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
