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

project = {'$project': {'month': {'$month': {'$toDate': '$InvoiceDate'}}}}
match = {'$match':{ 'Customer.Gender': 'Female' }}
group = {'$group': {'_id': '$month', 'count': {'$sum': 1}}}
result = list(shopcol.aggregate([match, project, group]))
print(result)
#[{'_id': 1, 'count': 458}, {'_id': 12, 'count': 744}, {'_id': 11, 'count': 189}]

project = {'$project': {'month': {'$month': {'$toDate': '$InvoiceDate'}}}}
match = {'$match':{ 'Customer.Gender': 'Male' }}
group = {'$group': {'_id': '$month', 'count': {'$sum': 1}}}
result = list(shopcol.aggregate([match, project, group]))
print(result)
#[{'_id': 12, 'count': 308}, {'_id': 11, 'count': 57}, {'_id': 1, 'count': 244}]
