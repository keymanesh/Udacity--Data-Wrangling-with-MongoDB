# -*- coding: utf-8 -*-
import pprint

def get_db():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    # 'examples' here is the database name. It will be created if it does not exist.
    db = client.torontomap3
    return db


def add_json(filename, db):
	with open(filename) as json_f:
		for line in json_f:
			db.torontomap.insertjson.load(line)

if __name__ == "__main__":

	db = get_db() # uncomment this line if you want to run this locally
	filename = "C:\Users\Mo\Desktop\MOOC-Courses\Udacity-Data Wrangling\\final project\\toronto_canada.osm\\toronto_canada.osm.2.json"

	
	#print "count ways", db.map.find({"type":"way"}).count()
	
	
	print "disticnt users", len(db.map.distinct("created.user"))
	
	
	
	#Query for top user/s
	query_topusers = [{"$group":{"_id":"$created.user", "count":{"$sum":1}}}, {"$sort":{"count":-1}}, {"$limit":3}]
	topusers = db.map.aggregate(query_topuser)
	data = [[x] for x in topusers]
	pprint.pprint(data)
	

	
	#Query top ameneties
	query_ameneties = [{"$match":{"amenity":{"$exists":1}}}, {"$group":{"_id":"$amenity","count":{"$sum":1}}}, {"$sort":{"count":-1}}, {"$limit":10}]
	topameneties =  db.map.aggregate(query_ameneties)
	data = [[x] for x in topameneties]
	pprint.pprint(data)
	

	# print "no. of documents" , db.map.find().count()                                             

	
    # Sort cities by count, descending
	query_cities = [{"$match":{"address.city":{"$exists":1}}}, {"$group":{"_id":"$address.city", "count":{"$sum":1}}}, {"$sort":{"count":-1}}, {"$limit":3}]
	cities = db.map.aggregate(query_cities)
	data = [[x] for x in cities]
	pprint.pprint(data)
	print "no cities". len(data)
	
	#print get_city(db)