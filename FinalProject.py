import lxml.etree as ET
from collections import defaultdict
import pprint
import re
import codecs
import json
from pymongo import MongoClient

expected_street_types = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", "Crescent", 'Terrace', 'Way', 'Circle', 'Sideroad', 'Line', 'Grove', 'Gate', 'Gardens']
expected_addr_types = ["street", "city", "housenumber", "postcode", "province", "Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons"]
directions = ['North', 'West', 'East', 'South']
mapping_Street = { "St": "Street",
            "St.": "Street",
            "STREET":"Street",
            "street":"Street",
            "Rd.": "Road",
            "Ave.": "Avenue",
            "ave": "Avenue",
            "Ave": "Avenue",
            "Dr.": "Drive",
            "Dr":"Drive",
            "Pkwy":"Parkway",
            "Blvd.": 'Boulevard',
            "Blvd": 'Boulevard',
            "blvd": 'Boulevard',
            "BLVD": 'Boulevard',
            "BLVD.": 'Boulevard',
            "Cres": "Crescent",
            "Cres.": "Crescent",
            "Grv": 'Grove',
            "Grv.": 'Grove'}   

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]
mapping_Orientation = { "N.": "North",
			"N": "North",
            "S.": "South",
            "S": "South",
            "E.":"East",
            "E":"East",
            "W.": "West",
            "W": "West",
            }

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
street_types = defaultdict(set)

	
tagCounts =  {'node': 0, 'tag': 0, 'nd': 0, 'bounds': 0, 'way': 0, 'relation':0, 'member':0}
problemAttrib = []
otherattrib = []
keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
abnormal_street_type_counts = {}

postcode_format = r"[A-Z][0-9][A-Z] [0-9][A-Z][0-9]$"

def shape_element(element):
    node = {
        "id":"" ,
        "type": "",
        "visible":"",
        "created": {
                  "version":"",
                  "changeset":"",
                  "timestamp":"",
                  "user":"",
                  "uid":""
                },
        "pos": [],

        "address": {
                  "housenumber": "",
                  "postcode": "",
                  "street": ""
                },

        "amenity": "",
        "cuisine": "",
        "name": "",
        "phone": ""
        }
    
    node = {'created':{}}
    address = {}
    node_keys = ['id', 'visible', 'amenity', 'cuisine', 'name', 'phone']
    if element.tag == "node" or element.tag == "way" :
        
            
        # YOUR CODE HERE
        node['type'] = element.tag
        attr = element.attrib
        
        if element.tag == 'way':
            node['node_refs'] = []
            for nd in element.iter("nd"):
                if 'ref' in nd.attrib:
                    node['node_refs'].append(nd.attrib['ref'])
        
        for tag in element.iter("tag"):
            if 'k' in tag.attrib:
                #print "k is", tag.attrib['k'], "and v is", tag.attrib['v']
                if(problemchars.search(tag.attrib['k'])):
                    continue
                elif tag.attrib['k'] in node_keys:
                    #print "aloooooooooooo"
                    node[tag.attrib['k']] = tag.attrib['v']
                
                elif(re.search(r"addr:(\S+)",tag.attrib['k'] )):
                    #print "search reesult is,", re.search("addr:(\S+)",tag.attrib['k']).group(1)    
                    match = re.search("addr:(\S+)",tag.attrib['k'] )
                    if match.group(1) in expected_addr_types:
                        if match.group(1) == 'street':
                            street_cleaned = clean_street_name(tag.attrib['v']) #clean street types if some abbreviations have been used
                            address[match.group(1)] = street_cleaned
                        elif match.group(1) == 'postcode':
                            if(tag.attrib['v']):
                                postcode_clearned = clean_postcode(tag.attrib['v'])
                        else:
                        #print "booooooooooooogh"
                            address[match.group(1)] = tag.attrib['v']
                
                
        
              
        if(address):
            node['address'] = {}
            for key in address:
                if address[key]:
                    node['address'][key] = address[key]
            
        
        #print(element.tag)
        
        for key in attr:
            if key in node_keys:
                node[key] = attr[key]
               
            elif key in CREATED:
                #node['created'] = {}
                node['created'][key] = attr[key]
                continue
            elif key == 'lat':
                node['pos'] = [float(attr['lat']), float(attr['lon'])]
                
            
            
            
        
        return node
    else:
        return None


def clean_postcode(codepost):
    
    cleaned_code = codepost.strip().upper()
    
    if len(cleaned_code)==6:
        cleaned_code = cleaned_code[:3] + " " + cleaned_code[-3:] 

    match = re.match(postcode_format, cleaned_code)
    if match: 
        return cleaned_code
    else:
        print codepost



def clean_street_name(street_name): #translate the abbreviations to full names
    cleaned_name = street_name

    if len(street_name.strip().split(" ") ) <= 1:
        return cleaned_name

    street_type = street_name.strip().split(" ")[-1] #last word in the street
    if street_type in mapping_Orientation.keys() or street_type in mapping_Orientation.values(): #Check if the street name has orientation as well
        if street_types in mapping_Orientation.keys():
            #print "street orientation cleaning required", street_name
            cleaned_name = cleaned_name.replace(street_type, mapping_Orientation[street_type] )
        street_type = street_name.strip().split(" ")[-2] #the word before last word in street name

    
    if street_type in mapping_Street:
        #print "street type clearning required", street_name
        cleaned_name = cleaned_name.replace(street_type, mapping_Street[street_type] )
        #print "new name is", cleaned_name
        

    return street_name

def start(elem):   # Called for each element
	tag = elem.tag 
	attrib = elem.attrib
	key_type(tag, attrib)  #check if there is some bad chars with attribs
	count_tags(elem)	#count to check how many different types of tag there is
	audit(elem)
    
            
    

def write_json(elem, fo, pretty=False): #import the element in a json file
    if elem:
        if pretty:
            fo.write(json.dumps(elem, indent=2)+"\n")
        else:
            fo.write(json.dumps(elem) + "\n")

def count_ways( street_name): # check street types 
    street_type = street_name.strip().split(" ")[-1] #last word in the street
    if street_type in mapping_Orientation.keys() or mapping_Orientation.values(): #Check if the street name has orientation as well
        if street_types in mapping_Orientation.keys():
            print "street orientation cleaning required", street_name
        street_type = street_name.strip().split(" ")[-2] #the word before last word in street name

	
    if street_type in mapping_Street:
        print "street type clearning required", street_name
    if not street_type in expected_street_types: #keep street types that are not in expected list in a dictionary for further investigation
        if street_type in abnormal_street_type_counts:
            abnormal_street_type_counts[street_type] += 1
        else:
            abnormal_street_type_counts[street_type] = 1

def count_tags( elem):

    if elem.tag in tagCounts:
        tagCounts[elem.tag] +=1
    else:
        tagCounts[elem.tag] =1
    
def key_type( tag, attrib):
    if tag == "tag":
        for key in attrib:
            if key == 'k':
                k_elem = attrib[key]
                if(lower.match(k_elem)):
                    keys['lower'] += 1
                elif(lower_colon.match(k_elem)):
                    keys['lower_colon'] += 1
                elif(problemchars.search(k_elem)):
                    keys['problemchars'] += 1
                    problemAttrib.append(k_elem)
                else:
                    keys['other'] += 1
                    if not k_elem.startswith(('canvec', 'turn', 'geobase')):
                    	if 'FIXME' in k_elem:
                            otherattrib.append(k_elem)
                        pass
                    

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected_street_types:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(elem):
    
    
    #if elem.tag == "node" or elem.tag == "way":
    for tag in elem.iter("tag"):
		#print "tag", tag.attrib
        if tag.attrib and is_street_name(tag):
            #print tag.attrib['v']
            audit_street_type(street_types, tag.attrib['v'])

    return street_types


def close():    # Called when all data has been parsed.
    return tagCounts, keys, problemAttrib, otherattrib, abnormal_street_type_counts




lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')


def get_db():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    # 'examples' here is the database name. It will be created if it does not exist.
    db = client.torontomap
    print "toronto db is created"
    return db

def xml_parser(filename):
        counter = 0
        dictionary = {}
        db = get_db()
        file_out = "{0}.test.toronto.json".format(filename)
        fo = codecs.open(file_out, "w")
        for event, elem in ET.iterparse(filename, events=("start", )):
            counter +=1
            #print 'Processing {e}'.format(e=ET.tostring(elem))
            #func(elem, *args, **kwargs)
            #print 'Clearing {e}'.format(e=ET.tostring(elem))
            start(elem)
    
            json_elem = shape_element(elem)
            if json_elem:
                write_json(json_elem, fo)
                #insert_in_mongo(json_elem, db)
            
            elem.clear()
                
            while elem.getprevious() is not None:
                #print('Deleting {p}'.format(p=(elem.getparent()[0]).tag))
                del elem.getparent()[0]
		    
        fo.close()
        
        
        print counter
def insert_in_mongo(elem, db):
    
    db.map.insert(elem)



if __name__ == "__main__":
    filepath = "C:\Users\Mo\Desktop\MOOC-Courses\Udacity-Data Wrangling\\final project\\toronto_canada.osm\\toronto_canada.osm"
    

    #process_map(filepath)
    #print count_tags(filepath)
    #keys = process_map(filepath)
    #xml_parser(ET.iterparse(filepath))
    xml_parser(filepath)

    #print count_tags3(filepath)

    tagcounts, keys, problems, others, ways = close()
    print "Tag Counts:", tagcounts
    print "Keys:", keys
    print "Problem Attrib:", problems
    print "Other Attrib:", others
    print "wa counts:", ways
    #print "street types", street_types
    #pprint.pprint(keys)