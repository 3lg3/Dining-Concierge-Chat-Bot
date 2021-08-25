import json


yelp_data = open('yelp_data' , 'r')
es_data = open('es_data.json', 'w')
rows = yelp_data.readlines()
key_set = set()
count = 1
for row in rows:
    row_dict = json.loads(row)
    row_id = row_dict['id']['s']
    row_type = row_dict['cuisine']['s']
    first = '{"index": { "_index": "restaurants", "_type": "restaurant", "_id": ' + str(count) + '}}'
    second = json.dumps({'id': row_id, 'cuisine': row_type})
    es_data.write(first + '\n')
    es_data.write(second + '\n')
    count += 1



