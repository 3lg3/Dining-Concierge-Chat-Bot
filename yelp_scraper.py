import requests
import json
import boto3
import datetime

API_KEY = 'MY-API-KEY'

url = 'https://api.yelp.com/v3/businesses/search'


def lambda_handler(event, context):
    # TODO implement
    db = boto3.resource('dynamodb')
    table = db.Table('yelp-restaurants')
    cuisines = ['Chinese', 'Japanese', 'Korean', 'Italian', 'French', 'Mexican']
    response = populate_table(table, 'Manhattan', cuisines)
    return response


def populate_table(table, location, cuisines):
    response = ''
    header = {'Authorization': 'Bearer %s' % API_KEY}
    # dynamo_data = []
    term = '{} restaurants'.format('chinese')
    keys = set()
    for cuisine in cuisines:
        term = '{} restaurants'.format(cuisine)
        params = {'term': term, 'location': location, 'limit': 50, 'offset': 0}
        check_count = requests.get(url, params=params, headers=header)
        rest_count = json.loads(check_count.text).get('total')
        # limit the number of restaurant for each cuisine to 1000 or less
        if rest_count > 1000:
            rest_count = 1000

        for offset in range(0, rest_count, 50):
            params['offset'] = offset
            raw_rows = requests.get(url, params=params, headers=header)
            rows = json.loads(raw_rows.text)['businesses']
            for row in rows:
                # check if the restaurant already exists
                if row['id'] in keys:
                    continue
                keys.add(row['id'])
                curr = {'id': row['id'],
                        'name': row['name'],
                        'insertedAtTimestamp': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f'),
                        'cuisine': cuisine
                        }
                address = ''
                if row['location']['address1'] is not None:
                    address += row['location']['address1']
                if row['location']['address2'] is not None:
                    address += ' ' + row['location']['address2']
                if row['location']['address3'] is not None:
                    address += ' ' + row['location']['address3']
                curr['address'] = address
                lat = row['coordinates']['latitude']
                long = row['coordinates']['longitude']
                if lat is not None and long is not None:
                    curr['coordinates'] = {'latitude': str(lat), 'longitude': str(long)}
                if row['review_count'] is not None:
                    curr['numberofreviews'] = row['review_count']
                if row['rating'] is not None:
                    curr['rating'] = str(row['rating'])
                if row['location']['zip_code'] is not None:
                    curr['zip'] = row['location']['zip_code']
                response = table.put_item(Item=curr)

    return response
