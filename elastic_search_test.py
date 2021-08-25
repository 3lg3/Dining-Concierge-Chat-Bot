import requests
ENDPOINT = 'https://search-restaurants-qwj66xmt3m5nnscivcr3kpytfq.us-east-1.es.amazonaws.com'

url = ENDPOINT + '/_search?size=3&&q=cuisine:' + 'chinese'
response = requests.get(url, auth=('MASTER_NAME', 'PASSWORD')).json()


results = response['hits']['hits']
rest_id = []
for result in results:
    rest_id.append(result["_source"]["id"])

print(rest_id)
