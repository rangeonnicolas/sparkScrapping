import json
from urllib import urlopen

first_json_url = "URL TO THE FIRST JSON"


def get_json_from_url(url):
	try:
		response = urlopen(url)
	except IOError:
		raise Exception("The file {0} seems to be unavailable".format(url))
		json_response = json.load(response)
	except:
		raise Exception("The JSON file {0} seems to be malformed".format(url))
	return json_response

def get_url_depth2(json):
	if not json is None:
		return json['resource']['assortedCustomerChoice']['_links']['self']['href']
	else:
		return None

def get_url_depth3(json):
	if not json is None:
		resource = json['resource']
		market = resource['merchandisingStrategy']['market']
		channel = resource['merchandisingStrategy']['channel']
		brand = resource['merchandisingStrategy']['brand']
		tbas = resource['rollingDistro']['timeBasedAssortmentStrategies']
		store_counts = []
		for s in tbas:
			store_counts += [s['value']['storeCount']]
		return {'market':market, 'channel':channel, 'brand': brand, 'store_counts': store_counts}
	else:
		return None

sc=SparkContext()
sqlc=HiveContext(sc)
sc.setLogLevel("ERROR")

json1 = json.load(urlopen(first_json_url,'r'))
urls_depth1 = [entry['_links']['source']['href'] for entry in json1['resource']['entries']]
ud1_rdd = sc.parallelize(urls_depth1,10)

result = ud1_rdd.map(get_json_from_url).map(get_url_depth2).map(get_json_from_url).map(get_url_depth3).collect()

print(result)




