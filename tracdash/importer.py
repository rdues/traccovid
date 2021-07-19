
import json, re, logging, gzip, pytz
import multiprocessing
from datetime import datetime
from pprint import pprint
from collections import Counter, deque

from elasticsearch import Elasticsearch

from . import helpers
from .exceptions import BulkInsertException
from . import unicodetokeniser
from . import stopwords, stopsources



MAX_DOCS_SIZE = 50000
MAX_BODY_SIZE = int(100000000 / 4)
STOPWORDS = stopwords.STOPWORDS_EN
STOPSOURCES = stopsources.STOPSOURCES

_es_ips = None
_index_name = None
_pool_size = None
_geo_helper = None
_geo_search_level = 0



def import_files(files, es_ips, index_name, pool_size = 16, geo_level = 0):
	"""
	Take a list of paths to jsonl.gz files for import,
	along with a list of ElasticSearch ip:port locations
	and the name of the ElasticSearch index to update.
	Files are processed in parallel using a pool of size pool_size.
	Geo boundary processing (i.e. assigning regions to tweets) is turned on if geo_level is set 
	(values 1, 2 or 3; corresponding to NUTS levels).
	
	First the index is created to ensure the correct type for each field.
	Then files are processed in parallel.
	Each tweet is processed in turn for each file, including embedded retweets and quote tweets.
	Tweets inserted into database when MAX_DOCS_SIZE is reached.
	ElasticSearch bulk called when body size reaches MAX_BODY_SIZE
	Duplicate tweets (with identical IDs) overwrite tweets in the ElasticSearch database.
	"""
	global _es_ips, _index_name, _pool_size, _geo_helper, _geo_search_level
	
	helpers.init_tokeniser()
	
	_geo_search_level = geo_level
	_geo_helper = helpers.init_geo(_geo_search_level)
	
	_es_ips = es_ips
	_index_name = index_name
	_pool_size = pool_size
	
	logging.info("creating index")
	_create_index()
	logging.info("index created")
	
	logging.info("starting import")
	
	with multiprocessing.get_context('fork').Pool(_pool_size) as pool:
		result = pool.map(_process_file, files)

	logging.info("import finished\n{}".format( "\n".join(result) ))



def _process_file(file):
	logging.info("starting file\t{}".format(file))
	
	es = None
	docs = []
	insert_count = 0
	tweet_count = 0
	
	try:
		es = Elasticsearch(_es_ips, timeout=(60*60))
	except:
		logging.exception("elasticsearch error\t{}".format(file))
		return "! " + file
	
	try:
		with gzip.open(file) as f:
			for line in f:
				line = line.strip()
				if line:
					try:
						tweet = json.loads(line)
					except:
						logging.exception("json parse error\t{}".format(line))
						raise
				
					if 'info' in tweet and 'activity_count' in tweet['info']:
						continue
					
					if 'lang' in tweet and tweet['lang'] == 'en':				
						_process_tweet(tweet, docs)
					else:
						logging.warning("no lang field\t{}".format(line))
					
					if len(docs) > MAX_DOCS_SIZE:
						insert_count += 1
						tweet_count += _insert_docs(es, docs, file, insert_count)
					
		
			if len(docs) > 0:
				tweet_count += _insert_docs(es, docs, file, 0)
	
	except:
		logging.exception("file error\t{}".format(file))
		return "! " + file
	
	logging.info("file finished\t{}\t{}".format(file, tweet_count))
	
	return "+ " + file



def _process_tweet(tweet, docs):
	try:
		# this really should be split into separate functions
		# but parts of the process depend on prior processed data from the tweet
	
		is_truncated = False
		has_extended = False
		extended_tweet = None
		
		anon_text = ""
		computed_text = ""
		types = Counter()
		unfiltered_types = Counter()
		bi_grams = Counter()
		tri_grams = Counter()
		
		username = '-'
		anon_profile = ""
		profile_types = Counter()
		unfiltered_profile_types = Counter()
		user_verified = False
		user_followers_count = 0
		user_friends_count = 0
		user_listed_count = 0
		user_favourites_count = 0
		user_statuses_count = 0
		user_created_timestamp = 0
		
		hashtags = Counter()
		links = Counter()
		simple_links = Counter()
		unwound_links = Counter()
		link_titles = Counter()
		link_title_types = Counter()
		link_description_types = Counter()
		websites = Counter()
		simple_websites = Counter()
		unwound_websites = Counter()
		mentions = Counter()
		media_files = Counter()
		media_urls = Counter()
		media_formats = Counter()
		media_websites = Counter()
		symbols = Counter()
	
		tweet_lng = None
		tweet_lat = None
		tweet_nuts_level = 0
		tweet_nuts1_code = None
		tweet_nuts1_name = None
		tweet_nuts2_code = None
		tweet_nuts2_name = None
		tweet_nuts3_code = None
		tweet_nuts3_name = None
		tweet_geo_desc = ''
	
		user_lng = None
		user_lat = None
		user_nuts_level = 0
		user_nuts1_code = None
		user_nuts1_name = None
		user_nuts2_code = None
		user_nuts2_name = None
		user_nuts3_code = None
		user_nuts3_name = None
		user_geo_desc = ''
		
		geo_source = None
		geo_nuts_level = 0
		geo_lng = None
		geo_lat = None
		geo_nuts1_code = None
		geo_nuts1_name = None
		geo_nuts2_code = None
		geo_nuts2_name = None
		geo_nuts3_code = None
		geo_nuts3_name = None
	
		tweet_id = None
	
		is_reply = False
		reply_to = None
		reply_to_username = None
	
		is_quote = False
		qtweet = None
		quoted_id = None
		quoted_username = None
	
		is_retweet = False
		retweet = None
		retweeted_id = None
		retweeted_username = None
	
		quote_count = 0
		reply_count = 0
		retweet_count = 0
		favorite_count = 0
	
		timestamp = None
		
		source = None
		
		user_connections = []
	
	
		# source (filter out posts by bots)
		
		if 'source' in tweet:
			source = tweet['source']
			
			if source in STOPSOURCES:
				logging.info("excluded - based on source\t{}".format(source))
				return		# don't process embedded tweets
		
	
		# truncated
		
		is_truncated = tweet['truncated']
		
		if 'extended_tweet' in tweet:
			extended_tweet = tweet['extended_tweet']
			has_extended = True
	
		
		# ids and replies
	
		tweet_id = tweet['id_str']
	
		username = helpers.hash('uname', tweet['user']['screen_name'])
	
		if 'in_reply_to_status_id_str' in tweet and tweet['in_reply_to_status_id_str']:
			is_reply = True
			reply_to = tweet['in_reply_to_status_id_str']
			if 'in_reply_to_screen_name' in tweet and tweet['in_reply_to_screen_name']:
				reply_to_username = helpers.hash('uname', tweet['in_reply_to_screen_name'])
	
		if 'retweeted_status' in tweet and tweet['retweeted_status']:
			is_retweet = True
			retweet = tweet['retweeted_status']
			retweeted_id = retweet['id_str']
			retweeted_username = helpers.hash('uname', retweet['user']['screen_name'])
	
		if 'quoted_status' in tweet and tweet['quoted_status']:
			is_quote = True
			qtweet = tweet['quoted_status']
			quoted_id = qtweet['id_str']
			quoted_username = helpers.hash('uname', qtweet['user']['screen_name'])
		
		if is_retweet and is_quote:
			# all tweets that twitter classifies as both are retweets of quoted tweets
			# remove quoted to fix
			is_quote = False
			quoted_id = None
			quoted_username = None
		
		
		# tweet text
	
		text = ''
		if retweet is not None:
			# the Twitter website shows the extended text for retweets
			# but extended_tweet is not present in the root level data
			# thus we extract the text from the retweet object
			if 'extended_tweet' in retweet:
				if 'full_text' in retweet['extended_tweet']:
					text = retweet['extended_tweet']['full_text']
				else:
					logging.warning("error - extended retweet missing text\t{}".format(tweet))
			else:
				text = retweet['text']
		elif extended_tweet is not None:
			if 'full_text' in extended_tweet:
				text = extended_tweet['full_text']
			else:
				logging.warning("error - extended tweet missing text\t{}".format(tweet))
		else:
			text = tweet['text']
		
		
		# profile 
		
		user_desc = ""
		if 'description' in tweet['user']:
			user_desc = tweet['user']['description']
		
		user_verified = tweet['user']['verified']
		
		user_followers_count = tweet['user']['followers_count']
		user_friends_count = tweet['user']['friends_count']
		user_listed_count = tweet['user']['listed_count']
		user_favourites_count = tweet['user']['favourites_count']
		user_statuses_count = tweet['user']['statuses_count']
		
		
		# counts
	
		if 'quote_count' in tweet:
			quote_count = tweet['quote_count']
		if 'reply_count' in tweet:
			reply_count = tweet['reply_count']
		retweet_count = tweet['retweet_count']
		favorite_count = tweet['favorite_count']
	
	
		# dates
	
		raw_date = tweet['created_at']
		parsed_date = datetime.strptime(raw_date, '%a %b %d %H:%M:%S %z %Y')
		parsed_date = parsed_date.astimezone(pytz.utc)
	
		timestamp = int(parsed_date.timestamp() * 1000.0)
		
		if 'created_at' in tweet['user']:
			user_raw_date = tweet['user']['created_at']
			user_parsed_date = datetime.strptime(raw_date, '%a %b %d %H:%M:%S %z %Y')
			user_parsed_date = parsed_date.astimezone(pytz.utc)
			user_created_timestamp = int(parsed_date.timestamp() * 1000.0)
			
	
		# entities
		
		entities = None
		if retweet is not None:
			# the Twitter website shows the extended text for retweets
			# but extended_tweet is not present in the root level data
			# thus we extract the entities from the retweet object
			if 'extended_tweet' in retweet:
				if 'entities' in retweet['extended_tweet']:
					entities = retweet['extended_tweet']['entities']
				else:
					logging.warning("error - extended retweet missing entities\t{}".format(tweet))
			else:
				entities = retweet['entities']
		elif extended_tweet is not None:
			if 'entities' in extended_tweet:
				entities = extended_tweet['entities']
			else:
				logging.warning("error - extended tweet missing entities\t{}".format(tweet))
		else:
			entities = tweet['entities']
		
	
		for ht in entities['hashtags']:
			tag = ht['text'].lower()
			hashtags[ tag ] += 1
	
		for um in entities['user_mentions']:
			uname = um['screen_name'].lower()
			mentions[ helpers.hash('uname', uname) ] += 1
	
		for url in entities['urls']:
			link = url['expanded_url']
			simple_links[ link ] += 1
			
			website = helpers.extract_website(link)
			if website:
				simple_websites[ website ] += 1
			
			if 'unwound' in url:
				if 'url' in url['unwound'] and url['unwound']['url']:
					link = url['unwound']['url']
					unwound_links[ link ] += 1
				
					website = helpers.extract_website(link)
					if website:
						unwound_websites[ website ] += 1
				
				if 'title' in url['unwound'] and url['unwound']['title']:
					anon_title = helpers.anonymize_text(url['unwound']['title'])
					link_titles[ anon_title ] += 1
					
					title_tokens = helpers.tokenise_text(anon_title)
					for token in title_tokens:
						link_title_types[token] += 1
				
				if 'description' in url['unwound'] and url['unwound']['description']:
					anon_desc = helpers.anonymize_text(url['unwound']['description'])
					
					desc_tokens = helpers.tokenise_text(anon_desc)
					for token in desc_tokens:
						link_description_types[token] += 1
				
			# unwound url if available, otherwise expanded url
			if helpers.include_link(link, tweet_id):
				link = helpers.anon_twitter_link(link)
				links[ link ] += 1
				
				website = helpers.extract_website(link)
				if website:
					websites[ website ] += 1
		
		if 'media' in entities:
			for item in entities['media']:
				if 'media_url_https' in item:
					media_file = item['media_url_https']
					media_files[media_file] += 1
				elif 'media_url' in item:
					media_file = item['media_url']
					media_files[media_file] += 1
				
				link = item['expanded_url']
				media_urls[ link ] += 1
				format = item['type']
				media_formats[ format ] += 1
			
				website = helpers.extract_website(link)
				if website:
					media_websites[ website ] += 1
		
		for sym in entities['symbols']:
			sym_str = sym['text']
			symbols[ sym_str ] += 1
	
	
		# geo
		
		if 'place' in tweet and tweet['place']:
			tweet_geo_desc = tweet['place']['full_name'] + " (" + tweet['place']['country'] + ")"
	
		if 'coordinates' in tweet and tweet['coordinates']:		
			# always point
			tweet_lng = tweet['coordinates']['coordinates'][0]
			tweet_lat = tweet['coordinates']['coordinates'][1]
			tweet_nuts_level = 3
				
		elif 'place' in tweet and tweet['place'] and tweet['place']['bounding_box']:
			place_type = tweet['place']['place_type']
		
			bbox = tweet['place']['bounding_box']['coordinates'][0]
			tweet_lng, tweet_lat = _geo_helper.average_coord(bbox)
		
			if place_type == 'country':
				tweet_nuts_level = 0
			if place_type == 'admin':
				tweet_nuts_level = 2
			if place_type == 'neighborhood':
				tweet_nuts_level = 2
			if place_type == 'city':
				tweet_nuts_level = 3
			if place_type == 'poi':
				tweet_nuts_level = 3
			
		if 'location' in tweet['user'] and tweet['user']['location']:
			user_geo_desc = tweet['user']['location']
		
		if 'derived' in tweet['user']:
			if 'locations' in tweet['user']['derived']:
				for location in tweet['user']['derived']['locations']:
					if 'locality' in location or 'sub_region' in location:		# 'region' in location 
						if location['geo']['type'] == 'point':
							user_lng = location['geo']['coordinates'][0]
							user_lat = location['geo']['coordinates'][1]
						else:
							bbox = location['geo']['coordinates']
							user_lng, user_lat = _geo_helper.average_coord(bbox)
						user_nuts_level = 2
						break
			
	
		# uk regions
		
		if _geo_search_level > 0:
	
			if tweet_lng and tweet_lat:
				if _geo_search_level >= 1 and tweet_nuts_level >= 1:
					tweet_nuts1_code, tweet_nuts1_name = _geo_helper.search_nuts_1(tweet_lng, tweet_lat)
				if _geo_search_level >= 2 and tweet_nuts_level >= 2:
					tweet_nuts2_code, tweet_nuts2_name = _geo_helper.search_nuts_2(tweet_lng, tweet_lat)
				if _geo_search_level >= 3 and tweet_nuts_level >= 3:
					tweet_nuts3_code, tweet_nuts3_name = _geo_helper.search_nuts_3(tweet_lng, tweet_lat)
		
				if tweet_nuts3_code is not None and tweet_nuts2_code is None:
					tweet_nuts2_code = tweet_nuts3_code[:4]
					tweet_nuts2_name = 'auto'
				if tweet_nuts2_code is not None and tweet_nuts1_code is None:
					tweet_nuts1_code = tweet_nuts2_code[:3]
					tweet_nuts1_name = 'auto'
			
				geo_source = "tweet"
				geo_lng = tweet_lng
				geo_lat = tweet_lat
				geo_nuts_level = tweet_nuts_level
				geo_nuts1_code = tweet_nuts1_code
				geo_nuts1_name = tweet_nuts1_name
				geo_nuts2_code = tweet_nuts2_code
				geo_nuts2_name = tweet_nuts2_name
				geo_nuts3_code = tweet_nuts3_code
				geo_nuts3_name = tweet_nuts3_name
		
	
			elif user_lng and user_lat:		
				if _geo_search_level >= 1 and user_nuts_level >= 1:
					user_nuts1_code, user_nuts1_name = _geo_helper.search_nuts_1(user_lng, user_lat)
				if _geo_search_level >= 2 and user_nuts_level >= 2:
					user_nuts2_code, user_nuts2_name = _geo_helper.search_nuts_2(user_lng, user_lat)
				if _geo_search_level >= 3 and user_nuts_level >= 3:
					user_nuts3_code, user_nuts3_name = _geo_helper.search_nuts_3(user_lng, user_lat)
		
				if user_nuts3_code is not None and user_nuts2_code is None:
					user_nuts2_code = user_nuts3_code[:4]
					user_nuts2_name = 'auto'
				if user_nuts2_code is not None and user_nuts1_code is None:
					user_nuts1_code = user_nuts2_code[:3]
					user_nuts1_name = 'auto'
			
				geo_source = "profile"
				geo_lng = user_lng
				geo_lat = user_lat
				geo_nuts_level = user_nuts_level
				geo_nuts1_code = user_nuts1_code
				geo_nuts1_name = user_nuts1_name
				geo_nuts2_code = user_nuts2_code
				geo_nuts2_name = user_nuts2_name
				geo_nuts3_code = user_nuts3_code
				geo_nuts3_name = user_nuts3_name
		
		
		# types
	
		anon_text = helpers.anonymize_text(text)
		tokens = helpers.tokenise_text(anon_text)
		computed_text = " ".join(tokens)
		bi_memory = deque([])
		tri_memory = deque([])

		for token in tokens:
			unfiltered_types[ token ] += 1
			if len(token) > 1 and not token in STOPWORDS and not token in hashtags:
				types[ token ] += 1
			
			bi_memory.append(token)
			if len(bi_memory) == 2:
				bi_gram = ' '.join(bi_memory)
				bi_grams[bi_gram] += 1
				bi_memory.popleft()
			
			tri_memory.append(token)
			if len(tri_memory) == 3:
				tri_gram = ' '.join(tri_memory)
				tri_grams[tri_gram] += 1
				tri_memory.popleft()
		
		
		# profile types
		
		if user_desc is not None and user_desc != "":
			anon_profile = helpers.anonymize_text(user_desc)
			profile_tokens = helpers.tokenise_text(anon_profile)

			for token in profile_tokens:
				unfiltered_profile_types[ token ] += 1
				if len(token) > 1 and not token in STOPWORDS:
					profile_types[ token ] += 1
		
		
		# user connections combined
		
		for uname in mentions:
			user_connections.append({ 'user': uname, 'conn': ['at'] })
		
		if retweeted_username:
			uc = next((uc for uc in user_connections if uc['user'] == retweeted_username), None)
			if uc is None:
				uc = { 'user': retweeted_username, 'conn': [] }
				user_connections.append(uc)
			uc['conn'].append('rt')
		
		if quoted_username:
			uc = next((uc for uc in user_connections if uc['user'] == quoted_username), None)
			if uc is None:
				uc = { 'user': quoted_username, 'conn': [] }
				user_connections.append(uc)
			uc['conn'].append('qt')
		
		if reply_to_username:
			uc = next((uc for uc in user_connections if uc['user'] == reply_to_username), None)
			if uc is None:
				uc = { 'user': reply_to_username, 'conn': [] }
				user_connections.append(uc)
			uc['conn'].append('re')
		
		
		# final doc
	
		doc = {
			'tweet_id':					tweet_id,
			'username':					username,
			
			'is_reply':					is_reply,
			'reply_id':					reply_to,
			'reply_to_username':		reply_to_username,
			
			'is_quote':					is_quote,
			'quoted_id':				quoted_id,
			'quoted_username':			quoted_username,
			
			'is_retweet':				is_retweet,
			'retweeted_id':				retweeted_id,
			'retweeted_username':		retweeted_username,
			
			'quote_count':				quote_count,
			'reply_count':				reply_count,
			'retweet_count':			retweet_count,
			'favorite_count':			favorite_count,
			
			'is_truncated':				is_truncated,
			'has_extended':				has_extended,
			'text': 					anon_text,
			'computed_text':			computed_text,
			'types':					helpers.counter_to_list(types),
			'unfiltered_types':			helpers.counter_to_list(unfiltered_types),
			'bi_grams':					helpers.counter_to_list(bi_grams),
			'tri_grams':				helpers.counter_to_list(tri_grams),
			'unfiltered_type_counts':	helpers.counter_to_object_list(unfiltered_types, key='type', val='freq'),
			
			'hashtags':					helpers.counter_to_list(hashtags),
			'user_mentions':			helpers.counter_to_list(mentions),
			'urls':						helpers.counter_to_list(links),
			'simple_urls':				helpers.counter_to_list(simple_links),
			'unwound_urls':				helpers.counter_to_list(unwound_links),
			'websites':					helpers.counter_to_list(websites),
			'simple_websites':			helpers.counter_to_list(simple_websites),
			'unwound_websites':			helpers.counter_to_list(unwound_websites),
			'url_titles':				helpers.counter_to_list(link_titles),
			'url_title_types':			helpers.counter_to_list(link_title_types),
			'url_description_types':	helpers.counter_to_list(link_description_types),
			'media_files':				helpers.counter_to_list(media_files),
			'media_urls':				helpers.counter_to_list(media_urls),
			'media_websites':			helpers.counter_to_list(media_websites),
			'media_formats':			helpers.counter_to_list(media_formats),
			'symbols':					helpers.counter_to_list(symbols),
			
			'profile_text':				anon_profile,
			'profile_types': 			helpers.counter_to_list(profile_types),
			'unfiltered_profile_types':	helpers.counter_to_list(unfiltered_profile_types),
			'profile_verified':			user_verified,
			'profile_followers_count':	user_followers_count,
			'profile_friends_count':	user_friends_count,
			'profile_listed_count':		user_listed_count,
			'profile_favourites_count':	user_favourites_count,
			'profile_statuses_count':	user_statuses_count,
			'profile_created_timestamp':	user_created_timestamp,
			
			'tweet_geo_coord':			_geo_helper.coords_object(tweet_lat, tweet_lng),
			'tweet_geo_description': 	tweet_geo_desc,
			'profile_geo_coord':		_geo_helper.coords_object(user_lat, user_lng),
			'profile_geo_descrption':	user_geo_desc,
			
			'timestamp':				timestamp,
			
			'source':					source,
			
			'user_connections':			user_connections
		}
	
		if _geo_search_level > 0:
			doc['geo_source'] = 			geo_source
			doc['geo_coord'] = 				_geo_helper.coords_object(geo_lat, geo_lng)
			doc['geo_nuts_level'] = 		geo_nuts_level
			doc['geo_nuts1_code'] = 		geo_nuts1_code
			doc['geo_nuts1_name'] = 		geo_nuts1_name
			doc['geo_nuts2_code'] = 		geo_nuts2_code
			doc['geo_nuts2_name'] = 		geo_nuts2_name
			doc['geo_nuts3_code'] = 		geo_nuts3_code
			doc['geo_nuts3_name'] = 		geo_nuts3_name
	
		docs.append(doc)
	
	except:
		logging.exception("tweet error\t{}".format(tweet))	
		raise
	
	# retweets and quote tweets
	if retweet:
		_process_tweet(retweet, docs)
	
	if qtweet:
		_process_tweet(qtweet, docs)



def _insert_docs(es, docs, file, insert_num):
	logging.info('start insert\t{}\t{}\t{}'.format( file, insert_num, len(docs) ))
	
	ops = 1
	done = 0
	body = ''
	for doc in docs:
		body += '{ "index" : { "_id" : "' + doc['tweet_id'] + '" } }\n' + json.dumps(doc) + '\n'
		if len(body) > MAX_BODY_SIZE:
			done += _insert_bulk(es, body, file, ops)
			ops += 1
			body = ''

	if len(body) > 0:
		done += _insert_bulk(es, body, file, 0)

	logging.info('insert result\t{}\t{}\t{}'.format( file, insert_num, done ))
		
	docs.clear()
	
	return done



def _insert_bulk(es, body, file, n):
	res = es.bulk(index=_index_name, body=body)
	if res['errors']:
		helpers.dump_es_error(res, file, n)
		logging.warning('bulk insert error\t{}\t{}\t{}'.format( file, n, len(res['items']) ))
		raise BulkInsertException('es.bulk returned errors')
	else:
		logging.info('bulk insert success\t{}\t{}\t{}'.format( file, n, len(res['items']) ))
		return len(res['items'])



INDEX_DEFINITION = {
	"settings": {
		"index": {
			"number_of_shards": 2
		}
	},
	"mappings": {
		"properties": {
			"tweet_id":                   { "type": "keyword" },
			"username":                   { "type": "keyword" },
			
			"is_reply":                   { "type": "boolean" },
			"reply_id":                   { "type": "keyword" },
			"reply_to_username":          { "type": "keyword" },
			
			"is_quote":                   { "type": "boolean" },
			"quoted_id":                  { "type": "keyword" },
			"quoted_username":            { "type": "keyword" },
			
			"is_retweet":                 { "type": "boolean" },
			"retweeted_id":               { "type": "keyword" },
			"retweeted_username":         { "type": "keyword" },
			
			"quote_count":                { "type": "long" },
			"reply_count":                { "type": "long" },
			"retweet_count":              { "type": "long" },
			"favorite_count":             { "type": "long" },
			
			"is_truncated":               { "type": "boolean" },
			"has_extended":               { "type": "boolean" },
			"text":                       { "type": "text" },
			"computed_text":              { "type": "keyword" },
			"types":                      { "type": "keyword" },
			"unfiltered_types":           { "type": "wildcard" },
			"bi_grams":                   { "type": "keyword" },
			"tri_grams":                  { "type": "keyword" },
			
			"unfiltered_type_counts":     { "type": "nested",
				"properties": {
					"type":                  { "type": "keyword" },
					"freq":                  { "type": "long" }
				}
			},
			
			"hashtags":                   { "type": "keyword" },
			"user_mentions":              { "type": "keyword" },
			"urls":                       { "type": "keyword" },
			"simple_urls":                { "type": "keyword" },
			"unwound_urls":               { "type": "keyword" },
			"websites":                   { "type": "keyword" },
			"simple_websites":            { "type": "keyword" },
			"unwound_websites":           { "type": "keyword" },
			"url_titles":                 { "type": "keyword" },
			"url_title_types":            { "type": "keyword" },
			"url_description_types":      { "type": "keyword" },
			"media_files":                { "type": "keyword" },
			"media_urls":                 { "type": "keyword" },
			"media_websites":             { "type": "keyword" },
			"media_formats":              { "type": "keyword" },
			"symbols":                    { "type": "keyword" },
			
			"profile_text":               { "type": "text" },
			"profile_types":              { "type": "keyword" },
			"unfiltered_profile_types":   { "type": "wildcard" },
			"profile_verified":           { "type": "boolean" },
			"profile_followers_count":    { "type": "long" },
			"profile_friends_count":      { "type": "long" },
			"profile_listed_count":       { "type": "long" },
			"profile_favourites_count":   { "type": "long" },
			"profile_statuses_count":     { "type": "long" },
			"profile_created_timestamp":  { "type": "date" },
			
			"tweet_geo_coord":            { "type": "geo_point" },
			"tweet_geo_description":      { "type": "keyword" },
			"profile_geo_coord":          { "type": "geo_point" },
			"profile_geo_descrption":     { "type": "keyword" },
			
			"geo_source":                 { "type": "keyword" },
			"geo_coord":                  { "type": "geo_point" },
			"geo_nuts_level":             { "type": "keyword" },
			"geo_nuts1_code":             { "type": "keyword" },
			"geo_nuts1_name":             { "type": "keyword" },
			"geo_nuts2_code":             { "type": "keyword" },
			"geo_nuts2_name":             { "type": "keyword" },
			"geo_nuts3_code":             { "type": "keyword" },
			"geo_nuts3_name":             { "type": "keyword" },
			
			"timestamp":                  { "type": "date" },
			
			"source":                     { "type": "keyword" },
			
			"user_connections":           { "type": "nested",
				"properties": {
					"user":                   { "type": "keyword" },
					"conn":                   { "type": "keyword" }
				}
			}
		}
	}
}


def _create_index():
	try:
		es = Elasticsearch(_es_ips, timeout=(60*60))
		res = es.indices.create(
			index = _index_name,
			body = INDEX_DEFINITION,
			ignore = 400
		)
		logging.info("result\t{}".format(res))
	except:
		logging.exception("index creation\t{}".format(_index_name))











