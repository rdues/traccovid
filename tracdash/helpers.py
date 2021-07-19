
import os, logging, re
from datetime import datetime
import hashlib
from pprint import pprint
from collections import Counter
import html

import geopandas as gpd

from . import unicodetokeniser
from . import geo



##########
# util
##########

def hash(prefix, str):
	return prefix + '_' + hashlib.sha256(str.lower().encode('utf-8')).hexdigest()


def escape_filename(name):
	return re.sub(r'\W', '-', name)


def counter_to_list(counter):
	list = []
	for k, v in counter.most_common():
		list.append(k)
	return list


def counter_to_object_list(counter, key = 'key', val = 'val'):
	list = []
	for k, v in counter.most_common():
		list.append({
			key: k,
			val: v
		})
	return list



##########
# text
##########

def init_tokeniser():
	unicodetokeniser.load()


URL_REGEX = re.compile(r'\S+://\S+')
EMAIL_REGEX = re.compile(r'\S+@\S+\.\S+')
ATTAG_REGEX = re.compile(r'@\S+')

STOPWORD_FILE = "stopwords.txt"


def anonymize_text(text):
	text = URL_REGEX.sub('', text)
	text = EMAIL_REGEX.sub('', text)
	text = ATTAG_REGEX.sub('', text)
	
	return text


def clean_text(text):
	text = html.unescape(text)
	text = unicodetokeniser.remove_control(text)
	text = unicodetokeniser.normalise(text)
	text = text.lower()
	
	text = unicodetokeniser.translit_punct(text)
	text = unicodetokeniser.reduce_whitespace(text)
	
	return text


def tokenise_text(text):
	text = clean_text(text)
	tokens = unicodetokeniser.tokenise(text)
	tokens = unicodetokeniser.split_apostrophes(tokens)
	tokens = unicodetokeniser.combine_hyphenated(tokens)
	tokens = unicodetokeniser.words_only(tokens)
	
	return tokens



##########
# links
##########

TWEET_URL_REGEX = re.compile(r'twitter\.com/.*/status/(\d+)')
TWITTER_USER_REGEX = re.compile(r'twitter\.com/[^/]+($|\?)')

def include_link(link, tweet_id):
	if not link:
		return False
	
	m = TWEET_URL_REGEX.search(link)		# link to current tweet
	if m:
		if m.group(1) == tweet_id:
			return False
	
	m = TWITTER_USER_REGEX.search(link)		# username
	if m:
		return False
	
	return True


TWITTER_REGEX = re.compile(r'^\w+://(?:www\.)?twitter\.com')
TWEET_URL_SUB_REGEX = re.compile(r'twitter\.com/.*/status/')
OTHER_TWITTER_URL_SUB_REGEX = re.compile(r'twitter\.com/.*/(lists|events|broadcasts|moments|timelines)/')
QUERY_SUB_REGEX = re.compile(r'\?.*$')

def anon_twitter_link(link):
	new_link = link
	
	m = TWITTER_REGEX.search(new_link)
	if m:
		new_link = TWEET_URL_SUB_REGEX.sub('twitter.com/i/web/status/', new_link)
		new_link = OTHER_TWITTER_URL_SUB_REGEX.sub(r'twitter.com/i/\1/', new_link)
		new_link = QUERY_SUB_REGEX.sub('', new_link)
	
	return new_link


WEBSITE_REGEX = re.compile(r'://(?:www\.)?([^/]+)')
TWITTER_WEBSITE_REGEX = re.compile(r'^\w+://(?:www\.)?(twitter\.com/i/(?:web/status|status|lists|events|broadcasts|moments|timelines))')

def extract_website(link):
	if not link:
		return None
	
	m = TWITTER_WEBSITE_REGEX.search(link)
	if m:
		return m.group(1)
	
	m = WEBSITE_REGEX.search(link)
	if m:
		return m.group(1)
	
	return None



##########
# logging
##########

logging_path = "."

def init_logging(log_path=".", log_name="tracdash", console=True, file=True, level=logging.INFO):
	global logging_path
	logging_path = log_path
	
	logFormatter = logging.Formatter("%(asctime)s\t[%(threadName)-12.12s]\t[%(levelname)-5.5s]\t%(message)s")
	rootLogger = logging.getLogger()

	if file:
		fileHandler = logging.FileHandler("{0}/{1}-{2}.log".format(log_path, log_name, datetime.now().strftime("%Y_%m_%d-%H_%M_%S")))
		fileHandler.setFormatter(logFormatter)
		rootLogger.addHandler(fileHandler)

	if console:
		consoleHandler = logging.StreamHandler()
		consoleHandler.setFormatter(logFormatter)
		rootLogger.addHandler(consoleHandler)
	
	rootLogger.setLevel(level)

	rootLogger.info("logging initialised")


def dump_es_error(res, file, n):
	with open( os.path.join(logging_path, "err_" + escape_filename(file) + "_" + str(n) + ".txt") , "w") as f:
		print('{}\t{}\t{}\terror\t{}'.format( datetime.now(), file, n, len(res['items']) ), file=f)
		pprint(res, f)



##########
# geo
##########

NUTS_1_SHP_FILE = "NUTS_Level_1__January_2018__Boundaries.shp"
NUTS_2_SHP_FILE = "NUTS_Level_2__January_2018__Boundaries.shp"
NUTS_3_SHP_FILE = "NUTS_Level_3__January_2018__Boundaries.shp"

def init_geo(geo_search_level, crs="EPSG:4326", shp_path=None,
		level1=NUTS_1_SHP_FILE,
		level2=NUTS_2_SHP_FILE,
		level3=NUTS_3_SHP_FILE):
	gdf_nuts_1 = None
	gdf_nuts_2 = None
	gdf_nuts_3 = None
	
	if not shp_path:
		shp_path = os.path.join(os.path.dirname(__file__), "NUTS")
	
	if geo_search_level >= 1:
		try:
			gdf_nuts_1 = gpd.read_file( os.path.join(shp_path, level1) )
			gdf_nuts_1.to_crs(crs=crs, inplace=True)
			gdf_nuts_1.rename({'nuts118cd': 'CODE', 'nuts118nm': 'NAME'}, axis=1, inplace=True)
			logging.info("Loaded shape file for NUTS Level 1")
		except:
			logging.exception("Failed to load shape file for NUTS Level 1")
	
	if geo_search_level >= 2:
		try:
			gdf_nuts_2 = gpd.read_file( os.path.join(shp_path, level2) )
			gdf_nuts_2.to_crs(crs=crs, inplace=True)
			gdf_nuts_2.rename({'nuts218cd': 'CODE', 'nuts218nm': 'NAME'}, axis=1, inplace=True)
			logging.info("Loaded shape file for NUTS Level 2")
		except:
			logging.exception("Failed to load shape file for NUTS Level 2")
	
	if geo_search_level >= 3:
		try:
			gdf_nuts_3 = gpd.read_file( os.path.join(shp_path, level3) )
			gdf_nuts_3.to_crs(crs=crs, inplace=True)
			gdf_nuts_3.rename({'nuts318cd': 'CODE', 'nuts318nm': 'NAME'}, axis=1, inplace=True)
			logging.info("Loaded shape file for NUTS Level 3")
		except:
			logging.exception("Failed to load shape file for NUTS Level 3")
	
	geohelper = geo.GeoHelper(crs, gdf_nuts_1, gdf_nuts_2, gdf_nuts_3)
	return geohelper
	


	


