"""
Connects to the ElasticSearch database.
Generates aggregations to be returned to the dashboard.
"""

from scipy.stats.distributions import chi2

import os, math, re, itertools, json
from copy import deepcopy
from elasticsearch import Elasticsearch
from pandas import json_normalize
from pprint import pprint
import pandas as pd
import numpy as np
import diskcache
from hashlib import md5

from ..exceptions import SearchException
from ..stopwords import SEED_STOPWORDS, STOPWORDS_EN
from ..unicodetokeniser.util import contains_digit
from .util import info, exception


# update accordingly
START_DATE = "2020-01-01"
END_DATE = "2021-04-30"
MIN_TIMESTAMP = "2020-01-01T00:00:00"

AGG_SIZE = 1000
RETURN_SIZE = 100
MIN_THRESHOLD_PROPORTION = 1.0/1000000		# 1 in 1 million
MIN_THRESHOLD_HARD = 10

# reference corpus collected mid-2018 via the Twitter Sample Stream (English only)
REFERENCE_TYPE_LIST = os.path.join(os.path.dirname(__file__), '..', 'data', 'twitter_reference_types.txt')
REFERENCE_CORPUS_SIZE = 49307264
REFERENCE_MIN_PROPORTION = 1.0/1000000
KEYNESS_SM_SMOOTHING = 100.0/1000000.0
KEYNESS_SM_THRESHOLD = 1.1
CANDIDATE_KEYWORDS_SIZE = 100000
CANDIDATE_NGRAMS_SIZE = 100000


RT_FILTER = {
  "script": {
    "script": {
      "source": "(!doc['is_retweet'].value && !doc['is_reply'].value && !doc['is_quote'].value) || (doc['is_retweet'].value && params['rt']) || (doc['is_quote'].value && params['qt']) || (doc['is_reply'].value && params['re'])",
      "lang": "painless",
      "params": {
        "rt": False,
        "qt": False,
        "re": False
      }
    }
  }
}


AGG_LIST = {
  "size": 0,
  "query": {
    "bool": {
      "must": [],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
    "total": {
      "value_count": { "field": "tweet_id" }
    },
    "counts": {
      "terms": {
        "field": "FIELD",
        "size": AGG_SIZE,
        "min_doc_count": MIN_THRESHOLD_HARD
      }
    }
  }
}


DAY_TOTALS = {
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
    "counts": {
      "date_histogram": {
        "field": "timestamp",
        "calendar_interval": "day"
      }
    }
  }
}


DAY_TOTALS_SHARING = {
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } }
      ]
    }
  },      
  "aggs": {
    "counts": {
      "date_histogram": {
        "field": "timestamp",
        "calendar_interval": "day"
      }
    },
    "rt_counts": {
      "filter": { "term": { "is_retweet": True } },
      "aggs": {
        "dates": {
          "date_histogram": {
            "field": "timestamp",
            "calendar_interval": "day"
          }
        }
      }
    },
    "qt_counts": {
      "filter": { "term": { "is_quote": True } },
      "aggs": {
        "dates": {
          "date_histogram": {
            "field": "timestamp",
            "calendar_interval": "day"
          }
        }
      }
    },
    "re_counts": {
      "filter": { "term": { "is_reply": True } },
      "aggs": {
        "dates": {
          "date_histogram": {
            "field": "timestamp",
            "calendar_interval": "day"
          }
        }
      }
    }
  }
}


SEARCH_DAYS = {
  "size": 0,
  "query": {
    "bool": {
      "must": [],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
    "counts": {
      "date_histogram": {
        "field": "timestamp",
        "calendar_interval": "day"
      }
    }
  }
}


TERM_RT = {
  "size": 0,
  "query": {
    "bool": {
      "must": [],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
    "rt_counts": {
      "terms": {
        "field": "is_retweet",
        "size": 2
      }
    },
    "qt_counts": {
      "terms": {
        "field": "is_quote",
        "size": 2
      }
    },
    "re_counts": {
      "terms": {
        "field": "is_reply",
        "size": 2
      }
    }
  }
}


TERM_TYPES = {
  "size": 0,
  "query": {
    "bool": {
      "must": [],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
  	"total": {
      "value_count": { "field": "tweet_id" }
    },
    "counts": {
      "terms": {
        "field": "types",
        "size": AGG_SIZE,
        "min_doc_count": MIN_THRESHOLD_HARD
      }
    }
  }
}


TERM_BIGRAMS = {
  "size": 0,
  "query": {
    "bool": {
      "must": [],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
    "total": {
      "value_count": { "field": "tweet_id" }
    },
    "counts": {
      "terms": {
        "field": "bi_grams",
        "size": CANDIDATE_NGRAMS_SIZE,
        "min_doc_count": MIN_THRESHOLD_HARD,
      }
    }
  }
}


TERM_TRIGRAMS = {
  "size": 0,
  "query": {
    "bool": {
      "must": [],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
    "total": {
      "value_count": { "field": "tweet_id" }
    },
    "counts": {
      "terms": {
        "field": "tri_grams",
        "size": CANDIDATE_NGRAMS_SIZE,
        "min_doc_count": MIN_THRESHOLD_HARD
      }
    }
  }
}


TERM_HASHTAGS = {
  "size": 0,
  "query": {
    "bool": {
      "must": [],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
  	"total": {
      "value_count": { "field": "tweet_id" }
    },
    "counts": {
      "terms": {
        "field": "hashtags",
        "size": AGG_SIZE,
        "min_doc_count": MIN_THRESHOLD_HARD
      }
    }
  }
}


TERM_WEBSITES = {
  "size": 0,
  "query": {
    "bool": {
      "must": [],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
  	"total": {
      "value_count": { "field": "tweet_id" }
    },
    "counts": {
      "terms": {
        "field": "websites",
        "size": AGG_SIZE,
        "min_doc_count": MIN_THRESHOLD_HARD
      }
    }
  }
}


TERM_URLS = {
  "size": 0,
  "query": {
    "bool": {
      "must": [],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
  	"total": {
      "value_count": { "field": "tweet_id" }
    },
    "counts": {
      "terms": {
        "field": "urls",
        "size": AGG_SIZE,
        "min_doc_count": MIN_THRESHOLD_HARD
      }
    }
  }
}


TERM_USERS = {
  "size": 0,
  "query": {
    "bool": {
      "must": [],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
  	"total": {
      "value_count": { "field": "tweet_id" }
    },
    "users": {
      "cardinality": { "field": "username" }
    },
    "counts": {
      "terms": {
        "field": "username",
        "size": 1000
        # no min threshold as we need to show many proportions to avoid the results being misleading
      }
    }
  }
}


CORPUS_SIZE = {
  "query": {
    "bool": {
      "must": [
      ],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  }
}


CORPUS_SIZE_RT = {
  "size": 0,
  "query": {
    "bool": {
      "filter": { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } }
    }
  },
  "aggs": {
    "counts": {
      "composite": {
        "sources": [
          { "is_retweet": { "terms": { "field": "is_retweet" } } },
          { "is_reply": { "terms": { "field":   "is_reply" } } },
          { "is_quote": { "terms": { "field":   "is_quote" } } }
        ]
      }
    }
  }
}


CORPUS_USERS_COUNT = {
  "query": {
    "bool": {
      "must": [
      ],
      "filter": [
        { "range": { "timestamp": { "gte": MIN_TIMESTAMP } } },
        RT_FILTER
      ]
    }
  },
  "aggs": {
    "users": {
      "cardinality": {
      	"field": "username",
      	"precision_threshold": 40000	# maximum
      }
    }
  }
}



class ESHelper:
	def __init__(self, es_instance, index_name, start_date=START_DATE, end_date=END_DATE, cache_path=None):
		self.es = es_instance
		self.index_name = index_name
		self.start_date = start_date
		self.end_date = end_date
		self.data_version = self.index_name[-2:]
		
		self.cache = None
		if cache_path is not None:
			self.init_cache(cache_path)
		
		self.init_stats()
		self.init_reference_types()
	
	
	def init_cache(self, cache_path):
		self.cache_path = os.path.join(cache_path, self.index_name)
		self.cache = diskcache.Cache(self.cache_path)
		self.cache.reset('cull_limit', 0)
		info("Using Elastic Search cache at {}".format(cache_path))
	
	
	def init_stats(self):
		self.max_total_documents = 0
		self.min_total_documents = 0
		self.min_threshold = MIN_THRESHOLD_HARD
		self.total_users = 0

		try:
			agg = deepcopy(CORPUS_SIZE_RT)
			res = self.search(agg)
			df = json_normalize(res['aggregations']['counts']['buckets'])
			df = df.rename(columns={
				'key.is_retweet': 'rt',
				'key.is_reply': 're',
				'key.is_quote': 'qt'
			})
			
			self.max_total_documents = df['doc_count'].sum()
			self.min_total_documents = df.query('rt == False and re == False and qt == False')['doc_count'].sum()
			self.min_threshold = max(MIN_THRESHOLD_HARD, int(self.min_total_documents * MIN_THRESHOLD_PROPORTION))
			
			info("Total documents: {} - {} Min threshold: {}".format(self.min_total_documents, self.max_total_documents, self.min_threshold))
			
			self.total_users = self.corpus_users_count()
			
			info("Total users estimate: {}".format(self.total_users))
			
		except Exception as e:
			message = "rt totals & users aggregations failed"
			exception(message, e)
			raise SearchException(message)
	
	
	def init_reference_types(self):
		self.reference_types = {}
		with open(REFERENCE_TYPE_LIST, 'r', encoding='utf-8') as f:
			for line in f:
				line = line.strip()
				if line:
					doc_prop, type = line.split('\t')
					self.reference_types[type] = float(doc_prop)
		info("Reference types loaded: {} types".format( len(self.reference_types) ))

	
	def date_list(self, str_format="%Y-%m-%dT00:00:00.000Z"):
		dates = []
		for d in pd.date_range(start=self.start_date, end=self.end_date):
			dates.append(d.strftime(str_format))
		return dates


	def keyness(self, type, doc_freq, corp_size, n_tests):
		a_rel = doc_freq / corp_size
		b_rel = self.reference_types.get(type, 0.0)
		
		if a_rel > b_rel:
			# simple maths
			sm = (a_rel + KEYNESS_SM_SMOOTHING) / (b_rel + KEYNESS_SM_SMOOTHING)
			
			if sm >= KEYNESS_SM_THRESHOLD:
				return sm
		
		return 0.0
	
	
	def search(self, query):
		self.set_min_threshold(query)
		
		res = None
		if self.cache is not None:
			res = self.cache.get(query, None)
		if res is None:
			res = self.es.search(index=self.index_name, body=query)
			if self.cache is not None:
				self.cache[query] = res
		return res
	
	
	def count(self, query):
		res = None
		if self.cache is not None:
			res = self.cache.get(query, None)
		if res is None:
			res = self.es.count(index=self.index_name, body=query)
			if self.cache is not None:
				self.cache[query] = res
		return res
	
	
	def add_rt_filter(self, agg, include_rt=False, include_qt=False, include_re=False):
		for filter in agg['query']['bool']['filter']:
			if 'script' in filter:
				filter['script']['script']['params']['rt'] = include_rt
				filter['script']['script']['params']['qt'] = include_qt
				filter['script']['script']['params']['re'] = include_re
	
	
	def add_wildcard_filter(self, agg, field, search):
		if search is not None and search != '':
			include = re.sub(r'\W+', '.+', search)
			query = re.sub(r'\W+', '*', search)
			agg['query']['bool']['must'].append({ "wildcard": { field: "*" + query + "*" } })
			agg['aggs']['counts']['terms']['include'] = ".*" + include + ".*"
	
	
	def add_term_filter(self, agg, term):
		if term is not None and term != '':
			search_term, field = self.search_term_and_field(term)
			agg['query']['bool']['must'].append( { "term": { field: search_term } } )
	
	
	def add_date_range(self, agg, date_range):
		if date_range and len(date_range) == 2:
			if date_range[0] is None:
				date_range[0] = self.start_date
			if date_range[1] is None:
				date_range[1] = self.end_date
			
			for filter in agg['query']['bool']['filter']:
				if 'range' in filter:
					filter['range']['timestamp'] = {
						'gte': date_range[0] + 'T00:00:00',
						'lte': date_range[1] + 'T23:59:59'
					}
	
	
	def set_min_threshold(self, agg):
		if 'aggs' in agg:
			if 'counts' in agg['aggs']:
				if 'terms' in agg['aggs']['counts']:
					if 'min_doc_count' in agg['aggs']['counts']['terms']:
						agg['aggs']['counts']['terms']['min_doc_count'] = self.min_threshold
	
	
	def search_term_and_field(self, term):
		field = "types"
		search_term = term
		if term.endswith("/"):
			field = "websites"
			search_term = term[:-1]
		elif term.startswith("#"):
			field = "hashtags"
			search_term = term[1:]
		return search_term, field
	
	
	def filter_keys(self, df, stopwords=set(), seeds=set(), terms=set()):
		term_stopwords = set([self.search_term_and_field(t)[0] for t in terms])
		filter = stopwords | seeds | term_stopwords
		df['key'] = df['key'].apply(lambda x: None if x in filter or contains_digit(x) else x)
		df = df.dropna(subset=['key'])
		return df
	
	
	def filter_urls(self, df):
		df['key'] = df['key'].apply(lambda x: None if 'twitter.com/i/web/status' in x else x)
		df = df.dropna(subset=['key'])
		return df
	
	
	def get_aggregation(self, agg, field):
		agg['aggs']['counts']['terms']['field'] = field
	
		try:
			res = self.search(agg)
			df = json_normalize(res['aggregations']['counts']['buckets'])
			total = res['aggregations']['total']['value']
			return df, total
		except Exception as e:
			message = "aggregation failed on {}".format(field)
			exception(message, e)
			raise SearchException(message)
	
	
	def corpus_size(self, include_rt=False, include_qt=False, include_re=False, date_range=None):
		agg = deepcopy(CORPUS_SIZE)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		if date_range:
			self.add_date_range(agg, date_range)
	
		try:
			res = self.count(agg)
			
			return int(res['count'])
		except Exception as e:
			message = "corpus size count failed"
			exception(message, e)
			raise SearchException(message)
	
	
	def corpus_users_count(self, include_rt=False, include_qt=False, include_re=False, date_range=None):
		agg = deepcopy(CORPUS_USERS_COUNT)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		if date_range:
			self.add_date_range(agg, date_range)
	
		try:
			res = self.search(agg)
			
			return int(res['aggregations']['users']['value'])
		except Exception as e:
			message = "corpus users count failed"
			exception(message, e)
			raise SearchException(message)
	
	
	def time_series_totals(self, include_rt=False, include_qt=False, include_re=False, date_range=None):
		df = pd.DataFrame(data = self.date_list(), columns = ['_key_as_string'])
		
		agg = deepcopy(DAY_TOTALS)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		self.add_date_range(agg, date_range)
	
		try:
			res = self.search(agg)			
			new_df = json_normalize(res['aggregations']['counts']['buckets'])
			new_df = new_df.drop(columns=['key'])
			new_df = new_df.rename(columns={"key_as_string": "_key_as_string", "doc_count": "_total"})
			
			df = df.merge(new_df, 'outer', '_key_as_string')
			
		except Exception as e:
			message = "types and date aggregation failed on {}".format(terms)
			exception(message, e)
			raise SearchException(message)

		df = df.fillna(0)
		
		return df
	
	
	def time_series_total_sharing(self):
		df = pd.DataFrame(data = self.date_list(), columns = ['_key_as_string'])
		
		agg = deepcopy(DAY_TOTALS_SHARING)
	
		try:
			res = self.search(agg)			
			new_df = json_normalize(res['aggregations']['counts']['buckets'])
			
			new_df = new_df.drop(columns=['key'])
			new_df = new_df.rename(columns={"key_as_string": "_key_as_string", "doc_count": "total"})
			
			df = df.merge(new_df, 'outer', '_key_as_string')
			
			for agg_name in ['rt_counts', 'qt_counts', 're_counts']:
				new_df = json_normalize(res['aggregations'][agg_name]['dates']['buckets'])
				new_df = new_df.drop(columns=['key'])
				new_df = new_df.rename(columns={"key_as_string": "_key_as_string", "doc_count": agg_name})
				
				df = df.merge(new_df, 'outer', '_key_as_string')
			
		except Exception as e:
			message = "date and sharing aggregation failed"
			exception(message, e)
			raise SearchException(message)

		df = df.fillna(0)
		
		return df
	
	
	def types_aggregation(self, include_rt=False, include_qt=False, include_re=False, search=None):
		agg = deepcopy(AGG_LIST)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		
		if search:
			self.add_wildcard_filter(agg, "types", search)
				
		df, total = self.get_aggregation(agg, "types")
				
		if len(df.index) == 0:
			return None, 0
		
		df = self.filter_keys(df, stopwords=STOPWORDS_EN, seeds=SEED_STOPWORDS)
		
		if len(df.index) == 0:
			return None, 0
		
		return df.head(RETURN_SIZE), total
	
	
	def keywords_aggregation(self, include_rt=False, include_qt=False, include_re=False, search=None):
		agg = deepcopy(AGG_LIST)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		agg["aggs"]["counts"]["terms"]["size"] = CANDIDATE_KEYWORDS_SIZE
		
		if search:
			self.add_wildcard_filter(agg, "types", search)
		
		df, total = self.get_aggregation(agg, "types")
		
		if len(df.index) == 0:
			return None, 0
		
		df = self.filter_keys(df, stopwords=STOPWORDS_EN, seeds=SEED_STOPWORDS)
		
		if len(df.index) == 0:
			return None, 0
		
		n_tests = len(df.index)
		corp_size = self.corpus_size(include_rt=include_rt, include_qt=include_qt, include_re=include_re)
		df['keyness'] = df.apply(lambda x: self.keyness(x['key'], x['doc_count'], corp_size, n_tests), axis=1)
		df = df.query('keyness > 0.0')
		df = df.sort_values(by='keyness', ascending=False)
		
		return df.head(RETURN_SIZE), total
	
	
	def hashtags_aggregation(self, include_rt=False, include_qt=False, include_re=False, search=None):
		agg = deepcopy(AGG_LIST)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		
		if search:
			self.add_wildcard_filter(agg, "hashtags", search)
		
		df, total = self.get_aggregation(agg, "hashtags")
		
		if len(df.index) == 0:
			return None, 0
		
		df = self.filter_keys(df, seeds=SEED_STOPWORDS)
		
		if len(df.index) == 0:
			return None, 0
		
		df['key'] = df['key'].apply(lambda x: "#" + x)
		
		return df.head(RETURN_SIZE), total
	
	
	def websites_aggregation(self, include_rt=False, include_qt=False, include_re=False, search=None):
		agg = deepcopy(AGG_LIST)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		
		if search:
			self.add_wildcard_filter(agg, "websites", search)
		
		df, total = self.get_aggregation(agg, "websites")
		
		if len(df.index) == 0:
			return None, 0
			
		df = self.filter_urls(df)
		
		if len(df.index) == 0:
			return None, 0
		
		df['key'] = df['key'].apply(lambda x: x + "/")
		
		return df.head(RETURN_SIZE), total
		
	
	def time_series_search(self, terms, include_rt=False, include_qt=False, include_re=False, normalise=False, date_range=None):
		df = pd.DataFrame(data = self.date_list(), columns = ['_key_as_string'])
		
		for term in terms:
			agg = deepcopy(SEARCH_DAYS)
			self.add_rt_filter(agg, include_rt, include_qt, include_re)
			self.add_term_filter(agg, term)
			self.add_date_range(agg, date_range)
		
			try:
				res = self.search(agg)			
				new_df = json_normalize(res['aggregations']['counts']['buckets'])
				
				if len(new_df.index) == 0:
					break
				
				new_df = new_df.drop(columns=['key'])
				new_df = new_df.rename(columns={"key_as_string": "_key_as_string", "doc_count": term})
				
				df = df.merge(new_df, 'outer', '_key_as_string')
				
			except Exception as e:
				message = "types and date aggregation failed on {}".format(terms)
				exception(message, e)
				raise SearchException(message)
		
		if len(df.index) == 0:
			return None
		
		df = df.fillna(0)
		
		# normalise
		if normalise:
			try:
				day_totals_df = self.time_series_totals(include_rt=include_rt, include_qt=include_qt, include_re=include_re)
		
				df = df.merge(day_totals_df, 'outer', '_key_as_string')
				df[terms] = df[terms].div(df['_total'], axis=0)
				df = df.drop(columns=['_total'])
			except Exception as e:
				message = "types and date aggregation failed on {}".format(terms)
				exception(message, )
				raise SearchException(message)
		
		return df
	
	
	def cooccuring_types_search(self, terms, include_rt=False, include_qt=False, include_re=False, date_range=None):
		df = None
		rank_df = None
		
		try:		
			for term in terms:
				agg = deepcopy(TERM_TYPES)
				self.add_rt_filter(agg, include_rt, include_qt, include_re)
				self.add_term_filter(agg, term)
				self.add_date_range(agg, date_range)
						
				res = self.search(agg)
				new_df = json_normalize(res['aggregations']['counts']['buckets'])
				
				if len(new_df.index) == 0:
					continue
			
				total = res['aggregations']['total']['value']
				
				new_df = self.filter_keys(new_df, stopwords=STOPWORDS_EN, seeds=SEED_STOPWORDS, terms=terms)
				
				if len(new_df.index) == 0:
					continue
			
				new_df = new_df.rename(columns={"key": "_key"})
				new_df[term] = new_df['doc_count'].div(total)
				new_df = new_df.drop(columns=['doc_count'])
			
				if df is None:
					df = new_df
				else:
					df = df.merge(new_df, 'outer', '_key')
			
			if df is None:
				return None, None
			
			df = df.fillna(0)
			
			# ranks
			rank_df = df.copy()
			for term in terms:
				if term in rank_df.columns:
					rank_df[term] = rank_df[term].rank(ascending=False).apply(lambda x: 51 - x if x <= 50 else 0)
			
			rank_df['_max'] = rank_df.max(axis=1, numeric_only=True)
			rank_df = rank_df.query('_max > 0')
			rank_df = rank_df.sort_values(by='_max', ascending=False)
			
			# filter and sort
			df = df[ df['_key'].isin(rank_df['_key']) ]
			df['_max'] = df.max(axis=1, numeric_only=True)
			df = df.sort_values(by='_max', ascending=False)
			
		except Exception as e:
			message = "cooccuring types aggregation failed on {}".format(terms)
			exception(message, e)
			raise SearchException(message)

		return df, rank_df
	
	
	def cooccuring_hashtags_search(self, terms, include_rt=False, include_qt=False, include_re=False, date_range=None):
		df = None
		rank_df = None
		
		try:
		
			for term in terms:
				agg = deepcopy(TERM_HASHTAGS)
				self.add_rt_filter(agg, include_rt, include_qt, include_re)
				self.add_term_filter(agg, term)
				self.add_date_range(agg, date_range)
				
				res = self.search(agg)			
				new_df = json_normalize(res['aggregations']['counts']['buckets'])
				
				if len(new_df.index) == 0:
					continue
				
				total = res['aggregations']['total']['value']
				
				new_df = self.filter_keys(new_df, stopwords=STOPWORDS_EN, seeds=SEED_STOPWORDS, terms=terms)
				
				if len(new_df.index) == 0:
					continue
				
				new_df['key'] = new_df['key'].apply(lambda x: "#" + x)
				
				new_df = new_df.rename(columns={"key": "_key"})				
				new_df[term] = new_df['doc_count'].div(total)
				new_df = new_df.drop(columns=['doc_count'])
				
				if df is None:
					df = new_df
				else:
					df = df.merge(new_df, 'outer', '_key')
			
			if df is None:
				return None, None
			
			df = df.fillna(0)
			
			# ranks
			rank_df = df.copy()
			for term in terms:
				if term in rank_df.columns:
					rank_df[term] = rank_df[term].rank(ascending=False).apply(lambda x: 51 - x if x <= 50 else 0)
			
			rank_df['_max'] = rank_df.max(axis=1, numeric_only=True)
			rank_df = rank_df.query('_max > 0')
			rank_df = rank_df.sort_values(by='_max', ascending=False)
			
			# filter and sort
			df = df[ df['_key'].isin(rank_df['_key']) ]
			df['_max'] = df.max(axis=1, numeric_only=True)
			df = df.sort_values(by='_max', ascending=False)
		
		except Exception as e:
			message = "cooccuring hashtags aggregation failed on {}".format(terms)
			exception(message, e)
			raise SearchException(message)
		
		return df, rank_df
	
	
	
	def term_rt_aggregation(self, term, include_rt=False, include_qt=False, include_re=False, date_range=None):
		agg = deepcopy(TERM_RT)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		self.add_term_filter(agg, term)
		self.add_date_range(agg, date_range)
		
		try:
			res = self.search(agg)
			
			df = json_normalize(res['aggregations']['rt_counts']['buckets']).rename(columns={"doc_count": "Retweets"}).drop(columns=['key'])
			qt_df = json_normalize(res['aggregations']['qt_counts']['buckets']).rename(columns={"doc_count": "Quote Tweets"}).drop(columns=['key'])
			re_df = json_normalize(res['aggregations']['re_counts']['buckets']).rename(columns={"doc_count": "Replies"}).drop(columns=['key'])
			
			df = df.merge(qt_df, 'outer', 'key_as_string')
			df = df.merge(re_df, 'outer', 'key_as_string')
			df = df.fillna(0)
			
			cols = []
			
			if not include_rt:
				df = df.drop(columns=['Retweets'])
			else:
				cols.append('Retweets')
			
			if not include_qt:
				df = df.drop(columns=['Quote Tweets'])
			else:
				cols.append('Quote Tweets')
			
			if not include_re:
				df = df.drop(columns=['Replies'])
			else:
				cols.append('Replies')
			
			if len(cols) == 0:
				return None
			
			total = df[cols[0]].sum()
			df[cols] = df[cols].div(total)
			
			df = pd.melt(df, id_vars=['key_as_string'], value_vars=cols, var_name='type', value_name='doc_count')
			
			df = df.sort_values(by='key_as_string', ascending=False)
			
			return df
		except Exception as e:
			message = "term rt aggregation failed on {}".format(term)
			exception(message, e)
			raise SearchException(message)
	
	
	def term_types_aggregation(self, term, include_rt=False, include_qt=False, include_re=False, date_range=None):
		agg = deepcopy(TERM_TYPES)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		self.add_term_filter(agg, term)
		self.add_date_range(agg, date_range)
		
		try:
			res = self.search(agg)
						
			df = json_normalize(res['aggregations']['counts']['buckets'])
			
			if len(df.index) == 0:
				return None, 0
			
			total = res['aggregations']['total']['value']
			
			df = self.filter_keys(df, stopwords=STOPWORDS_EN, seeds=SEED_STOPWORDS, terms=[term])
			
			if len(df.index) == 0:
				return None, 0
			
			df['proportion'] = df['doc_count'].div(total)
			
			return df.head(RETURN_SIZE), total
		except Exception as e:
			message = "term types aggregation failed on {}".format(term)
			exception(message, e)
			raise SearchException(message)
	
	
	def term_ngrams_aggregation(self, term, n, include=None, include_rt=False, include_qt=False, include_re=False, date_range=None):
		agg = None
		if n <= 2:
			agg = deepcopy(TERM_BIGRAMS)
		elif n >= 3:
			agg = deepcopy(TERM_TRIGRAMS)
		
		include_regex = re.compile(r"")
		if include:
				include_regex = re.compile(r"(?:^| )" + include + "(?: |$)")
		
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		self.add_term_filter(agg, term)
		self.add_date_range(agg, date_range)
		
		try:
			res = self.search(agg)
			
			df = json_normalize(res['aggregations']['counts']['buckets'])
			
			if len(df.index) == 0:
				return None, 0
			
			total = res['aggregations']['total']['value']
			
			df['key'] = df['key'].apply(lambda x: None if not include_regex.search(x) else x)
			df = df.dropna(subset=['key'])
			
			if len(df.index) == 0:
				return None, 0
			
			df['proportion'] = df['doc_count'].div(total)
			
			return df.head(RETURN_SIZE), total
		except Exception as e:
			message = "term types aggregation failed on {}".format(term)
			exception(message, e)
			raise SearchException(message)
	
	
	def term_hashtags_aggregation(self, term, include_rt=False, include_qt=False, include_re=False, date_range=None):
		agg = deepcopy(TERM_HASHTAGS)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		self.add_term_filter(agg, term)
		self.add_date_range(agg, date_range)
				
		try:
			res = self.search(agg)
			df = json_normalize(res['aggregations']['counts']['buckets'])
			
			if len(df.index) == 0:
				return None, 0
			
			total = res['aggregations']['total']['value']
			
			df = self.filter_keys(df, seeds=SEED_STOPWORDS, terms=[term])
			
			if len(df.index) == 0:
				return None, 0
			
			df['key'] = df['key'].apply(lambda x: "#" + x)
			df['proportion'] = df['doc_count'].div(total)
			
			return df.head(RETURN_SIZE), total
		except Exception as e:
			message = "term hashtags aggregation failed on {}".format(term)
			exception(message, e)
			raise SearchException(message)
	
	
	def term_websites_aggregation(self, term, include_rt=False, include_qt=False, include_re=False, date_range=None):
		agg = deepcopy(TERM_WEBSITES)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		self.add_term_filter(agg, term)
		self.add_date_range(agg, date_range)
		
		try:
			res = self.search(agg)
			df = json_normalize(res['aggregations']['counts']['buckets'])
			
			if len(df.index) == 0:
				return None, 0
			
			total = res['aggregations']['total']['value']
			
			df = self.filter_keys(df, terms=[term])
			df = self.filter_urls(df)
			
			if len(df.index) == 0:
				return None, 0
			
			df['key'] = df['key'].apply(lambda x: x + "/")
			df['proportion'] = df['doc_count'].div(total)
			
			return df.head(RETURN_SIZE), total
		except Exception as e:
			message = "term websites aggregation failed on {}".format(term)
			exception(message, e)
			raise SearchException(message)
	
	
	def term_urls_aggregation(self, term, include_rt=False, include_qt=False, include_re=False, website=None, date_range=None):
		agg = deepcopy(TERM_URLS)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		self.add_term_filter(agg, term)
		self.add_date_range(agg, date_range)
		
		if website:
			agg['aggs']['counts']['terms']['include'] = ".*" + website + ".*"
		
		try:
			res = self.search(agg)
			df = json_normalize(res['aggregations']['counts']['buckets'])
			
			if len(df.index) == 0:
				return None, 0
			
			df = self.filter_urls(df)
		
			if len(df.index) == 0:
				return None, 0
			
			total = res['aggregations']['total']['value']
			
			df['proportion'] = df['doc_count'].div(total)
			
			return df.head(RETURN_SIZE), total
		except Exception as e:
			message = "term urls aggregation failed on {}".format(term)
			exception(message, e)
			raise SearchException(message)
	
	
	def term_users_aggregation(self, term, include_rt=False, include_qt=False, include_re=False, date_range=None):
		agg = deepcopy(TERM_USERS)
		self.add_rt_filter(agg, include_rt, include_qt, include_re)
		self.add_term_filter(agg, term)
		self.add_date_range(agg, date_range)
		
		try:
			res = self.search(agg)
			
			df = json_normalize(res['aggregations']['counts']['buckets'])
			
			if len(df.index) == 0:
				return None, 0, 0
			
			total = res['aggregations']['total']['value']
			users = res['aggregations']['users']['value']
			
			df['proportion'] = df['doc_count'].div(total)
			
			return df, total, users
		except Exception as e:
			message = "term users aggregation failed on {}".format(term)
			exception(message, e)
			raise SearchException(message)
	
	
	
	
	
	
	
	
	
	