"""
The index page which shows summary information about the dataset.
"""

import re, math
from pprint import pprint
from hashlib import md5
from datetime import datetime, timedelta
from copy import deepcopy

import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, MATCH

import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots

import numpy as np
import pandas as pd 
import networkx as nx

from .elasticsearch import ESHelper
from . import events

from . import util



def _callbacks(app, es_helper):
	
	@app.callback(
		[Output('fig-totals', 'figure'), Output('fig-totals-time-series', 'figure')],
		Input('mem-totals', 'data')
	)
	@util.callback_log
	def update_time_series_graph(mem):		
		df = es_helper.time_series_total_sharing()
		df = df.rename(columns={
			"total": "Total",
			"rt_counts": "Retweets",
			"qt_counts": "Quote Tweets",
			"re_counts": "Replies",
		})
		
		sums = [
			{'key': 'Total', 'value': df['Total'].sum()},
			{'key': 'Retweets', 'value': df['Retweets'].sum()},
			{'key': 'Quote Tweets', 'value': df['Quote Tweets'].sum()},
			{'key': 'Replies', 'value': df['Replies'].sum()}
		]
		fig_bar = px.bar(sums,
			x="value",
			y="key",
			color="key",
			color_discrete_sequence=util.COLOUR_SEQUENCE
		)
		util.update_bar_figure(fig_bar)
		
		fig_ts = px.line(df, x="_key_as_string", y=df.columns,
			labels={
				"_key_as_string": "Date",
				"variable": "Number of tweets",
				"value": "Number of tweets"
			},
			color_discrete_sequence=util.COLOUR_SEQUENCE
		)
		
		util.update_time_series(fig_ts)
		
		return fig_bar, fig_ts



def _layout(app, es_helper):

	accounts_total = es_helper.total_users
	precision = max(0, int(math.log10(accounts_total) - 2) )
	accounts_count = int(round(accounts_total, -precision))

	return dbc.Container([
		dcc.Store(id='mem-totals', storage_type='session'),
				
		dbc.Row([ html.P("""
			The TRAC:COVID project investigates online conversation surrounding the COVID-19 pandemic
				using aggregated data sampled from Twitter (so individual tweets are not shown).
			The dashboard combines Corpus Linguistic tools (the linguistic and computational study of textual data)
				with data visualisations to allow the interpretation of a large number of tweets.
			Frequent patterns of word and hashtag use, word and hashtag combinations, change over time and
				the proliferation of web links can be viewed in the dashboard.
		""") ]),
		
		dbc.Row([
			dbc.Col([ ], width=6, className="text-left"),
			dbc.Col([ html.A("Dashboard >>>", href="dashboard", className="btn btn-primary") ], width=6, className="text-right")
		]),
		
		dbc.Row([ html.H3("About", className="mb-3") ]),

		dbc.Row([ html.P([
			"The project is being run by ",
			html.A("Birmingham City University", href="https://www.bcu.ac.uk/"),
			" and funded by the ",
			html.A("Arts and Humanities Research Council (AHRC)", href="https://ahrc.ukri.org/"),
			" as part of UKRI’s Covid-19 funding. Read about the ",
			html.A("project aims on the Birmingham Institute of Media and English website", href="https://www.bcu.ac.uk/english/research/english-linguistics/rdues/research-projects/trac-covid"),
			". Get in touch with the project team via Twitter: ",
			html.A("@traccovid", href="https://twitter.com/TracCovid"),
			"."
		]) ]),
		
		dbc.Row([ html.P([
			"To cite the project please use: ",
			"Kehoe, A., Gee, M., Lawson, R., McGlashan, M., Tkacukova, T. (2021). ",
			html.Em("TRAC:COVID – Trust and Communication: A Coronavirus Online Visual Dashboard"),
			". Available online at https://traccovid.com."
		]) ]),
		
		dbc.Row([ html.H3("Reports", className="mb-3") ]),
		
		dbc.Row([ html.P("""
			The TRAC:COVID project has produced two reports, each
				focussing on a potentially dangerous area of miscommunication relating to COVID-19.
				These case studies approach the problem from a linguistic perspective, examining the
				clarity and reception of official messaging and the trustworthiness of information sources.
		""") ]),
		
		dbc.Row([
			dbc.Col([ ], width=6, className="text-left"),
			dbc.Col([ html.A("Reports >>>", href="reports", className="btn btn-primary") ], width=6, className="text-right")
		]),
		
		dbc.Row([ html.H3("Total tweets sampled: {:,.0f}".format(es_helper.corpus_size(include_rt=True, include_re=True, include_qt=True)), className="mb-3") ]),
		
		dbc.Row([
			html.P("""
				The tweets were sampled between 1st January 2020 and 30th April 2021 using the Historical PowerTrack API.
				The tweets must have been classified as English language and the tweets or user profiles must match the United Kingdom country parameter.
				The tweets must mention COVID or Coronavirus (upper or lower case spelling) in the text or match frequent COVID-19 related hashtags.
			"""),
			html.P("""
				The query used was:
				(contains:coronavirus OR contains:covid OR #coronavirus OR #covid OR #covid19 OR #covid2019 OR #covid-19 OR #covid-2019 OR #covid_19 OR #covid_2019 OR #covidー19 OR #covidー2019 OR #coronavirusuk OR #coviduk OR #covid19uk OR #covid2019uk OR #covid-19uk OR #covid-2019uk OR #covid_19uk OR #covid_2019uk OR #covidー19uk OR #covidー2019uk) lang:en (place_country:gb OR profile_country:gb)
			"""),
			html.P("""
				The total numbers of tweets, including retweets, quote tweets and replies are shown here:
			""")
		]),
		
		dbc.Row([
			dbc.Col([
				util.loading([
					dcc.Graph(id='fig-totals', config=util.GRAPH_CONFIG)
				])
			], width=12)
		]),
		
		dbc.Row([ html.H3("Tweets over time", className="mb-3") ]),
		
		dbc.Row([ html.P("""
			This graph shows the number tweets in our sample on a day-by-day basis.
			The number of tweets in our sample increases rapidly between 8th - 12th March 2020 and stays high during the rest of March.
			From the start of April onwards the number of tweets declines until the start of September.
			Other periods with an increased number of tweets are September and October 2020 and January 2021.
		""") ]),
		
		dbc.Row([
			dbc.Col([
				util.loading([
					dcc.Graph(id='fig-totals-time-series', config=util.GRAPH_CONFIG)
				])
			], width=12)
		]),
		
		dbc.Row([ html.H3("Total accounts: {:,.0f}".format(accounts_count), className="my-3") ]),
		
		dbc.Row([ html.P("""
			This is the total number of Twitter accounts represented in our dataset.
			Note that for performance reasons, account counts are estimated.
		""") ]),
				
		dbc.Row([ html.P("""
			Continue to the next page to view the full dashboard including frequent words, hashtags and websites.
		""", className="mt-5") ]),
		
		dbc.Row([
			dbc.Col([ ], width=6, className="text-left"),
			dbc.Col([ html.A("Dashboard >>>", href="dashboard", className="btn btn-primary") ], width=6, className="text-right")
		]),
	])



def layout(app, es_helper):
	_callbacks(app, es_helper)
	return _layout(app, es_helper)








