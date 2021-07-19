"""
Various util functions to support the app.
"""

import re, math, traceback
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

import numpy as np
import pandas as pd 
import networkx as nx


DASHBOARD_VERSION = "1.2"



# logging helpers

def _log(message):
	# Python logging isn't compatible with plotly & gunicorn combination
	# so capture stdout instead
	print(message, flush=True)


def info(message):
	obj = {
		'time': str(datetime.now()),
		'message': message,
	}
	_log('INFO {}'.format(obj))


def exception(message, e):
	obj = {
		'time': str(datetime.now()),
		'message': message,
		'exception': traceback.format_exception(None, e, None)
	}
	_log('EXCEPTION {}'.format(obj))


# decorator function to log callbacks
def callback_log(func):
    def func_wrapper(*args, **kwargs):
        f_code = func.__code__
        cb = {
        	'time': str(datetime.now()),
        	'func': func.__name__,
        	'def': f_code.co_varnames[:f_code.co_argcount + f_code.co_kwonlyargcount],
        	'args': args,
        	'kwargs': kwargs
        }
        _log('CALLBACK {}'.format(cb))
        return func(*args, **kwargs)
    return func_wrapper



# dashboard consts

MIN_DATE = datetime(2020, 1, 1)		# for use with date picker
MAX_DATE = datetime(2021, 5, 1)	# date picker requires following day (bug in plotly?)
MIN_DATE_PLACEHOLDER = '01 Jan 2020'
MAX_DATE_PLACEHOLDER = '30 Apr 2021'

# COLOUR_SEQUENCE = ['#008cba', '#6f42c1', '#f04124', '#e99002', '#20c997', '#6610f6', '#e83e8c', '#fd7e14', '#43ac6a', '#5bc0de']
COLOUR_SEQUENCE = px.colors.qualitative.D3

DEFAULT_COLOUR = COLOUR_SEQUENCE[0]
DEFAULT_COLOUR_SEQUENCE = [ DEFAULT_COLOUR ]
DEFAULT_LIGHT_COLOUR = 'rgb(202, 222, 240)'
DEFAULT_LIGHT_COLOUR_SEQUENCE = [ DEFAULT_LIGHT_COLOUR ]
BACKGROUND_COLOUR = 'rgb(247, 251, 255)'
COLOUR_SCALE = 'blues'

TAB_STYLE = {
	'padding': '8px'
}

SELECTED_TAB_STYLE = {
 	'borderTop': '2px solid ' + DEFAULT_COLOUR,
	'padding': '8px'
}

LEFT_TAB_STYLE = {
	'height': '400px',
	'minHeight': '400px',
	'overflowY': 'scroll'
}


MODE_BAR_REMOVE = [
	'zoom2d', 'pan2d', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d',
	'zoom3d', 'pan3d', 'rbitRotation', 'tableRotation', 'handleDrag3d', 'resetCameraDefault3d', 'resetCameraLastSave3d', 'hoverClosest3d',
	'hoverClosestCartesian', 'hoverCompareCartesian',
	'zoomInGeo', 'zoomOutGeo', 'resetGeo', 'hoverClosestGeo',
	'hoverClosestGl2d', 'hoverClosestPie', 'toggleHover', 'resetViews', 'toImage: sendDataToCloud', 'toggleSpikelines', 'resetViewMapbox'
]

GRAPH_CONFIG = {
	'scrollZoom': False,
	'displaylogo': False,
	'modeBarButtonsToRemove': MODE_BAR_REMOVE,
	
}


# graph helpers

def loading(children):
	return dcc.Loading(children, type='dot', color=DEFAULT_COLOUR)


def blank():
	return {
		"layout": {
			"xaxis": {
				"visible": False
			},
			"yaxis": {
				"visible": False
			},
			"annotations": [
				{
					"text": " ",
					"xref": "paper",
					"yref": "paper",
					"showarrow": False,
					"font": {
						"size": 10
					}
				}
			],
			"height": 100
		}
	}


def noselection():
	return {
		"layout": {
			"xaxis": {
				"visible": False
			},
			"yaxis": {
				"visible": False
			},
			"annotations": [
				{
					"text": "Select a word, hashtag or website above...",
					"xref": "paper",
					"yref": "paper",
					"showarrow": False,
					"font": {
						"size": 10
					}
				}
			],
			"height": 100
		}
	}


def nodata():
	return {
		"layout": {
			"xaxis": {
				"visible": False
			},
			"yaxis": {
				"visible": False
			},
			"annotations": [
				{
					"text": "No matching data found<br>or data below frequency cut-off",
					"xref": "paper",
					"yref": "paper",
					"showarrow": False,
					"font": {
						"size": 10
					}
				}
			],
			"height": 100
		}
	}


SEARCH_REGEX = re.compile(r"^[\w\-\'\.]{3,}$")

def validate_search(search):
	invalid = False
	if search:
		search = search.lower()
		m = SEARCH_REGEX.search(search)
		if m is None:
			search = None
			invalid = True
	else:
		search = None
	
	return search, invalid


TOP_MARGIN = 0
BOTTOM_MARGIN = 12
RIGHT_MARGIN = 0
LEFT_MARGIN = 0

TITLE = dict(
	text='traccovid.com ',
	x=1,
	y=0,
	xanchor='right',
	yanchor='bottom',
	font=dict(
		size=8,
	),
	pad=dict(
		l=0, r=4, t=0, b=4,
	),
)


def update_bar_figure(fig):
	fig.update_layout(
		height=100,
		margin=dict(
			l=LEFT_MARGIN,
			r=RIGHT_MARGIN,
			b=BOTTOM_MARGIN,
			t=TOP_MARGIN,
			pad=0
		),
		showlegend=False,
		xaxis=dict(
			title_text = "Number of tweets",
			tickformat = ".3s",
		),
		yaxis=dict(
			title_text = "",
		),
		plot_bgcolor='rgba(255,255,255,0)',
		title=TITLE,
		dragmode=False
	)
	fig.update_traces(hovertemplate='%{x}<extra></extra>')


def update_left_figure(fig, n=100):
	fig.update_layout(
		autosize=False,
 		width=400,
		height=30 * n,
		margin=dict(
			l=LEFT_MARGIN,
			r=32,
			b=BOTTOM_MARGIN,
			t=TOP_MARGIN,
			pad=0
		),
		xaxis=dict(
			visible=False,
			showticklabels=False,
			fixedrange=True
		),
		yaxis=dict(
			autorange='reversed',
			visible=False,
			showticklabels=False,
			fixedrange=True
		),
		paper_bgcolor='rgb(255,255,255)',
		plot_bgcolor='rgb(255,255,255)',
		title=TITLE,
		# dragmode=False	# use fixed range on each axis instead of this
	)
	fig.update_traces(hovertemplate = '%{x:.3s}')


def update_time_series(fig):
	fig.update_layout(
		height=320,
		margin=dict(
			l=LEFT_MARGIN,
			r=RIGHT_MARGIN,
			b=BOTTOM_MARGIN,
			t=TOP_MARGIN,
			pad=0
		),
		legend=dict(
			orientation='h',
			xanchor='auto',
			yanchor='top',
			title_text=""
		),
		xaxis=dict(
			title_text = "",
			tickformat = "%d %b %y"
		),
		plot_bgcolor=BACKGROUND_COLOUR,
 		hovermode="x unified",
 		hoverdistance=1,
 		title=TITLE,
 		dragmode=False
	)
	fig.update_traces(mode="lines", hovertemplate=None)
	fig.update_traces(cliponaxis=False, selector=dict(type='scatter'))


def update_comparison_figure(fig, n=50):
	fig.update_xaxes(side="top")
	fig.update_yaxes(side="right")
	fig.update_layout(
		height = (20 * n) + 100,
		margin=dict(
			l=LEFT_MARGIN,
			r=RIGHT_MARGIN,
			b=BOTTOM_MARGIN,
			t=TOP_MARGIN,
			pad=0
		),
		coloraxis_showscale=False,
		title=TITLE,
		dragmode=False
	)
	fig.update_traces(hovertemplate='%{x}<br>%{y}<extra></extra>')


def get_network_figure_layout():
	return go.Layout(
		paper_bgcolor='rgb(255,255,255)',
		plot_bgcolor='rgb(255,255,255)',
		xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
		yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
		showlegend=False,
		title=TITLE,
		height=1000,
		dragmode=False,
		modebar_bgcolor='rgba(255,255,255,0)'
	)


def update_tree_map(fig):
	fig.update_layout(
		height=450,
		margin=dict(
			l=LEFT_MARGIN,
			r=RIGHT_MARGIN,
			b=BOTTOM_MARGIN,
			t=TOP_MARGIN,
			pad=0
		),
		paper_bgcolor='white',
		plot_bgcolor='white',
		showlegend=False,
		coloraxis_showscale=False,
		title=TITLE,
		dragmode=False
	)
	if len(fig.data) > 0:
		fig.data[0].hovertemplate = '%{label}<br>%{customdata[0]:,}<br>%{customdata[1]:.2%}'


def update_rt_bar(fig):
	fig.update_layout(
		height=100,
		margin=dict(
			l=LEFT_MARGIN,
			r=RIGHT_MARGIN,
			b=BOTTOM_MARGIN,
			t=TOP_MARGIN,
			pad=0
		),
		showlegend=False,
		xaxis=dict(
			title_text = "",
			range = [0, 1],
			tickformat = ".0%",
			ticks="inside",
			nticks=20
		),
		yaxis=dict(
			title_text = ""
		),
		plot_bgcolor='rgb(255,255,255)',
		title=TITLE,
		dragmode=False
	)
	fig.update_traces(hovertemplate='%{x}<extra></extra>')


def update_user_gauge(fig):
	fig.update_layout(
		grid = dict(rows = 1, columns = 2, pattern = "independent"),
		height = 300,
	)


def update_user_pie(fig):
	fig.update_traces(
 		textposition='inside',
		hoverinfo='label',
 		textinfo='value',
		hovertemplate='%{value:.3%}<extra></extra>',
		texttemplate='%{value:.1%}',
	)
	fig.update_layout(
		height = 300,
		margin=dict(
			l=LEFT_MARGIN,
			r=RIGHT_MARGIN,
			b=BOTTOM_MARGIN,
			t=TOP_MARGIN,
			pad=0
		),
		title=TITLE,
	)


def parse_treemap_click(event_data):
	if event_data is not None:
		points = event_data.get('points', None)
		if points is not None and len(points) > 0:
			p = points[0]
			if 'label' in p:
				return p['label']
	return None


def parse_dates(dates):
	if dates and 'xaxis.range[0]' in dates and 'xaxis.range[1]' in dates:
		return [ dates['xaxis.range[0]'][:10] , dates['xaxis.range[1]'][:10] ]
	return None


def format_date(date_str):
	return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")


def build_collocate_heatmap(rank_df, terms):
	colls = rank_df['_key'].tolist()
	rank_df = rank_df.drop(columns=['_key','_max'])
	nodes = rank_df.columns.tolist()
	
	heatmap_fig = px.imshow(rank_df,
		y = colls,
		color_continuous_scale = COLOUR_SCALE
	)
	update_comparison_figure(heatmap_fig, n=len(colls))
	
	return heatmap_fig
	
	
def build_collocate_network_graph(df, terms):
	G = nx.Graph()
	
	for index, row in df.iterrows():
		for term in terms:
			G.add_edge(term, row['_key'], weight=row[term])
	
	fixed_pos = {}
	if len(terms) > 1:
		radius = 1.0
		for t in range(0, len(terms)):
			theta = (((t + 1.0) / len(terms)) * math.pi * 2) + (math.pi / 2.0)
			fixed_pos[ terms[t] ] = [ radius * math.cos(theta) , radius * math.sin(theta) ]
	else:
		fixed_pos[ terms[0] ] = [0, 0]
	
	pos = nx.spring_layout(G, iterations=1000, pos=fixed_pos, fixed=fixed_pos.keys(), seed=1)
	
	all_weights = sorted( [ x[2]['weight'] for x in G.edges(data=True) ] )
	min_weight = 0
	max_weight = all_weights[-1]
	max_width = 10
	
	edge_traces = []
	for edge in G.edges():
		weight = G.edges()[edge]['weight']
		width = max_width * (weight - min_weight) / (max_weight - min_weight)
					
		node1 = edge[0]
		node2 = edge[1]
		x0, y0 = pos[node1]
		x1, y1 = pos[node2]
		trace = go.Scatter(
			x=[x0, x1, None], 
			y=[y0, y1, None],
			line = dict(width=width, color=DEFAULT_LIGHT_COLOUR),
			mode='lines',
			hoverinfo='skip'
		)
		edge_traces.append(trace)
	
	node_trace = go.Scatter(
		x=[], y=[], text=[],
		mode='markers+text',
		marker=dict(color=[], size=[], line=None)
	)
	for node in G.nodes():
		x, y = pos[node]
		node_trace['x'] += tuple([x])
		node_trace['y'] += tuple([y])
		if node in terms:
			node_trace['marker']['color'] += tuple([DEFAULT_LIGHT_COLOUR])
			node_trace['marker']['size'] += tuple([50])
			node_trace['text'] += tuple(["<b>{}</b>".format(node)])
		else:	
			node_trace['marker']['color'] += tuple(['rgba(0,0,0,0)'])
			node_trace['marker']['size'] += tuple([4])
			node_trace['text'] += tuple(["{}".format(node)])
		node_trace['hoverinfo'] = 'skip'
	
	network_fig = go.Figure(
		layout = get_network_figure_layout()
	)
	
	for trace in edge_traces:
		network_fig.add_trace(trace)
	network_fig.add_trace(node_trace)
	
	network_fig.update_yaxes(
		scaleanchor = "x",
		scaleratio = 1,
	)
	
	return network_fig


