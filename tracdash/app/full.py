"""
The full dashboard which displays all of the word, hashtag and website aggregations.
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

import numpy as np
import pandas as pd 
import networkx as nx

from .elasticsearch import ESHelper

from . import util
from . import events



def _callbacks(app, es_helper):

	# left side figures
	
	@app.callback(
		[Output('fig-types-list', 'figure'), Output("agg-search", "invalid"), Output("agg-search", "value"), Output("types-total", "children")],
		[Input('main-filter', 'value'), Input('agg-search-form', 'n_submit'), Input('agg-search-reset', 'n_clicks')],
		State('agg-search', 'value')
	)
	@util.callback_log
	def update_types_list(filter, search_submits, reset_clicks, search):
		changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
		if 'agg-search-reset' in changed_id:
			search = ""
	
		search, invalid = util.validate_search(search)
	
		df, total = es_helper.types_aggregation(
			include_rt=('rt' in filter),
			include_qt=('qt' in filter),
			include_re=('re' in filter),
			search=search
		)
		
		if df is None:
			return util.nodata(), invalid, search, [""]
				
		fig = px.bar(df, y="key", x="doc_count", text="key", orientation='h', log_x=True,
			labels={
				"key": "Word",
				"doc_count": "Frequency"
			},
			color_discrete_sequence=util.DEFAULT_COLOUR_SEQUENCE
		)
		util.update_left_figure(fig, len(df.index))
		
		total_message = ""
		if search:
			total_message = "Tweets matched: {:,}".format(total)
		
		return fig, invalid, search, [total_message]
	
	
	@app.callback(
		[Output('fig-keywords-list', 'figure'), Output("keywords-total", "children")],
		[Input('main-filter', 'value'), Input('agg-search-form', 'n_submit'), Input('agg-search-reset', 'n_clicks')],
		State('agg-search', 'value')
	)
	@util.callback_log
	def update_keywords_list(filter, search_clicks, reset_clicks, search):
		changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
		if 'agg-search-reset' in changed_id:
			search = ""
		
		search, invalid = util.validate_search(search)
		
		df, total = es_helper.keywords_aggregation(
			include_rt=('rt' in filter),
			include_qt=('qt' in filter),
			include_re=('re' in filter),
			search=search
		)
		
		if df is None:
			return util.nodata(), [""]
		
		fig = px.bar(df, y="key", x="keyness", text="key", orientation='h', log_x=True,
			labels={
				"key": "Keyword",
				"keyness": "Keyness",
				"doc_count": "Frequency"
			},
			color_discrete_sequence=util.DEFAULT_COLOUR_SEQUENCE
		)
		util.update_left_figure(fig, len(df.index))
		fig.update_traces(hovertemplate = 'x%{x:.1f}')
		
		total_message = ""
		if search:
			total_message = "Tweets matched: {:,}".format(total)
		
		return fig, [total_message]
	
	
	@app.callback(
		[Output('fig-hashtags-list', 'figure'), Output("hashtags-total", "children")],
		[Input('main-filter', 'value'), Input('agg-search-form', 'n_submit'), Input('agg-search-reset', 'n_clicks')],
		State('agg-search', 'value')
	)
	@util.callback_log
	def update_hashtags_list(filter, search_clicks, reset_clicks, search):
		changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
		if 'agg-search-reset' in changed_id:
			search = ""
		
		search, invalid = util.validate_search(search)
		
		df, total = es_helper.hashtags_aggregation(
			include_rt=('rt' in filter),
			include_qt=('qt' in filter),
			include_re=('re' in filter),
			search=search
		)
		
		if df is None:
			return util.nodata(), [""]
		
		fig = px.bar(df, y="key", x="doc_count", text="key", orientation='h', log_x=True,
			labels={
				"key": "Hashtag",
				"doc_count": "Frequency"
			},
			color_discrete_sequence=util.DEFAULT_COLOUR_SEQUENCE
		)
		util.update_left_figure(fig, len(df.index))
		
		total_message = ""
		if search:
			total_message = "Tweets matched: {:,}".format(total)
		
		return fig, [total_message]
	
	
	@app.callback(
		[Output('fig-websites-list', 'figure'), Output("websites-total", "children")],
		[Input('main-filter', 'value'), Input('agg-search-form', 'n_submit'), Input('agg-search-reset', 'n_clicks')],
		State('agg-search', 'value')
	)
	@util.callback_log
	def update_websites_list(filter, search_clicks, reset_clicks, search):
		changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
		if 'agg-search-reset' in changed_id:
			search = ""
		
		search, invalid = util.validate_search(search)
		
		df, total = es_helper.websites_aggregation(
			include_rt=('rt' in filter),
			include_qt=('qt' in filter),
			include_re=('re' in filter),
			search=search
		)
		
		if df is None:
			return util.nodata(), [""]
		
		fig = px.bar(df, y="key", x="doc_count", text="key", orientation='h', log_x=True,
			labels={
				"key": "Website",
				"doc_count": "Frequency"
			},
			color_discrete_sequence=util.DEFAULT_COLOUR_SEQUENCE
		)
		util.update_left_figure(fig, len(df.index))
		
		total_message = ""
		if search:
			total_message = "Tweets matched: {:,}".format(total)
		
		return fig, [total_message]
	
	
	# right side terms
	
	def _update_active_terms(data, terms):
		if terms is None:
			terms = []
		
		if data is not None:	
			for p in data['points']:
				term = p['y']
				if not term in terms:
					terms.append(term)
				
		return terms
	
	
	@app.callback(
		Output('terms-filter', 'value'),
		[Input('fig-types-list', 'clickData'), Input('fig-keywords-list', 'clickData'), Input('fig-hashtags-list', 'clickData'), Input('fig-websites-list', 'clickData')],
		State('terms-filter', 'value')
	)
	@util.callback_log
	def update_active_terms_from_graph(types_data, keywords_data, hashtags_data, websites_data, terms):	
		ctx = dash.callback_context
		if not ctx.triggered:
			pass
		else:
			if ctx.triggered[0]['prop_id'].startswith('fig-types-list'):
				return _update_active_terms(types_data, terms)
			elif ctx.triggered[0]['prop_id'].startswith('fig-keywords-list'):
				return _update_active_terms(keywords_data, terms)
			elif ctx.triggered[0]['prop_id'].startswith('fig-hashtags-list'):
				return _update_active_terms(hashtags_data, terms)
			elif ctx.triggered[0]['prop_id'].startswith('fig-websites-list'):
				return _update_active_terms(websites_data, terms)
			else:
				pass
	
	
	@app.callback(
		Output('mem-history', 'data'),
		Input('terms-filter', 'value'), State('mem-history', 'data')
	)
	def update_session_history(values, history):
		if history is None:
			history = []
		if values is not None:
			for term in values:
				if term in history:
					history.remove(term)
				history.append(term)
		
		return history

	
	@app.callback(
		Output('terms-filter', 'options'),
		Input('mem-history', 'modified_timestamp'), State('mem-history', 'data')
	)
	def update_terms_history(modified, history):
		if history is None:
			history = []
		history.reverse()
		return [{'label': term, 'value': term} for term in history]
	
	
	# right side figures
		
	
	@app.callback(
		Output('compare-dates', 'children'),
		[Input('date-picker', 'start_date'), Input('date-picker', 'end_date')]
	)
	def update_compare_dates(start_date, end_date):
		if start_date or end_date:
			if start_date is None:
				start_date = es_helper.start_date
			if end_date is None:
				end_date = es_helper.end_date
			return [" ({} - {})".format(util.format_date(start_date), util.format_date(end_date))]
		return [""]
	
	
	@app.callback(
		Output('fig-time-series', 'figure'),
		[Input('main-filter', 'value'), Input('terms-filter', 'value'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date'), Input('event-options', 'value')]
	)
	@util.callback_log
	def update_time_series_graph(filter, terms, start_date, end_date, event_options):
		if terms is None or len(terms) == 0:
			df = es_helper.time_series_totals(
				include_rt=('rt' in filter),
				include_qt=('qt' in filter),
				include_re=('re' in filter)
			)
			df = df.rename(columns={'_total': 'Total tweets per day'})
						
			fig = px.line(df, x="_key_as_string", y=df.columns,
				labels={
					"_key_as_string": "Date",
					"variable": "Total tweets per day",
					"value": "Total tweets per day"
				},
				color_discrete_sequence=util.COLOUR_SEQUENCE
			)
			
			fig.update_layout(
				showlegend=False
			)
		else:
			df = es_helper.time_series_search(
				terms,
				include_rt=('rt' in filter),
				include_qt=('qt' in filter),
				include_re=('re' in filter),
				normalise=('norm-ts' in filter)
			)
			
			if df is None:
				return util.nodata()
			
			y_axis_title = "Frequency"
			if 'norm-ts' in filter:
				y_axis_title = "Percent per day"
		
			fig = px.line(df, x="_key_as_string", y=df.columns,
				labels={
					"_key_as_string": "Date",
					"variable": "Term",
					"value": y_axis_title
				},
				color_discrete_sequence=util.COLOUR_SEQUENCE
			)
			
			if 'norm-ts' in filter:
				fig.update_layout(
					yaxis=dict(
						tickformat = "0.1%"
					)
				)
		
		util.update_time_series(fig)
		
		
		# events 
		
		if len(event_options) > 0:
			fig.update_layout(height=360)
			
			max_value = 0
			for col in df.columns:
				if col != '_key_as_string':
					max_value = max(max_value, df[col].max())
						
			type_order = []
			if 'health' in event_options:
				type_order.extend(events.EVENT_TYPE_ORDER)
			if 'gov' in event_options:
				type_order.extend(events.ANNOUNCEMENT_TYPE_ORDER)
			
			events_df = events.get_combined_events()
			offset = 1.0
			offset_inc = 0.033
			time_delta = timedelta(days=7)
			blank = 'rgba(0,0,0,0)'
			day_delta = timedelta(days=1)
			
			for t in type_order:
				if t in events.ANNOUNCEMENT_TYPE_ORDER:
					offset = 1.1
				elif t in events.EVENT_TYPE_ORDER:
					offset = 1.05

				type_df = events_df.query('type == "{}"'.format(t))
				type_df['offset'] = max_value * offset
				
				for i, row in type_df.iterrows():				
					mid = datetime.strptime(row['date'][:10], '%Y-%m-%d')
					start = mid
					end = mid + time_delta
				
					trace = go.Scatter(
						x=[ mid ],
						y=[ row['offset'] ],
						text=[ row['label'] ],
						customdata=[ row['type'] ],
						mode='markers',
						marker=dict(
							color=row['colour'],
							symbol=row['marker'],
							size=9,
							line=None
						),
						hovertemplate='<span style="font-size:8pt;"><b>%{customdata}</b><br>%{text}</span><extra></extra>',
						showlegend=False,
					)
					fig.add_trace(trace)
		
		
		# date range 
		
		if start_date is None:
			start_date = es_helper.start_date
		if end_date is None:
			end_date = es_helper.end_date
		
		fig.update_layout(xaxis_range=[start_date, end_date])
		
		return fig
	
	
	@app.callback(
		[Output('fig-types-comparison', 'figure'), Output('fig-types-network', 'figure')],
		[Input('main-filter', 'value'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date'), Input('terms-filter', 'value'), Input('compare-tabs', 'value')]
	)
	@util.callback_log
	def update_types_comparison_graph(filter, start_date, end_date, terms, tab):
		if tab != 'tab-1' or terms is None or len(terms) == 0:
			return [util.blank(), util.noselection()]
				
		df, rank_df = es_helper.cooccuring_types_search(
			terms,
			include_rt=('rt' in filter),
			include_qt=('qt' in filter),
			include_re=('re' in filter),
			date_range=[start_date, end_date]
		)
		
		if df is None or rank_df is None:
			return [util.blank(), util.nodata()]
		
		terms = [ t for t in terms if t in df.columns ]
		
		heatmap_fig = util.build_collocate_heatmap(rank_df, terms)
		network_fig = util.build_collocate_network_graph(df, terms)
		
		return [heatmap_fig, network_fig]
	
	
	@app.callback(
		[Output('fig-hashtags-comparison', 'figure'), Output('fig-hashtags-network', 'figure')],
		[Input('main-filter', 'value'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date'), Input('terms-filter', 'value'), Input('compare-tabs', 'value')]
	)
	@util.callback_log
	def update_hashtags_comparison_graph(filter, start_date, end_date, terms, tab):		
		if tab != 'tab-2' or terms is None or len(terms) == 0:
			return [util.blank(), util.noselection()]
						
		df, rank_df = es_helper.cooccuring_hashtags_search(
			terms,
			include_rt=('rt' in filter),
			include_qt=('qt' in filter),
			include_re=('re' in filter),
			date_range=[start_date, end_date]
		)
		
		if df is None or rank_df is None:
			return [util.blank(), util.nodata()]
		
		terms = [ t for t in terms if t in df.columns ]
		
		heatmap_fig = util.build_collocate_heatmap(rank_df, terms)
		network_fig = util.build_collocate_network_graph(df, terms)
		
		return [heatmap_fig, network_fig]
	
	
	# right side tabs
	
	@app.callback(
		Output('term-tabs', 'children'),
		[Input('main-filter', 'value'), Input('terms-filter', 'value'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date')]
	)
	@util.callback_log
	def update_term_tabs(filter, terms, start_date, end_date):		
		if terms is None or len(terms) == 0:
			return [dcc.Tab([],
						label="Select a word, hashtag, or website above...",
						style=util.TAB_STYLE, selected_style=util.SELECTED_TAB_STYLE
					)]
				
		children = []
		for term in terms:
			children.append( _term_tab(app, es_helper, term, filter, [start_date, end_date]) )
		
		return children
	
	
	# right side tab content
	
	@app.callback(
		[
			Output({'type': 'term-types', 'index': MATCH}, 'figure'),
			Output({'type': 'term-bi-grams', 'index': MATCH}, 'figure'),
			Output({'type': 'term-tri-grams', 'index': MATCH}, 'figure'),
			Output({'type': 'term-types', 'index': MATCH}, 'className'),
			Output({'type': 'term-bi-grams', 'index': MATCH}, 'className'),
			Output({'type': 'term-tri-grams', 'index': MATCH}, 'className'),
			Output({'type': 'term-types', 'index': MATCH}, 'clickData'),		# clear these for next event
			Output({'type': 'term-bi-grams', 'index': MATCH}, 'clickData'),
			Output({'type': 'term-tri-grams', 'index': MATCH}, 'clickData'),
		],
		[
			Input('main-filter', 'value'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date'), Input({'type': 'term-id', 'index': MATCH}, 'data'),
			Input({'type': 'term-types', 'index': MATCH}, 'clickData'),
			Input({'type': 'term-bi-grams', 'index': MATCH}, 'clickData'),
			Input({'type': 'term-tri-grams', 'index': MATCH}, 'clickData'),
		],
		[
			State({'type': 'term-types', 'index': MATCH}, 'figure'),
			State({'type': 'term-bi-grams', 'index': MATCH}, 'figure'),
			State({'type': 'term-tri-grams', 'index': MATCH}, 'figure'),
		]
	)
	@util.callback_log
	def update_types_treemap(filter, start_date, end_date, term, uni_event_data, bi_event_data, tri_event_data, uni_fig, bi_fig, tri_fig):
		if uni_fig is None:
			uni_fig = util.nodata()
		if bi_fig is None:
			bi_fig = util.nodata()
		if tri_fig is None:
			tri_fig = util.nodata()
		
		ret = [
			uni_fig, bi_fig, tri_fig, 
			'd-none', 'd-none', 'd-none',
			None, None, None
		]
			
		include = util.parse_treemap_click(tri_event_data)
		n = 4
		if not include:
			include = util.parse_treemap_click(bi_event_data)
			n = 3
		if not include:
			include = util.parse_treemap_click(uni_event_data)
			n = 2
		if not include:
			n = 1
				
		if include:
			if include.startswith('<'):
				n -= 2
				ret[(n-1)+3] = 'd-block'
				del ret[n-1]['data'][0]['level']
				return ret
			elif include == 'Words':
				n = 1
				ret[(n-1)+3] = 'd-block'
				del ret[n-1]['data'][0]['level']
				return ret
			elif n == 4:
				n -= 1
				ret[(n-1)+3] = 'd-block'
				return ret
				
		if n == 1:		
			df, total = es_helper.term_types_aggregation(
				term,
				include_rt=('rt' in filter),
				include_qt=('qt' in filter),
				include_re=('re' in filter),
				date_range=[start_date, end_date]
			)
			
			if df is None:
				df = pd.DataFrame(columns = ['key','doc_count','proportion'], data = [
					{'key':'No matching data found or data below frequency cut-off', 'doc_count':1, 'proportion':1}
				])
			
			df['root'] = 'Words'
		else:
			df, total = es_helper.term_ngrams_aggregation(
				term,
				n,
				include=include,
				include_rt=('rt' in filter),
				include_qt=('qt' in filter),
				include_re=('re' in filter),
				date_range=[start_date, end_date]
			)
			
			if df is None:
				df = pd.DataFrame(columns = ['key','doc_count','proportion'], data = [
					{'key':'No matching data found or data below frequency cut-off', 'doc_count':1, 'proportion':1}
				])
			
			df['root'] = ('<' * (n-1)) + ' ' + include
		
		root_freq = 0
		root_prop = 0
		if n == 1:
			root_freq = total
			root_prop = 1
		if n == 2:
			try:
				root_freq = uni_event_data['points'][0]['customdata'][0]
				root_prop = uni_event_data['points'][0]['customdata'][1]
			except:
				pass
		elif n == 3:
			try:
				root_freq = bi_event_data['points'][0]['customdata'][0]
				root_prop = bi_event_data['points'][0]['customdata'][1]
			except:
				pass
			
		colour_range = [
			0.0,
			df['doc_count'].max()
		]
	
		fig = px.treemap(
			df,
			path=['root', 'key'],
			values='doc_count',
			color='doc_count',
			color_continuous_scale=util.COLOUR_SCALE,
			custom_data=['doc_count', 'proportion'],
			range_color = colour_range
		)
		fig.data[0]['customdata'][-1][0] = root_freq
		fig.data[0]['customdata'][-1][1] = root_prop
		util.update_tree_map(fig)
		
		
		ret[n-1] = fig
		ret[(n-1)+3] = 'd-block'
		
		return ret
	
	
	@app.callback(
		Output({'type': 'term-hashtags', 'index': MATCH}, 'figure'),
		[Input('main-filter', 'value'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date'), Input({'type': 'term-id', 'index': MATCH}, 'data')]
	)
	@util.callback_log
	def update_hashtags_treemap(filter, start_date, end_date, term):		
		hashtags_df, total = es_helper.term_hashtags_aggregation(
			term,
			include_rt=('rt' in filter),
			include_qt=('qt' in filter),
			include_re=('re' in filter),
			date_range=[start_date, end_date]
		)
		
		if hashtags_df is None:
			hashtags_df = pd.DataFrame(columns = ['key','doc_count','proportion'], data = [
				{'key':'No matching data found or data below frequency cut-off', 'doc_count':1, 'proportion':1}
			])
		
		colour_range = [
			0.0,
			hashtags_df['doc_count'].max()
		]
		
		hashtags_df['root'] = 'Hashtags'
	
		fig = px.treemap(
			hashtags_df,
			path=['root', 'key'],
			values='doc_count',
			color='doc_count',
			color_continuous_scale=util.COLOUR_SCALE,
			custom_data=['doc_count', 'proportion'],
			range_color = colour_range
		)
		fig.data[0]['customdata'][-1][0] = total
		fig.data[0]['customdata'][-1][1] = 1
		util.update_tree_map(fig)
				
		return fig
	
	
	@app.callback(
		[Output({'type': 'term-websites', 'index': MATCH}, 'figure'), Output({'type': 'term-websites', 'index': MATCH}, 'className')],
		[Input('main-filter', 'value'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date'), Input({'type': 'term-id', 'index': MATCH}, 'data')]
	)
	@util.callback_log
	def update_websites_treemap(filter, start_date, end_date, term):
		if term.endswith('/'):
			websites_fig_classes = 'd-none'
			fig = {}
		else:
			websites_fig_classes = ''
					
			websites_df, total = es_helper.term_websites_aggregation(
				term,
				include_rt=('rt' in filter),
				include_qt=('qt' in filter),
				include_re=('re' in filter),
				date_range=[start_date, end_date]
			)
			
			if websites_df is None:
				return util.nodata(), 'd-none'
			
			colour_range = [
				0.0,
				websites_df['doc_count'].max()
			]
			
			websites_df['root'] = 'Websites'
			
			fig = px.treemap(
				websites_df,
				path=['root', 'key'],
				values='doc_count',
				color='doc_count',
				color_continuous_scale=util.COLOUR_SCALE,
				custom_data=['doc_count', 'proportion'],
				range_color = colour_range
			)
			fig.data[0]['customdata'][-1][0] = total
			fig.data[0]['customdata'][-1][1] = 1
			util.update_tree_map(fig)
		
		return fig, websites_fig_classes
	
	
	@app.callback(
		Output({'type': 'term-urls', 'index': MATCH}, 'data'),
		[Input('main-filter', 'value'), Input({'type': 'term-websites', 'index': MATCH}, 'clickData'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date')],
		State({'type': 'term-id', 'index': MATCH}, 'data')
	)
	@util.callback_log
	def update_urls_table(filter, event_data, start_date, end_date, term):	
		website = None
		if event_data is not None:
			points = event_data.get('points', None)
			if points is not None and len(points) > 0:
				p = points[0]
				if 'label' in p and 'percentEntry' in p and p['percentEntry'] != 1:
					website = p['label']
				
		urls_df, total = es_helper.term_urls_aggregation(
			term,
			include_rt=('rt' in filter),
			include_qt=('qt' in filter),
			include_re=('re' in filter),
			website=website,
			date_range=[start_date, end_date]
		)
		
		if urls_df is None:
			return [{'doc_count': 0, 'proportion': 0.0, 'key': 'No matching data found or data below frequency cut-off'}]
		
		urls_df['proportion'] = urls_df['proportion'].multiply(100).round(2)
		
		return urls_df.to_dict('records')
	
	
	@app.callback(
		Output({'type': 'term-rt', 'index': MATCH}, 'figure'),
		[Input('main-filter', 'value'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date'), Input({'type': 'term-id', 'index': MATCH}, 'data')]
	)
	@util.callback_log
	def update_rt_bar(filter, start_date, end_date, term):		
		df = es_helper.term_rt_aggregation(
			term,
			include_rt=('rt' in filter),
			include_qt=('qt' in filter),
			include_re=('re' in filter),
			date_range=[start_date, end_date]
		)
		
		if df is None:
			return util.nodata()
		
		fig = px.bar(df,
			x="doc_count",
			y="type",
			color="key_as_string",
			color_discrete_sequence=[util.DEFAULT_COLOUR, util.DEFAULT_LIGHT_COLOUR]
		)
		util.update_rt_bar(fig)
			
		return fig


	@app.callback(
		[Output({'type': 'term-users', 'index': MATCH}, 'figure'), Output({'type': 'term-users-guage', 'index': MATCH}, 'figure')],
		[Input('main-filter', 'value'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date'), Input({'type': 'term-id', 'index': MATCH}, 'data')]
	)
	@util.callback_log
	def update_users_pie(filter, start_date, end_date, term):		
		users_df, total, users = es_helper.term_users_aggregation(
			term,
			include_rt=('rt' in filter),
			include_qt=('qt' in filter),
			include_re=('re' in filter),
			date_range=[start_date, end_date]
		)
		
		if users_df is None:
			users_df = pd.DataFrame(columns = ['key','doc_count','proportion'], data = [
				{'key':'No matching data found or data below frequency cut-off', 'doc_count':1, 'proportion':1}
			])
		
		users_df = users_df.drop(columns=['key'])
	
		fig = px.pie(
			users_df.head(50),
			values='proportion',
			color_discrete_sequence=util.DEFAULT_COLOUR_SEQUENCE,
			hole=0.3
		)
		util.update_user_pie(fig)
		
		gauge = go.Figure()
		gauge.add_trace(go.Indicator(
			domain = {'row': 0, 'column': 0},
			mode = "number",
			value = users,
			title = {'text': "Accounts", "font": {"size": 20 } },
			number = {"font": {"size": 40} },
		))
		gauge.add_trace(go.Indicator(
			domain = {'row': 0, 'column': 1},
			mode = "number",
			value = total,
			title = {'text': "Tweets", "font": {"size": 20 } },
			number = {"font": {"size": 40} },
		))
		util.update_user_gauge(gauge)
				
		return fig, gauge



def _term_tab(app, es_helper, term, filter, date_range):
	term_hash = md5(term.encode("utf-8")).hexdigest()
	
	tab_label = term
	
	date_label = ""
	if date_range and len(date_range) == 2 and (date_range[0] or date_range[1]):
		if date_range[0] is None:
			date_range[0] = es_helper.start_date
		if date_range[1] is None:
			date_range[1] = es_helper.end_date
		date_label = " ({} - {})".format( util.format_date(date_range[0]), util.format_date(date_range[1]))
	
	if term.endswith('/'):
		websites_fig_classes = 'd-none'
	else:
		websites_fig_classes = ''

	urls_table = dash_table.DataTable(
		id = {
			'type': 'term-urls',
			'index': term_hash
		},
		columns = [
			{"name": "Freq.", "id": "doc_count"},
			{"name": "Url", "id": "key"}
		],
		data = [],
		style_table = {
			'maxHeight': '450px',
			'overflow': 'auto'
		},
		style_cell_conditional=[
			{
				'if': {'column_id': 'key'},
				'textAlign': 'left'
			}
		],
		style_data_conditional=[
			{
				'if': {
					'state': 'selected'  # 'active' | 'selected'
				},
			   'backgroundColor': util.BACKGROUND_COLOUR,
			   'border': '1px solid ' + util.DEFAULT_COLOUR
			}
		]
	)

	header_classes = ''
	row_classes = 'mb-4'

	return dcc.Tab([
		dcc.Store(id={
			'type': 'term-id',
			'index': term_hash
		},
		data=term,
		storage_type='session'
		),
		
		html.Div([
			dbc.Container([
				dbc.Row([
					dbc.Col([
						dbc.Button("?", id="co-words-tooltip-target", size="sm", className="float-right"),
						dbc.Tooltip(f"""
							Hover over a box to see the number and percentage of tweets which contain the word, out of the tweets which contain "{term}".
							Darker colours and larger boxes represent higher frequency.
							Up to 100 words shown.
							Clicking a word will show frequent phrases of two words.
							Clicking a two word phrase will show frequent phrases of three words.
							Please note that phrases may take some time to load.
							""",
							target="co-words-tooltip-target",
						),
					
						html.H5([ 'Words in the same tweets as "{}"{}'.format(term, date_label) ], className=header_classes),
						
						util.loading([
							dcc.Graph(id={
								'type': 'term-types',
								'index': term_hash
							},
							config=util.GRAPH_CONFIG)
						]),
						util.loading([
							dcc.Graph(id={
								'type': 'term-bi-grams',
								'index': term_hash
							},
							config=util.GRAPH_CONFIG)
						]),
						util.loading([
							dcc.Graph(id={
								'type': 'term-tri-grams',
								'index': term_hash
							},
							config=util.GRAPH_CONFIG)
						])
					],
					className='p-1 mt-2',
					width=12),
				],
				className = row_classes),
		
				dbc.Row([
					dbc.Col([
						dbc.Button("?", id="co-hashtags-tooltip-target", size="sm", className="float-right"),
						dbc.Tooltip(f"""
							Hover over a box to see the number and percentage of tweets which contain the hashtag, out of the tweets which contain "{term}".
							Darker colours and larger boxes represent higher frequency.
							Up to 100 hashtags shown.
							""",
							target="co-hashtags-tooltip-target",
						),
					
						html.H5([ 'Hashtags in the same tweets as "{}"{}'.format(term, date_label) ], className=header_classes),
						
						util.loading([
							dcc.Graph(id={
								'type': 'term-hashtags',
								'index': term_hash
							},
							config=util.GRAPH_CONFIG)
						])
					],
					className='p-1',
					width=12),
				],
				className = row_classes),
			
				dbc.Row([
					dbc.Col([
						dbc.Button("?", id="co-websites-tooltip-target", size="sm", className="float-right"),
						dbc.Tooltip(f"""
							Hover over a box to see the number and percentage of tweets which contain a link to the website, out of the tweets which contain "{term}".
							Darker colours and larger boxes represent higher frequency.
							Up to 100 websites shown.
							Selecting a website will filter the list of URLs below.
							""",
							target="co-websites-tooltip-target",
						),
					
						html.H5([ 'Websites linked to in tweets containing "{}"{}'.format(term, date_label) ], className=header_classes),
						
						util.loading([
							dcc.Graph(id={
								'type': 'term-websites',
								'index': term_hash
							},
							config=util.GRAPH_CONFIG,
							className=websites_fig_classes
							)
						]),
						
						util.loading([
							urls_table
						])
					],
					className='p-1',
					width=12),
				],
				className = row_classes),
				
				dbc.Row([
					dbc.Col([
						dbc.Button("?", id="sharing-tooltip-target", size="sm", className="float-right"),
						dbc.Tooltip("""
							Note that the three possibilities (replies, quote tweets and retweets) are only displayed if they have been selected in the dashboard options (above).
							It is possible for a tweet to fall into more than one category.
							""",
							target="sharing-tooltip-target",
						),
						
						html.H5([ 'Percentage of tweets containing "{}" which are replies, quote tweets or retweets{}'.format(term, date_label) ], className=header_classes),
						
						util.loading([
							dcc.Graph(id={
								'type': 'term-rt',
								'index': term_hash
							},
							config=util.GRAPH_CONFIG)
						])
					],
					className='p-1',
					width=12),
				],
				className = row_classes),
			
				dbc.Row([
					dbc.Col([
						dbc.Button("?", id="co-users-tooltip-target", size="sm", className="float-right"),
						dbc.Tooltip(f"""
							The 50 largest percentages are shown.
							Some variation between accounts should be expected, but a large segment indicates an account tweeted "{term}" more than others.
							""",
							target="co-users-tooltip-target",
						),
						
						html.H5([ 'Distribution of accounts posting tweets containing "{}"{}'.format(term, date_label) ], className=header_classes),
					],
					className='p-1',
					width=12),
				],
				className = row_classes),
				
				dbc.Row([
					dbc.Col([
						util.loading([
							dcc.Graph(id={
								'type': 'term-users-guage',
								'index': term_hash
							},
							config=util.GRAPH_CONFIG)
						])
					],
					className='p-1',
					width=6),
					
					dbc.Col([
						util.loading([
							dcc.Graph(id={
								'type': 'term-users',
								'index': term_hash
							},
							config=util.GRAPH_CONFIG)
						])
					],
					className='p-1',
					width=6),
				],
				className = row_classes),
				
			],
			fluid=True)
		],
		className='border')
	],
	label=tab_label,
	style=util.TAB_STYLE, selected_style=util.SELECTED_TAB_STYLE)




def _layout(es_helper):

	left_tab_words_content = util.loading([
		html.Div([
			html.P("Most frequent words in the dataset", className="py-1 my-0 font-italic"),
			
			dcc.Graph(id='fig-types-list', config=util.GRAPH_CONFIG),
			
			html.P([""], id="types-total", className="mt-2 text-muted"),
		],
		id='types-list', className='border p-1', style=util.LEFT_TAB_STYLE
		)
	])
	
	left_tab_keywords_content = util.loading([
		html.Div([
			dbc.Button("?", id="keywords-tooltip-target", size="sm", className="float-right"),
			dbc.Tooltip("""
					Keywords are words used more often than would typically be expected.
					These have been calculated by comparing our COVID-19 specific sample to a generic sample of tweets.
					The values indicate how many times more frequent a word is in the COVID-19 sample than in the generic sample.
				""",
				target="keywords-tooltip-target",
			),
			html.P("Words occurring more than is typical", className="py-1 my-0 font-italic"),
			
			dcc.Graph(id='fig-keywords-list', config=util.GRAPH_CONFIG),
			
			html.P([""], id="keywords-total", className="mt-2 text-muted"),
		],
		id='keywords-list', className='border p-1', style=util.LEFT_TAB_STYLE
		)
	])
	
	left_tab_hashtags_content = util.loading([
		html.Div([
			html.P("Most frequent hashtags in the dataset", className="py-1 my-0 font-italic"),
			
			dcc.Graph(id='fig-hashtags-list', config=util.GRAPH_CONFIG),
				
			html.P([""], id="hashtags-total", className="mt-2 text-muted"),
		],
		id='hashtags-list', className='border p-1', style=util.LEFT_TAB_STYLE
		)
	])
	
	left_tab_links_content = util.loading([
		html.Div([
			dbc.Button("?", id="websites-tooltip-target", size="sm", className="float-right"),
			dbc.Tooltip("""
					Note that twitter.com addresses are split into sub paths to show the type of content being linked.
				""",
				target="websites-tooltip-target",
			),
			html.P("Most frequent websites in the dataset", className="py-1 my-0 font-italic"),
			
			dcc.Graph(id='fig-websites-list', config=util.GRAPH_CONFIG),
						
			html.P([""], id="websites-total", className="mt-2 text-muted"),
		],
		id='websites-list', className='border p-1', style=util.LEFT_TAB_STYLE
		)
	])
	
	left_tabs = dcc.Tabs(
		[
			dcc.Tab(left_tab_words_content, label="Words", style=util.TAB_STYLE, selected_style=util.SELECTED_TAB_STYLE),
			dcc.Tab(left_tab_keywords_content, label="Keywords", style=util.TAB_STYLE, selected_style=util.SELECTED_TAB_STYLE),
			dcc.Tab(left_tab_hashtags_content, label="Hashtags", style=util.TAB_STYLE, selected_style=util.SELECTED_TAB_STYLE),
			dcc.Tab(left_tab_links_content, label="Websites", style=util.TAB_STYLE, selected_style=util.SELECTED_TAB_STYLE),
		]
	)
	
	# other left side content
	
	left_options = html.Div(
		[
			dbc.Form([
				dbc.FormGroup([
					dbc.InputGroup([
						dbc.Input(id="agg-search", placeholder="Search term...", type="text", value="", debounce=True),
						dbc.Button("X", type="button", color="primary", outline=True, id='agg-search-reset', n_clicks=0, className="text-right"),
						dbc.Button("Search", type="submit", color="primary", id='agg-search-submit', n_clicks=0, className="text-right"),
						dbc.FormFeedback("Please enter a single term of three or more letters", valid=False)
					])
				]),
			], id="agg-search-form", n_submit=0),
			dcc.Checklist(id='main-filter',
				options = [
					{'label': 'include retweets', 'value': 'rt'},
					{'label': 'include quote tweets', 'value': 'qt'},
					{'label': 'include replies', 'value': 're'},
					{'label': 'show percentages on date graph', 'value': 'norm-ts'}
				],
				value = ['re', 'qt', 'norm-ts'],
				labelStyle = {'display':'block'},
				inputClassName = 'mr-2',
				className = 'm-3'
			)
		],
		style = {
			'padding': '4px',
			'border': '1px solid #d6d6d6',
		}
	)
	
	
	# right side elements
	
	right_header = html.Div([
		html.H5("Selected items"),
		
		html.P("""
			Click on a word, hashtag or website on the left to view it in more detail below.
			Currently selected items will appear here:
		"""),
	
		dcc.Dropdown(id='terms-filter',
			options = [],
			value = [],
			multi = True,
			searchable = False,
			placeholder="",
			style={
				'color': util.DEFAULT_COLOUR
			}
		),
		
		html.H5("See below for more details", className='mt-3'),
		
		html.Div([
			html.A("Frequency over time", href="#timeseries", className="btn btn-primary m-2", role="button"),
			html.A("Words and hashtags in the same tweets", href="#compare", className="btn btn-primary m-2", role="button")
		], className='text-center')
	], className="mt-4")
	
	compare_tabs = [
		dcc.Tab(
			[
				html.Div(
					[	
						dbc.Row(
							[
								dbc.Col(
									[						
										util.loading([ 
											dcc.Graph(id='fig-types-comparison', config=util.GRAPH_CONFIG),
										]),
									],
									width=3,
									className="border-right"
								),
								dbc.Col(
									[
										util.loading([ dcc.Graph(id='fig-types-network', config=util.GRAPH_CONFIG) ]),
									],
									width=9
								),
							]
						)
					],
					className='border p-2'
				)
			],
			label="Words in the same tweets", style=util.TAB_STYLE, selected_style=util.SELECTED_TAB_STYLE
		),
		dcc.Tab(
			[
				html.Div(
					[
						dbc.Row(
							[
								dbc.Col(
									[
										util.loading([
											dcc.Graph(id='fig-hashtags-comparison', config=util.GRAPH_CONFIG),
										]),
									],
									width=3,
									className="border-right"
								),
								dbc.Col(
									[
										util.loading([ dcc.Graph(id='fig-hashtags-network', config=util.GRAPH_CONFIG) ]),
									],
									width=9
								),
							]
						)
					],
					className='border p-2'
				)
			],
			label="Hashtags in the same tweets", style=util.TAB_STYLE, selected_style=util.SELECTED_TAB_STYLE
		)
	]
	
	# right side grid
	
	right_grid = [
		dbc.Row([
			dbc.Col([
				html.H3("Frequency over time", id='timeseries', className="mb-3"),
				
				dbc.Button("?", id="time-series-tooltip-target", size="sm", className="float-right"),
				dbc.Tooltip([
						"""
						Adjusting the date range will modify all graphs below.
						Turning on events will display diamonds and circles at the top of the graph,
						hover over them to reveal the event that happened on that date.
						"""
					],
					target="time-series-tooltip-target"
				),
				
				html.Div([
					html.Div("Date range: ", className='float-left mt-3 mr-1'),
					
					dcc.DatePickerRange(
						id='date-picker',
						min_date_allowed=util.MIN_DATE,
						max_date_allowed=util.MAX_DATE,
						display_format="DD MMM YYYY",
						clearable=True,
						start_date_placeholder_text=util.MIN_DATE_PLACEHOLDER,
						end_date_placeholder_text=util.MAX_DATE_PLACEHOLDER,
						className="float-left"
					),
					
					dcc.Checklist(
						id='event-options',
						options=[
							{'label': 'Show government events', 'value': 'gov'},
							{'label': 'Show health related events', 'value': 'health'},
						],
						value=[],
						labelStyle={
							'display': 'inline-block',
							'marginRight': '8px',
							'marginLeft': '8px',
							'backgroundPosition': 'center right',
							'backgroundRepeat': 'no-repeat',
							'paddingRight': '16px'
						},
						inputStyle={'marginRight': '4px'},
						className='float-left mt-3',
					),
					
				], className='mb-3'),
				
				html.Div([], className="clearfix"),
				
				util.loading([
					dcc.Graph(id='fig-time-series', config=util.GRAPH_CONFIG)
				])
			],
			width=True,
			className='my-4'),
		]),
		dbc.Row([
			dbc.Col([
				dbc.Button("?", id="compare-tooltip-target", size="sm", className="float-right m-2"),
				dbc.Tooltip([
						"""
						Left figure: The selected items (word, hashtag or website) are shown at the top of the figure.
						Darker colours indicate words/hashtags that appear more often with the selected items (based on rank).
						Sorted by best rank. Top 50 for each item shown.
						""",
						html.Br(), html.Br(),
						"""
						Right figure: The selected items (word, hashtag or website) are shown near the centre of the graph surrounded by a circle.
						Words/hashtags which appear in the same tweets as the selected items are shown with lines connecting them to the central items. 
						Thicker lines and closer proximity indicate that they appear together in more tweets.
						Top 50 for each item shown.
						"""
					],
					target="compare-tooltip-target"
				),
			
				html.H3("Comparison", id='compare', className="mb-3"),
			
				html.P([
					"Words/hashtags which appear in the same tweets as the items selected above are listed",
					html.Span([""], id="compare-dates"),
					". ",
					"Multiple items can be chosen for comparison."
				]),
				
				dcc.Tabs(compare_tabs, id='compare-tabs'),
			],
			width=True,
			className='my-4'),
		]),
		dbc.Row([
			dbc.Col([
				html.H3("Word, hashtag and website use in detail", id='detail', className="mb-3"),
				
				dcc.Tabs([], id='term-tabs')
			],
			width=True,
			className='my-4'),
		])
	]


	# put it all together

	return dbc.Container([
		dcc.Store(id='mem-terms', storage_type='session'),
		dcc.Store(id='mem-history', storage_type='session'),

		dbc.Row([
			dbc.Col([
				html.H3("Words, hashtags and websites", className="mb-3"),
				
				html.P("""
					The TRAC:COVID dashboard shows the words, hashtags and websites used in UK tweets during the COVID-19 pandemic.
					The bar charts on the left are sorted by frequency, showing the items used most often in the dataset.
					On the right are options to search the lists and filter by retweets, quote tweets and replies.
				"""),
				
				html.P("""
					Clicking on items in the bar charts on the left will show additional information about that word, hashtag or website below.
				"""),
			],
			width=True,
			className='mb-4'),
		]),
	
		dbc.Row([
			dbc.Col([
				left_tabs,
				
				dbc.Button("Expand list...", id="btn-expand", color="primary", outline=True, size="sm")
			],
			width=7),
			
			dbc.Col([
				left_options,
				
				right_header
			],
			width=5),
		]),
		
		dbc.Row([
			dbc.Col(
				right_grid
			,
			width=True,
			className='my-4'),
		]),
	], fluid=True)



def layout(app, es_helper):
	_callbacks(app, es_helper)
	return _layout(es_helper)








