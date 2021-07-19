"""
Loads event information from the supporting data files
for use in the dashboard time series graphs.
"""

import os, math
import pandas as pd

ANNOUNCEMENTS_CSV = os.path.join(os.path.dirname(__file__), '..', 'data', 'announcements.csv')
EVENTS_CSV = os.path.join(os.path.dirname(__file__), '..', 'data', 'health_events.csv')
COMBINED_EVENTS_CSV = os.path.join(os.path.dirname(__file__), '..', 'data', 'combined_events.csv')

# D3 colours ['#1F77B4', '#FF7F0E', '#2CA02C', '#D62728', '#9467BD', '#8C564B', '#E377C2', '#7F7F7F', '#BCBD22', '#17BECF']

EVENT_COLOURS = {
	'First cases in UK from abroad': '#E377C2',
	'First cases within UK': '#E377C2',
	'Other': '#E377C2',
	'Vaccine milestones': '#E377C2',
	'First COVID-19 variants': '#E377C2',
	'Vaccines approved': '#E377C2',
	'First people vaccinated': '#E377C2',
}

ANNOUNCEMENT_MARKERS = {
	'Government announcements': 'diamond-open',
	'Government events': 'diamond',
}

EVENT_TYPE_ORDER = [
	'Other',
	'First people vaccinated',
	'Vaccines approved',
	'Vaccine milestones',
	'First COVID-19 variants',
	'First cases within UK',
	'First cases in UK from abroad',
]

ANNOUNCEMENT_TYPE_ORDER = [
	'Government events',
# 	'Government announcements',
]

announcements_df = None
events_df = None
combined_events_df = None


def _reformat_date(uk_date):
	if uk_date and type(uk_date) is str:
		d, m, y = uk_date.split('/')
		return "{}-{}-{}".format(y, m, d)
	return None


def _max_date(row):
	date = None
	if row['empty_date'] is not None:
		if date is None or row['empty_date'] > date:
			date = row['empty_date']
	if row['announcement_date'] is not None:
		if date is None or row['announcement_date'] > date:
			date = row['announcement_date']
	if row['event_date'] is not None:
		if date is None or row['event_date'] > date:
			date = row['event_date']
	return date


def _min_date(row):
	date = None
	if row['empty_date'] is not None:
		if date is None or row['empty_date'] < date:
			date = row['empty_date']
	if row['announcement_date'] is not None:
		if date is None or row['announcement_date'] < date:
			date = row['announcement_date']
	if row['event_date'] is not None:
		if date is None or row['event_date'] < date:
			date = row['event_date']
	return date


def _wrap_desc(desc, wrap_len = 15):
	tokens = desc.split(' ')
	lines = []
	if len(tokens) > wrap_len:
		wrap_at = int(len(tokens) / 2)
		lines.append(' '.join(tokens[:wrap_at]))
		lines.append(' '.join(tokens[wrap_at:]))
	else:
		lines.append(' '.join(tokens))
	return '<br>'.join(lines)


def _announcement_label(row):
	label = ""
	if row['empty_date'] is not None and row['empty_desc'] is not None:
		label += '<b>' + row['empty_date'] + ' - Empty Announcement</b><br>' + _wrap_desc(row['empty_desc']) + '<br>'
	if row['announcement_date'] is not None and row['announcement_desc'] is not None:
		label += '<b>' + row['announcement_date'] + ' - Announcement</b><br>' + _wrap_desc(row['announcement_desc']) + '<br>'
	if row['event_date'] is not None and row['event_desc'] is not None:
		label += '<b>' + row['event_date'] + ' - Event</b><br>' + _wrap_desc(row['event_desc']) + '<br>'
	return label


def get_events():
	global events_df
	
	if events_df is None:
		events_df = pd.read_csv(EVENTS_CSV)
		events_df['date'] = events_df['date'].apply(lambda x: _reformat_date(x))
		events_df['colour'] = events_df['type'].apply(lambda x: get_colour(x))
		events_df['label'] = events_df.apply(lambda x: str(x['type']) + " - " + str(x['description']), axis=1)
		
		events_df = events_df.sort_values(by=['date'], ignore_index=True)
		events_df['offset'] = events_df.index
		events_df['offset'] = events_df['offset'].apply(lambda x: -(x % 6))
	
	return events_df
	

def get_announcements():
	global announcements_df
	
	if announcements_df is None:
		announcements_df = pd.read_csv(ANNOUNCEMENTS_CSV)
		announcements_df['empty_date'] = announcements_df['empty_date'].apply(lambda x: _reformat_date(x))
		announcements_df['announcement_date'] = announcements_df['announcement_date'].apply(lambda x: _reformat_date(x))
		announcements_df['event_date'] = announcements_df['event_date'].apply(lambda x: _reformat_date(x))
		announcements_df['date'] = announcements_df.apply(lambda x: _max_date(x), axis=1)
		announcements_df['min_date'] = announcements_df.apply(lambda x: _min_date(x), axis=1)
		announcements_df['label'] = announcements_df.apply(lambda x: _announcement_label(x), axis=1)
				
		announcements_df = announcements_df.sort_values(by=['min_date'], ignore_index=True)
		announcements_df['offset'] = announcements_df.index
		announcements_df['offset'] = announcements_df['offset'].apply(lambda x: 21 - ((x % 20)))
	
	return announcements_df


def get_combined_events():
	global combined_events_df
	
	if combined_events_df is None:
		df = pd.read_csv(COMBINED_EVENTS_CSV)
		df['date'] = df['date'].apply(lambda x: _reformat_date(x))
		df['colour'] = df['type'].apply(lambda x: get_colour(x))
		df['label'] = df['description'].apply(lambda x: _wrap_desc(x))
		df['marker'] = df['type'].apply(lambda x: get_marker(x))
		combined_events_df = df
	
	return combined_events_df


def get_colour(label):
	if label in EVENT_COLOURS:
		return EVENT_COLOURS[label]
	else:
		return '#17BECF'


def get_marker(label):
	if label in ANNOUNCEMENT_MARKERS:
		return ANNOUNCEMENT_MARKERS[label]
	else:
		return 'circle'
