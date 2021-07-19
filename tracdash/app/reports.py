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
	pass



def _layout(app, es_helper):

	return dbc.Container([
		dbc.Row([ html.H3("Reports", className="mb-3") ]),
			
		dbc.Row([ html.P("""
			The TRAC:COVID project has produced the following open-access reports regarding the
				clarity and reception of official messaging and the trustworthiness of information sources during the COVID-19 pandemic.
		""") ]),
		
		dbc.Row([
			dbc.Col([
				html.H4("Government management of the COVID-19 communication and public perception of the pandemic", className="mb-3 mt-3"),
			
				html.P([
					"""
					Dr Tatiana Tkacukova with Matt Gee, Dr Andrew Kehoe, Dr Robert Lawson, Dr Mark McGlashan
					"""
				]),
				html.P([
					"""
					School of English, Birmingham City University 
					"""
				]),
			
				html.H5("Executive summary", className="mb-3"),
			
				html.P([
					"""
					The study presented here discusses public reception of the UK-wide government restrictions and regulations in relation to the COVID-19 pandemic, focusing on language use on Twitter to (1) track the prevalence of diverse opinions and changes in public perceptions and (2) reflect on clarity of official messaging. Our report relates to the four themes outlined as part of the
					""",
					html.Em(["""
						Initial learning from the government’s response to the COVID-19 pandemic
						"""]),
					"""
					collated by the National Audit Office: 
					"""
				], className="text-justify"),
				html.Ul([
					html.Li(["transparency and public trust: providing transparent public-facing advice through clear and timely communication. "]),
					html.Li(["data and evidence: monitoring public perception of government advice, identifying issues with public compliance and quantifying different types of behaviours/reactions (compliance, non-compliance, call for stricter measures), validating the effectiveness of interventions by systematically gathering and evaluating end-user feedback (comments from the public). "]),
					html.Li(["coordination and delivery models: ensuring that public facing communication from government departments, central and local government, and public sector bodies is effectively coordinated and well-aligned. "]),
					html.Li(["supporting and protecting people: understanding the pandemic’s impact on different groups and the risk of widening inequalities. "]),
				]),
				html.P([
					"""
					The report is based on the results of the UKRI/AHRC-funded TRAC:COVID project carried out at Birmingham City University. The first section draws on the dashboard created as part of the project, accessible online at https://traccovid.com. The dashboard is an open access tool based on 84,138,394 tweets related to coronavirus posted by users in the UK between 1st January 2020 and 30th April 2021. The tool helps explore how social media have been used in the UK during the pandemic to talk about COVID-19. Our analysis shows that throughout the pandemic there has been a widespread support for the main measures used to contain the COVID-19 virus outbreak. In fact, a considerable number of tweets supported the introduction of even stronger measures than those imposed by the government, and many criticised non-compliance as a sign of selfish behaviour. The results also indicate a presence of users who actively used terms related to conspiracy theories and, although these views were found to be in the minority, it is important not to underestimate the role they play in undermining the efforts to contain the pandemic.  
					"""
				], className="text-justify"),
				html.P([
					"""
					The second part of the report reflects on the comprehensibility of official messages sent from government accounts and the accounts of public health bodies. The analysis shows a wide range of language-related problems, ranging from complex use of vocabulary and grammar and vague references to inaccurate information and potential exclusion of some of the intended recipients. 
					"""
				], className="text-justify"),
			], width=12),
		]),
		
		dbc.Row([
			dbc.Col([ ], width=6, className="text-left"),
			dbc.Col([ html.A("Read the report >>>", href="http://www.open-access.bcu.ac.uk/id/eprint/11960", className="btn btn-primary") ], width=6, className="text-right")
		]),
		
		dbc.Row([ html.H3("Dashboard", className="mb-3 mt-5") ]),
		
		dbc.Row([ html.P("""
			Continue to the next page to view the full dashboard including frequent words, hashtags and websites.
		""") ]),
		
		dbc.Row([
			dbc.Col([ ], width=6, className="text-left"),
			dbc.Col([ html.A("Dashboard >>>", href="dashboard", className="btn btn-primary") ], width=6, className="text-right")
		]),
	])



def layout(app, es_helper):
	_callbacks(app, es_helper)
	return _layout(app, es_helper)








