"""
Builds the app and handles routing to pages.
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, MATCH

from .elasticsearch import ESHelper

from . import index, full, reports


def _navbar(path):
	if not path.endswith("/"):
		path += "/"
	
	nav = [
		dbc.NavLink("Dashboard", href=(path + "dashboard")),
		dbc.NavLink("About", href=(path)),
		dbc.NavLink("Reports", href=(path + "reports")),
		dbc.NavLink("@traccovid", href="https://twitter.com/TracCovid")
	]
	
	navbar = dbc.Navbar(
		[
			html.A(
				[
					dbc.NavbarBrand([
						html.Img(src=(path + "assets/logo.png"), height="60px", className="float-left mr-3"),
						html.H3("TRAC:COVID", className="ml-2"),
						html.H6("Trust and Communication: a Coronavirus Online Visual Dashboard", className="ml-2")
					])
				],
				href=path
			),
			
			dbc.Nav(nav,
				className="ml-auto",
				navbar=True
			)
		],
		className="mb-4 sticky-top py-0"
	)
	
	return navbar



def _footer():
	return html.Footer(
		dbc.Container(
			[
				dbc.Row([
					dbc.Col([
						html.A("Project Details", href="https://www.bcu.ac.uk/english/research/english-linguistics/rdues/research-projects/trac-covid"),
						html.Br(),
						html.A("GitHub Repository", href="https://github.com/rdues/traccovid"),
						html.Br(),
						"Contact ",
						html.A("@traccovid", href="https://twitter.com/traccovid"),
						html.Br(),
						html.Br(),
						html.A("Terms and Privacy Policy", href="/traccovid/assets/terms.html"),
						html.Br(),
						html.A("Accessibility Statement", href="/traccovid/assets/accessibility.html"),
					], width=4, className="text-left"),
					
					dbc.Col([
						html.A([
							html.Img(src="https://i.creativecommons.org/l/by/4.0/80x15.png", alt="Creative Commons License"),
							" The work is licensed under a Creative Commons Attribution 4.0 International License"
						], href="http://creativecommons.org/licenses/by/4.0/", rel="license"),
						html.Br(),
						html.Br(),
						"To cite the project please use:",
						html.Br(),
						"Kehoe, A., Gee, M., Lawson, R., McGlashan, M., Tkacukova, T. (2021). ",
						html.Em("TRAC:COVID – Trust and Communication: A Coronavirus Online Visual Dashboard"),
						". Available online at ",
						html.A("https://traccovid.com", href="https://traccovid.com")
					], width=4, className="text-left", style={"fontSize": "10pt"}),
					
					dbc.Col([
						html.A([ html.Img(src="assets/bcu_logo_web.png", height=60) ], href="https://www.bcu.ac.uk/"),
						html.Br(),
						html.Br(),
						html.A([ html.Img(src="assets/AHRC_logo_web.png", height=60) ], href="https://ahrc.ukri.org/"),
					], width=4, className="text-left"),
				]),
			]
		),
		className="footer bg-light p-5 mt-4 text-muted text-center",
		style={
			"minHeight": "100px"
		}
	)



def _create_layouts(app, es_helper):
	layouts = {
		'about': 		index.layout(app, es_helper),
		'dashboard':	full.layout(app, es_helper),
		'reports':		reports.layout(app, es_helper),
	}
	return layouts
	


def _create_callbacks(app, es_helper, layouts):
	@app.callback(
		Output('page-content', 'children'),
		Input('url', 'pathname')
	)
	def display_page(path):
		path_end = path.split('/')[-1]
	
		if path_end in layouts:
			return layouts[path_end]
		else:
			return layouts['about']



def prepare_app(es, index_name, cache_path=None, path="/traccovid/"):
	es_helper = ESHelper(es, index_name, cache_path=cache_path)
	
	app = dash.Dash(__name__, 
		external_stylesheets=[dbc.themes.COSMO, ""],
		external_scripts=["https://www.googletagmanager.com/gtag/js?id=G-NL85KPYYCS"],
		url_base_pathname=path,
		title="TRAC:COVID",
		suppress_callback_exceptions=True
	)
	
	app.layout = html.Div([
		html.A('Skip to content', href='#page-content', style={
			'position': 'absolute',
 			'top': '-100px',
 			'left': '0px',
		}),
	
		dcc.Location(id='url'),
		
		_navbar(path),
		
		html.Main(id='page-content',
		style={
			"minHeight": "1000px"
		}),
		
		_footer()
	])
	
	layouts = _create_layouts(app, es_helper)
	
	_create_callbacks(app, es_helper, layouts)
	
	return app



