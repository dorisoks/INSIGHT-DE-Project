import dash
from dash.dependencies import Input, Output, State
#from dash.dependencies import Input, Output, State, Event
import dash_core_components as dcc
import dash_html_components as html
import plotly.plotly as py
from plotly import graph_objs as go
from plotly.graph_objs.scatter import Marker
from plotly.graph_objs.layout import Margin
import dash_core_components as dcc
#from plotly.graph_objs import Data
from plotly.graph_objs import Scatter, Bar, Area, Histogram, Layout
from plotly.graph_objs import Scattermapbox
#from plotly.graph_objs import *
from flask import Flask
#from flask_cors import CORS
import pandas as pd
import numpy as np
import os

app = dash.Dash('CheckinApp')
server = app.server

from cassandra.cluster import Cluster

CASSANDRA_SERVER    = ['52.88.251.94']
CASSANDRA_NAMESPACE = "playground"
cluster = Cluster(CASSANDRA_SERVER)
session = cluster.connect()
session.execute("USE " + CASSANDRA_NAMESPACE)


if 'DYNO' in os.environ:
	app.scripts.append_script({
        'external_url': 'https://cdn.rawgit.com/chriddyp/ca0d8f02a1659981a0ea7f013a378bbd/raw/e79f3f789517deec58f41251f7dbb6bee72c44ab/plotly_ga.js'
    })

#mapbox_access_token = 'pk.eyJ1IjoiYWxpc2hvYmVpcmkiLCJhIjoiY2ozYnM3YTUxMDAxeDMzcGNjbmZyMmplZiJ9.ZjmQ0C2MNs1AzEBC_Syadg'
mapbox_access_token = 'pk.eyJ1IjoiamFyb2R5eWgiLCJhIjoiY2pybjN5bHVtMHBwYzN5cDlnZnIyc3c4MiJ9.Qhcq3sfowYmoUj_2Dw21vw'



app.layout = html.Div([
            html.Div([
                html.Div([
                    html.H2("Venue Visit Monitoring App", style={'font-family': 'Dosis'}),
                    html.Img(src="https://s3-us-west-1.amazonaws.com/plotly-tutorials/logo/new-branding/dash-logo-by-plotly-stripe.png",
                            style={
                                'height': '100px',
                                'float': 'right',
                                'position': 'relative',
                                'bottom': '145px',
                                'left': '5px'
                            },
                    ),
                ]),
                html.Div([
                    dcc.Graph(id='map-graph')
                ]),
		dcc.Interval(id='venuevisit-update', interval=3000, n_intervals=0),
            ], className="graph twelve coluns"),
            html.Div([
                html.Div([
		    html.H2("Find Out Your Friends Rating!", style={'font-family':'Dosis'}),
		    dcc.Input(
			id='textinput',
    			placeholder='Enter a value...',
    			type='text',
    			value='256627')
		]),
                html.Div([
                    dcc.Graph(id='map-graph-user')
                ]),
            ], className="a graph twelve coluns"),
], style={"padding-bottom": "120px","padding-top": "120px", "padding-left":"100px","padding-right":"100px"})


@app.callback(Output("map-graph", "figure"),
		[Input('venuevisit-update','n_intervals')])
def update_graph(interval):
    zoom = 12.0
    latInitial = 37.766083
    lonInitial = -122.448649
    bearing = 0
   
    venue_list = session.execute('select venue_id, latitude, longitude,SUM(visit) AS totalvisit from venuevisitloc GROUP BY venue_id LIMIT 500')
    venue_name = []
    venue_visit = []
    venue_lat = []
    venue_lon = []
    for venue in venue_list:
	venue_name.append(venue.venue_id)
  	venue_visit.append(venue.totalvisit)
	venue_lat.append(venue.latitude)
	venue_lon.append(venue.longitude)

    # to get fixed color range:
    y = np.array(venue_visit)
    color=np.array(['rgb(255,255,255)']*y.shape[0])
    for i in range(y.shape[0]):
        if y[i] < 10: color[i] = 'rgb(166,206,227)'
        elif y[i] >= 10 and y[i] < 100: color[i] = 'rgb(31,120,180)'
        elif y[i] >= 100 and y[i] < 200: color[i] = 'rgb(178,223,138)'
        elif y[i] >= 200 and y[i] < 400: color[i] = 'rgb(51,160,44)'
        elif y[i] >= 400 and y[i] < 800: color[i] = 'rgb(251,154,153)'
        else: color[i] = 'rgb(227,26,28)'

    return go.Figure(
        data=[
            Scattermapbox(
               # lat=["37.752443", "37.807771", "37.810088", "37.769361", "37.802067",
               #      "40.7127", "40.7589", "40.8075", "40.7489"],
               # lon=["-122.447543", "-122.473899", "-122.410428", "-122.485742",
               #      "-122.418840", "-74.0134", "-73.9851", "-73.9626",
               #       "-73.9680"],
                lat = venue_lat,
		lon = venue_lon,
		mode='markers',
                hoverinfo="text",
                text = venue_visit,
		#text=["Twin Peaks", "Golden Gate Bridge",
                #      "Pier 39", "Golden Gate Park",
                #      "Lombard Street", "One World Trade Center",
                #      "Times Square", "Columbia University",
                #      "United Nations HQ"],
                # opacity=0.5,
                marker=dict(
                    size=8,
                    #color="#ffa0a0"
		   # color = venue_visit,
		    color = color.tolist(),
#		    colorbar=dict(
 #             	  	title='Colorbar'
  #         	    ),
#	            colorscale = [[0, 'rgb(166,206,227)'], [0.25, 'rgb(31,120,180)'], [0.45, 'rgb(178,223,138)'], [0.65, 'rgb(51,160,44)'], [0.85, 'rgb(251,154,153)'], [1, 'rgb(227,26,28)']],
		   # colorscale='Jet',
		),
            ),
        ],
        layout=Layout(
            autosize=True,
            height=750,
            margin=Margin(l=0, r=0, t=0, b=0),
            showlegend=False,
            mapbox=dict(
                accesstoken=mapbox_access_token,
                 center=dict(
                     lat=latInitial, # 40.7272
                     lon=lonInitial # -73.991251
                 ),
                style='dark',
                bearing=bearing,
                zoom=zoom
            ) 
        )
    )


@app.callback(Output("map-graph-user", "figure"),
                [Input('textinput','value')])
def update_graph_user(user):
    zoom = 12.0
    latInitial = 37.766083
    lonInitial = -122.448649
    bearing = 0
  
    venue_list = session.execute("select venue_id, user_id, lat, log, rating  from friendratingloc WHERE user_id = \'{}\'".format(user))
    venue_name = []
    venue_lat = []
    venue_lon = []
    venue_rating = []
    for venue in venue_list:
        venue_name.append(venue.venue_id)
        venue_lat.append(venue.lat)
        venue_lon.append(venue.log)
	venue_rating.append(venue.rating)

    return go.Figure(
        data=[
            Scattermapbox(
                #lat=["37.752443", "37.807771", "37.810088", "37.769361", "37.802067"],
                #lon=["-122.447543", "-122.473899", "-122.410428", "-122.485742",
                #     "-122.418840"],
                lat = venue_lat,
		lon = venue_lon,
		mode='markers',
                hoverinfo="text",
                text = venue_rating,
 		#text=["1", "2", "3", "4", "5"],
                # opacity=0.5,
                marker=dict(
                    size=12,
                    color=venue_rating
                ),
            ),
        ],
        layout=Layout(
            autosize=True,
            height=750,
            margin=Margin(l=0, r=0, t=0, b=0),
            showlegend=False,
            mapbox=dict(
                accesstoken=mapbox_access_token,
                 center=dict(
                     lat=latInitial, # 40.7272
                     lon=lonInitial # -73.991251
                 ),
                style='dark',
                bearing=bearing,
                zoom=zoom
            )
        )
    )


external_css = ["https://cdnjs.cloudflare.com/ajax/libs/skeleton/2.0.4/skeleton.min.css",
                "//fonts.googleapis.com/css?family=Raleway:400,300,600",
                "//fonts.googleapis.com/css?family=Dosis:Medium",
                "https://cdn.rawgit.com/plotly/dash-app-stylesheets/62f0eb4f1fadbefea64b2404493079bf848974e8/dash-uber-ride-demo.css",
                "https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css"]


for css in external_css:
    app.css.append_css({"external_url": css})


if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0", port = 80)
