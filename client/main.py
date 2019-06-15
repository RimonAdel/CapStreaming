import dash
from dash.dependencies import Output,Input
import dash_core_components as dcc
import dash_html_components as html
import plotly
import plotly.graph_objs as go
import Controller
X = [0]
Y = [0]

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Interval(id='interval1', interval=1 * 1000, n_intervals= 0),
    dcc.Graph(id='live-graph', animate=True),
])


@app.callback(dash.dependencies.Output('live-graph', 'figure'),
    [dash.dependencies.Input('interval1', 'n_intervals')])
def update_interval(n):
    X,Y = Controller.getNumberOfTripsperDay()
    Controller.writeInFile()
    data = plotly.graph_objs.Scatter(
            x=list(X),
            y=list(Y),
            name='Scatter',
            mode= 'lines+markers'
            )
    return {'data': [data], 'layout': go.Layout(xaxis=dict(range=[0,365+10]),
                                         yaxis=dict(range=[min(Y), max(Y)]), )}

if __name__ == '__main__':
    Controller.run()
    app.run_server()
