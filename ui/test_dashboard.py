# ui/test_dashboard.py

import pandas as pd
import plotly.graph_objs as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output

def build_dash_app(db):
    app = Dash(__name__)
    app.title = "Crypto Trading Bot Dashboard"

    app.layout = html.Div([
        html.H1("🚀 Live Crypto Dashboard", style={"textAlign": "center"}),
        dcc.Interval(id="interval", interval=60*1000, n_intervals=0),  # 1-minute refresh
        dcc.Graph(id="candlestick-chart")
    ])

    @app.callback(
        Output("candlestick-chart", "figure"),
        Input("interval", "n_intervals")
    )
    def update_chart(n):
        df = db.query_df("SELECT * FROM candles ORDER BY timestamp ASC")
        fig = go.Figure(data=[
            go.Candlestick(
                x=df['timestamp'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name="1H Candles"
            )
        ])

        fig.update_layout(
            xaxis_rangeslider_visible=False,
            margin=dict(l=10, r=10, t=30, b=10),
            template="plotly_dark",
            height=600
        )

        return fig

    return app
