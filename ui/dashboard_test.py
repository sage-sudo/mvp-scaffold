# ui/dashboard_test.py

import sqlite3
import pandas as pd
import plotly.graph_objs as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from dynamics.dynamics_params_test import DB_PATH

DB_PATH = DB_PATH

def load_candles():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM candles ORDER BY timestamp ASC", conn)
    conn.close()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def build_dash_app(db=None):
    app = Dash(__name__)
    app.title = "Crypto Dashboard"

    app.layout = html.Div([
        html.H1("🚀 Live Crypto Dashboard", style={"textAlign": "center"}),
        dcc.Interval(id="interval", interval=60*1000, n_intervals=0),  # Update every 60 seconds
        dcc.Graph(id="candlestick-chart")
    ])

    @app.callback(
        Output("candlestick-chart", "figure"),
        Input("interval", "n_intervals")
    )
    def update_chart(n):
        df = load_candles()
        if df.empty:
            # Return an empty plot with a message if no data
            fig = go.Figure()
            fig.update_layout(
                title="No data yet — waiting for candles...",
                template="plotly_dark"
            )
            return fig

        fig = go.Figure(data=[
            go.Candlestick(
                x=df['timestamp'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name="Candles"
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
