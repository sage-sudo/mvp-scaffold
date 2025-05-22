# ui/dashboard.py
import sqlite3, pandas as pd, plotly.graph_objs as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from dynamics.dynamics_params_test import DB_PATH

DB_PATH = DB_PATH

def load_candles():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM candles ORDER BY timestamp ASC", conn)
    conn.close()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def build_dash_app():
    app = Dash(__name__)
    app.layout = html.Div([
        html.H1("📈 Live BTC/USD OHLC", style={"textAlign": "center"}),
        dcc.Interval(id="interval", interval=60*1000, n_intervals=0),
        dcc.Graph(id="chart")
    ])

    @app.callback(Output("chart", "figure"), Input("interval", "n_intervals"))
    def update(n):
        df = load_candles()
        fig = go.Figure(data=[go.Candlestick(
            x=df["timestamp"],
            open=df["open"], high=df["high"],
            low=df["low"], close=df["close"]
        )])
        fig.update_layout(template="plotly_dark", height=600)
        return fig

    return app
