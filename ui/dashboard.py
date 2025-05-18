# ui/dashboard.py

import duckdb
import pandas as pd
import plotly.graph_objs as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output

#from dynamics.dynamic_params import FILEX

#DB_PATH = f"test_{FILEX}.duckdb"

DB_PATH = "data/test_5_min.duckdb"

def load_candles():
    con = duckdb.connect(DB_PATH)
    df = con.execute("SELECT * FROM candles ORDER BY timestamp ASC").fetchdf()
    con.close()
    return df

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
    df = load_candles()
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

if __name__ == "__main__":
    app.run(debug=True, port=8050)
