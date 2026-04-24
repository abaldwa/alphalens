"""alphalens/dashboard/pages/portfolio_entry.py — Page 7: Portfolio Entry"""
import base64, io
import dash, dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html
dash.register_page(__name__, path="/entry", name="Entry", order=7)
from alphalens.dashboard.components.shared import *
from alphalens.core.ingestion.universe import get_all_symbols

SYMBOLS = get_all_symbols()
TF_OPTIONS = [{"label": l, "value": v} for l, v in [
    ("Intraday", "intraday"), ("Swing", "swing"),
    ("Medium-term", "medium"), ("Long-term Investment", "long_term")]]

layout = page_layout([
    section_header("Portfolio Entry", "Manual entry · Zerodha Holdings CSV · Zerodha Tradebook CSV"),
    dbc.Row([
        # ── Manual Entry ──────────────────────────────────────────────
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Manual Stock Entry",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase"}),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col(dcc.Dropdown(id="me-symbol", options=[{"label": s, "value": s} for s in SYMBOLS],
                                             placeholder="Symbol…",
                                             style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                                                    "fontSize": "0.83rem"}), width=6),
                        dbc.Col(dcc.Dropdown(id="me-tf", options=TF_OPTIONS, value="swing",
                                             clearable=False,
                                             style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                                                    "fontSize": "0.83rem"}), width=6),
                    ], className="mb-2"),
                    dbc.Row([
                        dbc.Col(dbc.Input(id="me-qty", type="number", placeholder="Quantity",
                                          style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb", "fontSize": "0.83rem"}), width=6),
                        dbc.Col(dbc.Input(id="me-price", type="number", placeholder="Avg cost (₹)",
                                          style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb", "fontSize": "0.83rem"}), width=6),
                    ], className="mb-2"),
                    dbc.Row([
                        dbc.Col(dbc.Input(id="me-target", type="number", placeholder="Target price (₹)",
                                          style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb", "fontSize": "0.83rem"}), width=6),
                        dbc.Col(dbc.Input(id="me-sl", type="number", placeholder="Stop loss (₹)",
                                          style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb", "fontSize": "0.83rem"}), width=6),
                    ], className="mb-3"),
                    dbc.Button("Add to Portfolio", id="me-submit", n_clicks=0,
                               color="warning", style={"width": "100%", "fontWeight": "700"}),
                    html.Div(id="me-result", style={"marginTop": "8px"}),
                ], style={"padding": "16px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=5),

        # ── Zerodha CSV Upload ────────────────────────────────────────
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Zerodha CSV Upload",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase"}),
                dbc.CardBody([
                    dbc.Tabs([
                        dbc.Tab(label="Holdings", tab_id="holdings-tab"),
                        dbc.Tab(label="Tradebook", tab_id="tradebook-tab"),
                    ], id="csv-tabs", active_tab="holdings-tab", style={"marginBottom": "12px"}),

                    dcc.Dropdown(id="csv-tf", options=TF_OPTIONS, value="long_term",
                                 clearable=False,
                                 style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                                        "fontSize": "0.83rem", "marginBottom": "12px"}),

                    dcc.Upload(
                        id="csv-upload",
                        children=html.Div([
                            html.Div("📂", style={"fontSize": "2rem", "marginBottom": "4px"}),
                            html.Div("Drop Zerodha CSV here or click to browse",
                                     style={"fontSize": "0.82rem", "color": "#9ca3af"}),
                            html.Div("Holdings: Instrument, Qty, Avg cost, LTP …",
                                     style={"fontSize": "0.7rem", "color": "#6b7280", "marginTop": "4px"}),
                        ]),
                        style={
                            "width": "100%", "height": "120px",
                            "borderWidth": "2px", "borderStyle": "dashed",
                            "borderColor": BORDER_COLOR, "borderRadius": "8px",
                            "textAlign": "center", "display": "flex",
                            "alignItems": "center", "justifyContent": "center",
                            "cursor": "pointer", "background": "#0d0d0d",
                        },
                        accept=".csv",
                    ),
                    html.Div(id="csv-preview", style={"marginTop": "12px"}),
                    dbc.Button("Import", id="csv-import", n_clicks=0, disabled=True,
                               color="warning", style={"width": "100%", "marginTop": "8px"}),
                    html.Div(id="csv-result", style={"marginTop": "8px"}),
                    dcc.Store(id="csv-content-store"),
                ], style={"padding": "16px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=7),
    ]),
])

@callback(Output("me-result", "children"),
          Input("me-submit", "n_clicks"),
          State("me-symbol", "value"), State("me-tf", "value"),
          State("me-qty", "value"),    State("me-price", "value"),
          State("me-target", "value"), State("me-sl", "value"),
          prevent_initial_call=True)
def add_manual_entry(n, symbol, tf, qty, price, target, sl):
    if not all([symbol, tf, qty, price]):
        return dbc.Alert("Please fill Symbol, Timeframe, Quantity and Avg Cost.", color="danger", style={"fontSize": "0.82rem"})
    from alphalens.core.portfolio.manager import PortfolioManager
    pm = PortfolioManager()
    if not pm.can_add(tf):
        cap = pm.get_capacity(tf)
        return dbc.Alert(f"Portfolio full for {tf} ({cap['used']}/{cap['max']} slots). Exit a position first.", color="warning", style={"fontSize": "0.82rem"})
    hid = pm.open_position(symbol, tf, int(qty), float(price),
                            float(target) if target else None,
                            float(sl) if sl else None)
    return dbc.Alert(f"✓ Added {symbol} [{tf}] qty={qty} @₹{float(price):,.2f} (ID #{hid})", color="success", style={"fontSize": "0.82rem"})

@callback(Output("csv-preview", "children"), Output("csv-content-store", "data"),
          Output("csv-import", "disabled"),
          Input("csv-upload", "contents"),
          State("csv-upload", "filename"), State("csv-tabs", "active_tab"),
          prevent_initial_call=True)
def preview_csv(contents, filename, active_tab):
    if not contents:
        return html.Div(), None, True
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)
    from alphalens.core.ingestion.zerodha_import import ZerodhaImporter
    imp = ZerodhaImporter()
    if active_tab == "holdings-tab":
        result = imp.validate_holdings_csv(decoded)
    else:
        result = imp.validate_tradebook_csv(decoded)

    if not result.get("valid"):
        return dbc.Alert(f"Invalid CSV: {result.get('missing_cols', result.get('error', 'Unknown error'))}",
                         color="danger", style={"fontSize": "0.82rem"}), None, True
    preview_rows = result.get("preview", [])
    import pandas as pd
    preview_table = dbc.Alert([
        html.Div(f"✓ Valid · {result['row_count']} rows · {filename}", style={"fontWeight": "700", "marginBottom": "8px"}),
        html.Div(str(list(preview_rows[0].keys())[:6]) if preview_rows else "",
                 style={"fontSize": "0.72rem", "color": "#9ca3af"}),
    ], color="success", style={"fontSize": "0.82rem"})
    return preview_table, content_string, False

@callback(Output("csv-result", "children"),
          Input("csv-import", "n_clicks"),
          State("csv-content-store", "data"), State("csv-tabs", "active_tab"),
          State("csv-tf", "value"),
          prevent_initial_call=True)
def import_csv(n, content_b64, active_tab, tf):
    if not content_b64:
        return dbc.Alert("No file loaded.", color="danger", style={"fontSize": "0.82rem"})
    import base64
    decoded = base64.b64decode(content_b64)
    from alphalens.core.ingestion.zerodha_import import ZerodhaImporter
    imp = ZerodhaImporter()
    if active_tab == "holdings-tab":
        result = imp.import_holdings_bytes(decoded, timeframe=tf)
        msg = f"✓ Holdings imported: {result.get('imported', 0)} positions added"
    else:
        result = imp.import_tradebook_bytes(decoded, default_timeframe=tf)
        msg = f"✓ Tradebook imported: {result.get('closed_trades', 0)} trades, {result.get('open_positions', 0)} open positions"
    if "error" in result:
        return dbc.Alert(f"Error: {result['error']}", color="danger", style={"fontSize": "0.82rem"})
    return dbc.Alert(msg, color="success", style={"fontSize": "0.82rem"})
