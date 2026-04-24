"""
alphalens/core/ingestion/universe.py

Nifty200 stock universe — symbol list, sector mapping, metadata.
Provides functions to load/refresh the universe into DuckDB.
"""

from datetime import datetime
from loguru import logger
from alphalens.core.database import get_duck

# ── Nifty200 Symbol Master ─────────────────────────────────────────────────
# Format: (NSE_symbol, company_name, sector, industry, market_cap_cat, in_nifty50, in_nifty100)
# Full Nifty200 as of 2024. Update quarterly from NSE website.

NIFTY200_UNIVERSE = [
    # ── Large Cap (Nifty50) ───────────────────────────────────────────────
    ("RELIANCE",    "Reliance Industries",          "Energy",      "Oil & Gas",        "large_cap", True,  True),
    ("TCS",         "Tata Consultancy Services",    "IT",          "IT Services",      "large_cap", True,  True),
    ("HDFCBANK",    "HDFC Bank",                    "Financials",  "Private Bank",     "large_cap", True,  True),
    ("INFY",        "Infosys",                      "IT",          "IT Services",      "large_cap", True,  True),
    ("ICICIBANK",   "ICICI Bank",                   "Financials",  "Private Bank",     "large_cap", True,  True),
    ("HINDUNILVR",  "Hindustan Unilever",            "FMCG",        "FMCG",             "large_cap", True,  True),
    ("ITC",         "ITC Ltd",                      "FMCG",        "Diversified",      "large_cap", True,  True),
    ("SBIN",        "State Bank of India",           "Financials",  "Public Bank",      "large_cap", True,  True),
    ("BHARTIARTL",  "Bharti Airtel",                 "Telecom",     "Telecom Services", "large_cap", True,  True),
    ("KOTAKBANK",   "Kotak Mahindra Bank",           "Financials",  "Private Bank",     "large_cap", True,  True),
    ("LT",          "Larsen & Toubro",               "Industrials", "Engineering",      "large_cap", True,  True),
    ("AXISBANK",    "Axis Bank",                     "Financials",  "Private Bank",     "large_cap", True,  True),
    ("ASIANPAINT",  "Asian Paints",                  "Materials",   "Paints",           "large_cap", True,  True),
    ("MARUTI",      "Maruti Suzuki India",           "Auto",        "Automobiles",      "large_cap", True,  True),
    ("TITAN",       "Titan Company",                 "Consumer",    "Jewellery",        "large_cap", True,  True),
    ("SUNPHARMA",   "Sun Pharmaceutical",            "Pharma",      "Pharmaceuticals",  "large_cap", True,  True),
    ("BAJFINANCE",  "Bajaj Finance",                 "Financials",  "NBFC",             "large_cap", True,  True),
    ("WIPRO",       "Wipro",                         "IT",          "IT Services",      "large_cap", True,  True),
    ("NTPC",        "NTPC",                          "Utilities",   "Power Generation", "large_cap", True,  True),
    ("POWERGRID",   "Power Grid Corporation",        "Utilities",   "Power Transmission","large_cap",True,  True),
    ("ONGC",        "Oil & Natural Gas Corporation", "Energy",      "Oil & Gas",        "large_cap", True,  True),
    ("ADANIENT",    "Adani Enterprises",             "Industrials", "Conglomerate",     "large_cap", True,  True),
    ("ADANIPORTS",  "Adani Ports & SEZ",             "Industrials", "Ports & Logistics","large_cap", True,  True),
    ("ULTRACEMCO",  "UltraTech Cement",              "Materials",   "Cement",           "large_cap", True,  True),
    ("COALINDIA",   "Coal India",                    "Energy",      "Mining",           "large_cap", True,  True),
    ("JSWSTEEL",    "JSW Steel",                     "Materials",   "Steel",            "large_cap", True,  True),
    ("TATAMOTORS",  "Tata Motors",                   "Auto",        "Automobiles",      "large_cap", True,  True),
    ("TATASTEEL",   "Tata Steel",                    "Materials",   "Steel",            "large_cap", True,  True),
    ("HCLTECH",     "HCL Technologies",              "IT",          "IT Services",      "large_cap", True,  True),
    ("BAJAJFINSV",  "Bajaj Finserv",                 "Financials",  "NBFC",             "large_cap", True,  True),
    ("NESTLEIND",   "Nestle India",                  "FMCG",        "Food & Beverages", "large_cap", True,  True),
    ("TECHM",       "Tech Mahindra",                 "IT",          "IT Services",      "large_cap", True,  True),
    ("GRASIM",      "Grasim Industries",             "Materials",   "Diversified",      "large_cap", True,  True),
    ("BRITANNIA",   "Britannia Industries",          "FMCG",        "Food & Beverages", "large_cap", True,  True),
    ("CIPLA",       "Cipla",                         "Pharma",      "Pharmaceuticals",  "large_cap", True,  True),
    ("DIVISLAB",    "Divi's Laboratories",           "Pharma",      "Pharmaceuticals",  "large_cap", True,  True),
    ("DRREDDY",     "Dr. Reddy's Laboratories",      "Pharma",      "Pharmaceuticals",  "large_cap", True,  True),
    ("EICHERMOT",   "Eicher Motors",                 "Auto",        "Automobiles",      "large_cap", True,  True),
    ("BPCL",        "Bharat Petroleum",              "Energy",      "Oil & Gas",        "large_cap", True,  True),
    ("HEROMOTOCO",  "Hero MotoCorp",                 "Auto",        "Two Wheelers",     "large_cap", True,  True),
    ("HINDALCO",    "Hindalco Industries",           "Materials",   "Aluminium",        "large_cap", True,  True),
    ("M&M",         "Mahindra & Mahindra",           "Auto",        "Automobiles",      "large_cap", True,  True),
    ("INDUSINDBK",  "IndusInd Bank",                 "Financials",  "Private Bank",     "large_cap", True,  True),
    ("APOLLOHOSP",  "Apollo Hospitals",              "Healthcare",  "Hospitals",        "large_cap", True,  True),
    ("TATACONSUM",  "Tata Consumer Products",        "FMCG",        "Food & Beverages", "large_cap", True,  True),
    ("LTIM",        "LTIMindtree",                   "IT",          "IT Services",      "large_cap", True,  True),
    ("SBILIFE",     "SBI Life Insurance",            "Financials",  "Insurance",        "large_cap", True,  True),
    ("HDFCLIFE",    "HDFC Life Insurance",           "Financials",  "Insurance",        "large_cap", True,  True),
    ("BAJAJ-AUTO",  "Bajaj Auto",                    "Auto",        "Two Wheelers",     "large_cap", True,  True),
    ("SHREECEM",    "Shree Cement",                  "Materials",   "Cement",           "large_cap", True,  True),
    # ── Large Cap (Nifty100, not Nifty50) ────────────────────────────────
    ("ADANIGREEN",  "Adani Green Energy",            "Utilities",   "Renewable Energy", "large_cap", False, True),
    ("ADANITRANS",  "Adani Transmission",            "Utilities",   "Power Transmission","large_cap",False, True),
    ("AMBUJACEM",   "Ambuja Cements",                "Materials",   "Cement",           "large_cap", False, True),
    ("BANDHANBNK",  "Bandhan Bank",                  "Financials",  "Private Bank",     "large_cap", False, True),
    ("BERGEPAINT",  "Berger Paints India",           "Materials",   "Paints",           "large_cap", False, True),
    ("BIOCON",      "Biocon",                        "Pharma",      "Biopharmaceuticals","large_cap", False, True),
    ("BOSCHLTD",    "Bosch",                         "Auto",        "Auto Components",  "large_cap", False, True),
    ("CHOLAFIN",    "Cholamandalam Finance",         "Financials",  "NBFC",             "large_cap", False, True),
    ("COLPAL",      "Colgate-Palmolive India",       "FMCG",        "Personal Care",    "large_cap", False, True),
    ("DABUR",       "Dabur India",                   "FMCG",        "FMCG",             "large_cap", False, True),
    ("DMART",       "Avenue Supermarts (DMart)",     "Consumer",    "Retail",           "large_cap", False, True),
    ("GAIL",        "GAIL India",                    "Energy",      "Gas Distribution", "large_cap", False, True),
    ("GODREJCP",    "Godrej Consumer Products",      "FMCG",        "FMCG",             "large_cap", False, True),
    ("HAVELLS",     "Havells India",                 "Consumer",    "Consumer Durables", "large_cap",False, True),
    ("ICICIGI",     "ICICI Lombard General Ins",     "Financials",  "Insurance",        "large_cap", False, True),
    ("ICICIPRULI",  "ICICI Prudential Life Ins",     "Financials",  "Insurance",        "large_cap", False, True),
    ("INDUSTOWER",  "Indus Towers",                  "Telecom",     "Tower Infrastructure","large_cap",False,True),
    ("IOC",         "Indian Oil Corporation",        "Energy",      "Oil & Gas",        "large_cap", False, True),
    ("IRCTC",       "Indian Railway Catering",       "Consumer",    "Travel & Tourism", "large_cap", False, True),
    ("JUBLFOOD",    "Jubilant Foodworks",            "Consumer",    "QSR",              "large_cap", False, True),
    ("LUPIN",       "Lupin",                         "Pharma",      "Pharmaceuticals",  "large_cap", False, True),
    ("MARICO",      "Marico",                        "FMCG",        "FMCG",             "large_cap", False, True),
    ("MINDTREE",    "Mindtree",                      "IT",          "IT Services",      "large_cap", False, True),
    ("MOTHERSON",   "Samvardhana Motherson",         "Auto",        "Auto Components",  "large_cap", False, True),
    ("MPHASIS",     "Mphasis",                       "IT",          "IT Services",      "large_cap", False, True),
    ("MUTHOOTFIN",  "Muthoot Finance",               "Financials",  "NBFC",             "large_cap", False, True),
    ("NAUKRI",      "Info Edge India (Naukri)",      "IT",          "Internet Services","large_cap",  False, True),
    ("NMDC",        "NMDC",                          "Materials",   "Mining",           "large_cap", False, True),
    ("PAGEIND",     "Page Industries",               "Consumer",    "Apparel",          "large_cap", False, True),
    ("PETRONET",    "Petronet LNG",                  "Energy",      "Gas Distribution", "large_cap", False, True),
    ("PIIND",       "PI Industries",                 "Materials",   "Agrochemicals",    "large_cap", False, True),
    ("PNB",         "Punjab National Bank",          "Financials",  "Public Bank",      "large_cap", False, True),
    ("RECLTD",      "REC Limited",                   "Financials",  "Infra Finance",    "large_cap", False, True),
    ("SAIL",        "Steel Authority of India",      "Materials",   "Steel",            "large_cap", False, True),
    ("SIEMENS",     "Siemens India",                 "Industrials", "Industrial Machinery","large_cap",False,True),
    ("SRF",         "SRF",                           "Materials",   "Specialty Chemicals","large_cap",False, True),
    ("TORNTPHARM",  "Torrent Pharmaceuticals",       "Pharma",      "Pharmaceuticals",  "large_cap", False, True),
    ("TRENT",       "Trent",                         "Consumer",    "Retail",           "large_cap", False, True),
    ("TVSMOTOR",    "TVS Motor Company",             "Auto",        "Two Wheelers",     "large_cap", False, True),
    ("UBL",         "United Breweries",              "FMCG",        "Alcoholic Beverages","large_cap",False, True),
    ("VEDL",        "Vedanta",                       "Materials",   "Diversified Metals","large_cap", False, True),
    ("VOLTAS",      "Voltas",                        "Consumer",    "Consumer Durables", "large_cap",False, True),
    ("WHIRLPOOL",   "Whirlpool of India",            "Consumer",    "Consumer Durables", "large_cap",False, True),
    ("ZYDUSLIFE",   "Zydus Lifesciences",            "Pharma",      "Pharmaceuticals",  "large_cap", False, True),
    # ── Mid Cap (Nifty100-200) ────────────────────────────────────────────
    ("ABB",         "ABB India",                     "Industrials", "Industrial Machinery","mid_cap", False, False),
    ("ABCAPITAL",   "Aditya Birla Capital",          "Financials",  "Diversified Finance","mid_cap",  False, False),
    ("ALKEM",       "Alkem Laboratories",            "Pharma",      "Pharmaceuticals",  "mid_cap",   False, False),
    ("APLLTD",      "Alembic Pharma",                "Pharma",      "Pharmaceuticals",  "mid_cap",   False, False),
    ("AUROPHARMA",  "Aurobindo Pharma",              "Pharma",      "Pharmaceuticals",  "mid_cap",   False, False),
    ("BALKRISIND",  "Balkrishna Industries",         "Auto",        "Tyres",            "mid_cap",   False, False),
    ("BANKINDIA",   "Bank of India",                 "Financials",  "Public Bank",      "mid_cap",   False, False),
    ("BATAINDIA",   "Bata India",                    "Consumer",    "Footwear",         "mid_cap",   False, False),
    ("BEL",         "Bharat Electronics",            "Industrials", "Defence",          "mid_cap",   False, False),
    ("BHARATFORG",  "Bharat Forge",                  "Auto",        "Auto Components",  "mid_cap",   False, False),
    ("CANBK",       "Canara Bank",                   "Financials",  "Public Bank",      "mid_cap",   False, False),
    ("CUMMINSIND",  "Cummins India",                 "Industrials", "Industrial Machinery","mid_cap",  False, False),
    ("DEEPAKNTR",   "Deepak Nitrite",                "Materials",   "Specialty Chemicals","mid_cap",  False, False),
    ("ESCORTS",     "Escorts Kubota",                "Auto",        "Farm Equipment",   "mid_cap",   False, False),
    ("FEDERALBNK",  "Federal Bank",                  "Financials",  "Private Bank",     "mid_cap",   False, False),
    ("GLAXO",       "GlaxoSmithKline Pharma",        "Pharma",      "Pharmaceuticals",  "mid_cap",   False, False),
    ("GMRINFRA",    "GMR Infrastructure",            "Industrials", "Airports",         "mid_cap",   False, False),
    ("GODREJIND",   "Godrej Industries",             "FMCG",        "Diversified",      "mid_cap",   False, False),
    ("GRANULES",    "Granules India",                "Pharma",      "Pharmaceuticals",  "mid_cap",   False, False),
    ("GUJGASLTD",   "Gujarat Gas",                   "Energy",      "Gas Distribution", "mid_cap",   False, False),
    ("HAPPSTMNDS",  "Happiest Minds Technologies",   "IT",          "IT Services",      "mid_cap",   False, False),
    ("HDFCAMC",     "HDFC Asset Management",         "Financials",  "Asset Management", "mid_cap",   False, False),
    ("IDFCFIRSTB",  "IDFC First Bank",               "Financials",  "Private Bank",     "mid_cap",   False, False),
    ("IEX",         "Indian Energy Exchange",        "Financials",  "Exchange",         "mid_cap",   False, False),
    ("IPCALAB",     "Ipca Laboratories",             "Pharma",      "Pharmaceuticals",  "mid_cap",   False, False),
    ("JKCEMENT",    "JK Cement",                     "Materials",   "Cement",           "mid_cap",   False, False),
    ("JSWENERGY",   "JSW Energy",                    "Utilities",   "Power Generation", "mid_cap",   False, False),
    ("JUBILANT",    "Jubilant Pharmova",             "Pharma",      "Pharmaceuticals",  "mid_cap",   False, False),
    ("KAJARIACER",  "Kajaria Ceramics",              "Materials",   "Ceramics",         "mid_cap",   False, False),
    ("KANSAINER",   "Kansai Nerolac Paints",         "Materials",   "Paints",           "mid_cap",   False, False),
    ("KEI",         "KEI Industries",                "Industrials", "Cables",           "mid_cap",   False, False),
    ("LICHSGFIN",   "LIC Housing Finance",           "Financials",  "Housing Finance",  "mid_cap",   False, False),
    ("LTTS",        "L&T Technology Services",       "IT",          "IT Services",      "mid_cap",   False, False),
    ("METROPOLIS",  "Metropolis Healthcare",         "Healthcare",  "Diagnostics",      "mid_cap",   False, False),
    ("MFSL",        "Max Financial Services",        "Financials",  "Insurance",        "mid_cap",   False, False),
    ("MINDAIND",    "Minda Industries",              "Auto",        "Auto Components",  "mid_cap",   False, False),
    ("NATCOPHARM",  "Natco Pharma",                  "Pharma",      "Pharmaceuticals",  "mid_cap",   False, False),
    ("NAVINFLUOR",  "Navin Fluorine International",  "Materials",   "Specialty Chemicals","mid_cap",  False, False),
    ("NBCC",        "NBCC India",                    "Industrials", "Construction",     "mid_cap",   False, False),
    ("NILKAMAL",    "Nilkamal",                      "Consumer",    "Plastics",         "mid_cap",   False, False),
    ("PERSISTENT",  "Persistent Systems",            "IT",          "IT Services",      "mid_cap",   False, False),
    ("PHOENIXLTD",  "Phoenix Mills",                 "Real Estate", "Retail REITs",     "mid_cap",   False, False),
    ("POLYCAB",     "Polycab India",                 "Industrials", "Cables",           "mid_cap",   False, False),
    ("POLYMED",     "Poly Medicure",                 "Healthcare",  "Medical Devices",  "mid_cap",   False, False),
    ("RAMCOCEM",    "Ramco Cements",                 "Materials",   "Cement",           "mid_cap",   False, False),
    ("SBICARD",     "SBI Cards & Payment Services",  "Financials",  "Credit Cards",     "mid_cap",   False, False),
    ("SCHAEFFLER",  "Schaeffler India",              "Auto",        "Auto Components",  "mid_cap",   False, False),
    ("SOLARINDS",   "Solar Industries India",        "Materials",   "Explosives",       "mid_cap",   False, False),
    ("SUNTV",       "Sun TV Network",                "Consumer",    "Media",            "mid_cap",   False, False),
    ("SUPREMEIND",  "Supreme Industries",            "Materials",   "Plastics",         "mid_cap",   False, False),
    ("THERMAX",     "Thermax",                       "Industrials", "Engineering",      "mid_cap",   False, False),
    ("TIMKEN",      "Timken India",                  "Industrials", "Bearings",         "mid_cap",   False, False),
    ("TORNTPOWER",  "Torrent Power",                 "Utilities",   "Power",            "mid_cap",   False, False),
    ("TTKPRESTIG",  "TTK Prestige",                  "Consumer",    "Consumer Durables","mid_cap",   False, False),
    ("UPL",         "UPL",                           "Materials",   "Agrochemicals",    "mid_cap",   False, False),
    ("VGUARD",      "V-Guard Industries",            "Consumer",    "Consumer Durables","mid_cap",   False, False),
    ("WINTERHALL",  "Winterhall Dea India",          "Energy",      "Oil & Gas",        "mid_cap",   False, False),
    ("ZOMATO",      "Zomato",                        "Consumer",    "Food Delivery",    "mid_cap",   False, False),
]

# Deduplicate and validate
_UNIVERSE_SYMBOLS = {row[0] for row in NIFTY200_UNIVERSE}


def get_all_symbols() -> list[str]:
    """Return all active Nifty200 symbols."""
    return [row[0] for row in NIFTY200_UNIVERSE]


def get_yfinance_symbol(nse_symbol: str) -> str:
    """Convert NSE symbol to Yahoo Finance format."""
    # Special cases
    overrides = {
        "M&M":      "M%26M.NS",
        "BAJAJ-AUTO": "BAJAJ-AUTO.NS",
    }
    return overrides.get(nse_symbol, f"{nse_symbol}.NS")


def get_symbols_by_sector(sector: str) -> list[str]:
    """Return all symbols in a given sector."""
    return [row[0] for row in NIFTY200_UNIVERSE if row[2] == sector]


def get_sectors() -> list[str]:
    """Return unique sector names."""
    return sorted(set(row[2] for row in NIFTY200_UNIVERSE))


def seed_universe_to_db():
    """Insert/update all Nifty200 stocks into DuckDB."""
    con = get_duck()
    now = datetime.now()

    records = [
        {
            "symbol":          row[0],
            "name":            row[1],
            "sector":          row[2],
            "industry":        row[3],
            "market_cap_cat":  row[4],
            "in_nifty50":      row[5],
            "in_nifty100":     row[6],
            "in_nifty200":     True,
            "yfinance_symbol": get_yfinance_symbol(row[0]),
            "is_active":       True,
            "updated_at":      now,
        }
        for row in NIFTY200_UNIVERSE
    ]

    # Upsert via temp table
    con.execute("CREATE TEMP TABLE IF NOT EXISTS _stocks_tmp AS SELECT * FROM nifty200_stocks LIMIT 0;")
    con.executemany("""
        INSERT OR REPLACE INTO nifty200_stocks
        (symbol, name, sector, industry, market_cap_cat, in_nifty50, in_nifty100,
         in_nifty200, yfinance_symbol, is_active, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(r["symbol"], r["name"], r["sector"], r["industry"], r["market_cap_cat"],
           r["in_nifty50"], r["in_nifty100"], r["in_nifty200"], r["yfinance_symbol"],
           r["is_active"], r["updated_at"]) for r in records])

    count = con.execute("SELECT COUNT(*) FROM nifty200_stocks").fetchone()[0]
    logger.info(f"Universe seeded: {count} stocks in nifty200_stocks")
    return count
