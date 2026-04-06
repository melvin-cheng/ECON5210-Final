# import libraries
import pandas as pd
import numpy as np
from pathlib import Path

# global variables
_HERE = Path(__file__).resolve().parent
_DATA = _HERE.parent / "data"  # ../data  from retrieve/

EXCEL_PATH = _DATA / "newYieldData.xlsx"


# CURRENTLY HAVE REMOVED HK & KOREA DUE TO SPARSE DATA

_CSV_TEMPLATES = {
    "US": _DATA / "US{}.csv",
    # "KOREA": _DATA / "SouthKorea{}.csv",
    "BRAZIL": _DATA / "Brazil{}.csv",
    "MEXICO": _DATA / "Mexico{}.csv",
    # "HK": _DATA / "HK{}.csv",
    "INDIA": _DATA / "India{}.csv",
    "JAPAN": _DATA / "Japan{}.csv",
    "SWITZ": _DATA / "Switzerland{}.csv",
}

# columns in the Excel file (in order)
_EXCEL_COLS = ["US", "UK", "FRA", "GER", "AUS", "INDO", "KOREA", "BRAZIL", "MEXICO"]

# columns to drop
_DROP = ["HK", "KOREA"]

# certain tenors, some Excel columns are bad — pull from CSV instead
_REPLACE_MAP = {
    1: ["MEXICO"],
    2: ["US"],
    3: ["BRAZIL"],
    5: ["BRAZIL", "MEXICO"],
}

# countries that always come from CSV (never in the Excel file)
_ALWAYS_FROM_CSV = ["INDIA", "JAPAN", "SWITZ"]


# helper functions
def _read_excel(term: int):
    sheet = f"{term} year"
    df = pd.read_excel(EXCEL_PATH, sheet_name=sheet,
                       skiprows=5,        # skip BBG metadata
                       index_col=0)       # column 0 = "Dates"
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[df.index.notna()]
    df.columns = _EXCEL_COLS
    df = df.apply(pd.to_numeric, errors="coerce")
    return df


def _read_csv(country: str, term: int):
    template = _CSV_TEMPLATES[country]
    path = Path(str(template).format(term))

    if not path.exists():
        raise FileNotFoundError(f"Missing CSV: {path}")

    df = pd.read_csv(path, index_col=0)
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[df.index.notna()]

    series = pd.to_numeric(df.iloc[:, 0], errors="coerce")
    series.name = country
    return series


# get_yield API
def get_yield(term: int, freq=None):

    if term not in _REPLACE_MAP:
        raise ValueError(f"Unsupported tenor: {term}. Choose from {list(_REPLACE_MAP)}")

    # Excel base
    df = _read_excel(term)
    cols_to_replace = _REPLACE_MAP[term]
    df = df.drop(columns=cols_to_replace, errors="ignore")

    # CSV supplements
    csv_countries = _ALWAYS_FROM_CSV + cols_to_replace
    csv_series = [_read_csv(c, term) for c in csv_countries]
    csv_df = pd.concat(csv_series, axis=1)

    # merge and clean
    df = pd.concat([df, csv_df], axis=1)
    df = df.drop(columns=_DROP, errors="ignore")
    df = df.sort_index()
    df = df.apply(pd.to_numeric, errors="coerce")
    df = df.ffill()
    df = df.dropna()

    # optional resample
    if freq is not None:
        df = df.resample(freq).last().dropna()

    return df
