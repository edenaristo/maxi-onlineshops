###   ██████╗ ██████╗  █████╗ ██████╗     ███╗   ███╗███████╗██████╗  ██████╗██╗  ██╗ █████╗ ███╗   ██╗████████╗
###  ██╔════╝ ██╔══██╗██╔══██╗██╔══██╗    ████╗ ████║██╔════╝██╔══██╗██╔════╝██║  ██║██╔══██╗████╗  ██║╚══██╔══╝
###  ██║  ███╗██████╔╝███████║██████╔╝    ██╔████╔██║█████╗  ██████╔╝██║     ███████║███████║██╔██╗ ██║   ██║
###  ██║   ██║██╔══██╗██╔══██║██╔══██╗    ██║╚██╔╝██║██╔══╝  ██╔══██╗██║     ██╔══██║██╔══██║██║╚██╗██║   ██║
###  ╚██████╔╝██║  ██║██║  ██║██████╔╝    ██║ ╚═╝ ██║███████╗██║  ██║╚██████╗██║  ██║██║  ██║██║ ╚████║   ██║
###   ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝     ╚═╝     ╚═╝╚══════╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝   ╚═╝
###
###  update_grab.py: Updates Grab Merchant price and stock based on AVO system
###
###  input:  csv file from AVO (master + promo),
###          Grab price ZIP (contains the menu csv at its root),
###          Grab stock csv
###  output: same ZIP but with the menu csv prices updated (everything else byte-for-byte kept),
###          same stock csv but with Current Stock updated (everything else kept)
###
###  This code is owned by CV. MAXI RAYA
###  Made for the same pipeline as update_shopee.py / update_tokopedia.py
###
####################################################################################################################################################
####################################################################################################################################################

#===================================================================================================================================================
# IMPORTS
#===================================================================================================================================================
import os
import sys
import io
import csv
import math
import time
import zipfile
import shutil
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
from tabulate import tabulate
import re


#===================================================================================================================================================
# GLOBAL VARIABLES
#===================================================================================================================================================
# --- settings ---
SETTINGS_FEE_PATH = 'SETTINGS/fee.xlsx'
SETTINGS_CONSTANT_PATH = 'SETTINGS/constant.xlsx'

# --- inputs (one file per folder, exactly like shopee / tokopedia) ---
INPUT_AVO_MASTER_PATH = 'INPUT/AVO_MASTER'      # brmaster csv
INPUT_AVO_PROMO_PATH = 'INPUT/AVO_PROMO'        # brjual csv
INPUT_GRAB_PRICE_PATH = 'INPUT/GRAB_PRICE'      # the Grab ZIP (price template)
INPUT_GRAB_STOCK_PATH = 'INPUT/GRAB_STOCK'      # the Grab stock csv

# --- outputs ---
OUTPUT_PATH = 'OUTPUT'
GRAB_PRICE_OUTPUT_FILE_NAME = 'grab_price_output'   # the .zip (this name is free to change)
GRAB_STOCK_OUTPUT_FILE_NAME = 'grab_stock_output'   # the stock .csv (this name is free to change)

# --- logs ---
LOG_PATH = 'LOGS'
LOG_OUTPUT_FILE_NAME = 'grab_logfile'

# --- behaviour toggles ---
INPUT_DELETION = False   # True -> delete input files after processing
OUTPUT_DELETION = True   # True -> wipe OUTPUT folder before writing
ZERO_STOCK_ON_SKU_ERROR = True   # stock file: flagged SKU -> Current Stock = 0 (same safety as shopee/toko).
                                 # set False to leave the old stock untouched on flagged rows.

# --- grab column names (the headers exactly as they appear in the files) ---
MENU_SKU_COL = 'SKUNumber'          # our AVO key inside the menu csv
MENU_PRICE_COL = '*Price'           # the column we change in the menu csv
MENU_CATEGORY_COL = '*GrabCategoryName'   # == sub-department in fee.xlsx
MENU_NAME_COL = '*ItemName'         # only used for nicer logs

STOCK_SKU_COL = 'Item Code'         # our AVO key inside the stock csv
STOCK_VALUE_COL = 'Current Stock'   # the column we change in the stock csv
STOCK_NAME_COL = 'Item Name'        # only used for nicer logs

# --- file layout (VERIFIED against the supplied samples) ---
# menu csv : row 0 = header, row 1 = instruction row, row 2+ = data
MENU_HEADER_ROW = 0
MENU_FIRST_DATA_ROW = 2
# stock csv: row 0 = header, row 1 = instruction row, row 2 = code row, row 3+ = data
STOCK_HEADER_ROW = 0
STOCK_FIRST_DATA_ROW = 3

SKU_REGEX = r'^\d+(x\d+)?$'   # plain SKU, or boxed SKU like 1234x6


#===================================================================================================================================================
# HELPER FUNCTIONS
#===================================================================================================================================================
def format_table(df, table_name):
    return f"\n{table_name}\n" + ("-" * len(table_name)) + "\n" + (tabulate(df, headers="keys", tablefmt="grid")) + "\n"


def find_single_file(folder, extension):
    """Return the path of the one file with the given extension in a folder, or raise."""
    files = [f for f in os.listdir(folder) if f.lower().endswith(extension)]
    if len(files) != 1:
        raise SystemExit(f"{folder} -> Error: There must be exactly 1 '{extension}' file in the folder (found {len(files)}).")
    return os.path.join(folder, files[0])


def detect_line_terminator(raw_bytes):
    """Preserve the original record separator: CRLF if present, else LF."""
    return '\r\n' if b'\r\n' in raw_bytes else '\n'


def split_sku(sku):
    """Split a SKU like '1234x6' into (base='1234', multiplier=6). Plain SKU -> (sku, 1)."""
    if 'x' in sku:
        parts = sku.split('x')
        return parts[0], int(parts[1])
    return sku, 1


def calc_price(avo_row, multiplier, fee):
    """Replicates the shopee / tokopedia price logic (tail tiers + promo discount + markup + fee gross-up)."""
    # choose the tail tier based on the box multiplier
    if multiplier >= avo_row["Tail 2"] and pd.notna(avo_row["Price/pcs 3"]):
        tail = 3
    elif multiplier >= avo_row["Tail 1"] and pd.notna(avo_row["Price/pcs 2"]):
        tail = 2
    else:
        tail = 1

    # base per-piece price, taking the cheaper of promo vs tier when a promo is active
    if pd.notna(avo_row["Discount Price"]):
        if tail == 2:
            avo_price = min(avo_row["Discount Price"], avo_row["Price/pcs 2"])
        elif tail == 3:
            avo_price = min(avo_row["Discount Price"], avo_row["Price/pcs 3"])
        else:
            avo_price = avo_row["Discount Price"]
    elif tail == 2:
        avo_price = avo_row["Price/pcs 2"]
    elif tail == 3:
        avo_price = avo_row["Price/pcs 3"]
    else:
        avo_price = avo_row["Price"]

    # box correction, markup, then gross-up for the marketplace fee
    avo_price = avo_price * multiplier
    return float(math.ceil((avo_price * (1 + MARKUP)) / (1 - fee)))


def calc_stock(avo_row, multiplier):
    """Replicates the shopee / tokopedia stock logic (box correction + safety divisor, never negative)."""
    avo_stock = avo_row["Stock"] // multiplier
    return int(max(avo_stock // STOCK_DIVISOR, 0))


#===================================================================================================================================================
# PATH PROCESSING
#===================================================================================================================================================
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)   # running as an .exe
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))   # running as a .py script


#===================================================================================================================================================
# CONSTANT READING
#===================================================================================================================================================
# NOTE: constant.xlsx was assumed to be the SAME key-value format as shopee/tokopedia
#       (column A = name, column B = value, no header).
constants_df = pd.read_excel(SETTINGS_CONSTANT_PATH, header=None, engine='openpyxl')

STOCK_DIVISOR = constants_df.loc[constants_df[0] == "STOCK_DIVISOR", 1].values[0]
FREE_ONGKIR_FEE = constants_df.loc[constants_df[0] == "FREE_ONGKIR_FEE", 1].values[0]
MARKUP = constants_df.loc[constants_df[0] == "MARKUP", 1].values[0]
# max allowed move from the old price, e.g. 0.4 = the new price is clamped to within -40% .. +40% of the old one
MAX_PRICE_CHANGE = constants_df.loc[constants_df[0] == "MAX_PRICE_CHANGE", 1].values[0]


#===================================================================================================================================================
# AVO MASTER DATA INPUTTING
#===================================================================================================================================================
avo_input_file_path = find_single_file(INPUT_AVO_MASTER_PATH, '.csv')
avo_df = pd.read_csv(avo_input_file_path, header=0, skiprows=[0, 1, 2, 3],
                     dtype={0: str, 5: int, 6: float, 7: float, 8: float})
avo_df.columns = avo_df.columns.str.strip()
avo_df.rename(columns={"Sku": "SKU", "Price 1": "Price", "T.Sku": "TSKU"}, inplace=True)

# convert price 2 / price 3 into per-piece prices
avo_df["Price/pcs 2"] = avo_df["Price 2"] / avo_df["Tail 1"]
avo_df["Price/pcs 3"] = avo_df["Price 3"] / avo_df["Tail 2"]
avo_df["Price/pcs 2"] = avo_df["Price/pcs 2"].replace(0, np.nan)
avo_df["Price/pcs 3"] = avo_df["Price/pcs 3"].replace(0, np.nan)


#===================================================================================================================================================
# AVO PROMO DATA INPUTTING
#===================================================================================================================================================
avo_promo_file_path = find_single_file(INPUT_AVO_PROMO_PATH, '.csv')
avo_promo_df = pd.read_csv(avo_promo_file_path, header=0, skiprows=[0, 1, 2],
                           dtype={1: str, 3: float, 6: float})
avo_promo_df.columns = avo_promo_df.columns.str.strip()
avo_promo_df.rename(columns={"Sku": "SKU", "Price/pcs": "Price Awal", "Price/pcs.1": "Price Akhir",
                             "Awal": "Waktu Awal", "Akhir": "Waktu Akhir"}, inplace=True)

avo_promo_df["Waktu Awal"] = pd.to_datetime(avo_promo_df["Waktu Awal"], format="%m/%d/%Y")
avo_promo_df["Waktu Akhir"] = pd.to_datetime(avo_promo_df["Waktu Akhir"], format="%m/%d/%Y")
avo_promo_df["Waktu Akhir"] = avo_promo_df["Waktu Akhir"].apply(lambda x: x.replace(hour=23, minute=59, second=59))

now = datetime.now()
filtered_promo_df = avo_promo_df[(avo_promo_df["Waktu Awal"] <= now) & (avo_promo_df["Waktu Akhir"] >= now)]
filtered_promo_df = filtered_promo_df[filtered_promo_df["Price Akhir"] != 0]

avo_df = avo_df.merge(filtered_promo_df[['SKU', 'Price Akhir']], on='SKU', how='left')
avo_df.rename(columns={'Price Akhir': 'Discount Price'}, inplace=True)


#===================================================================================================================================================
# FEE SETTINGS DATA INPUTTING
#===================================================================================================================================================
# fee.xlsx columns: department, sub-department, fee
# sub-department is unique across the whole sheet, so we map sub-department -> fee directly.
settings_fee_df = pd.read_excel(SETTINGS_FEE_PATH, header=0, engine='openpyxl',
                                dtype={"department": str, "sub-department": str, "fee": float})
settings_fee_df.drop_duplicates(subset="sub-department", keep='first', inplace=True)

HIGHEST_FEE = settings_fee_df["fee"].max()
FEE_BY_SUBDEPT = settings_fee_df.set_index("sub-department")["fee"].to_dict()


#===================================================================================================================================================
# READ THE GRAB PRICE ZIP  (find the menu csv at the root of the zip)
#===================================================================================================================================================
grab_zip_path = find_single_file(INPUT_GRAB_PRICE_PATH, '.zip')

with zipfile.ZipFile(grab_zip_path, 'r') as zin:
    zip_infos = zin.infolist()
    # the menu csv is the single csv living at the ROOT of the zip (no folder in its path)
    root_csvs = [zi for zi in zip_infos if zi.filename.lower().endswith('.csv') and '/' not in zi.filename.strip('/')]
    if len(root_csvs) != 1:
        raise SystemExit(f"GRAB_PRICE zip -> Error: expected exactly 1 csv at the zip root (found {len(root_csvs)}).")
    menu_info = root_csvs[0]
    menu_raw_bytes = zin.read(menu_info.filename)
    # keep every other entry so we can rebuild the zip untouched
    other_entries = [(zi, zin.read(zi.filename)) for zi in zip_infos if zi.filename != menu_info.filename]

menu_line_term = detect_line_terminator(menu_raw_bytes)
menu_rows = list(csv.reader(io.StringIO(menu_raw_bytes.decode('utf-8'))))

menu_header = menu_rows[MENU_HEADER_ROW]
menu_sku_idx = menu_header.index(MENU_SKU_COL)
menu_price_idx = menu_header.index(MENU_PRICE_COL)
menu_cat_idx = menu_header.index(MENU_CATEGORY_COL)
menu_name_idx = menu_header.index(MENU_NAME_COL) if MENU_NAME_COL in menu_header else None


#===================================================================================================================================================
# READ THE GRAB STOCK CSV
#===================================================================================================================================================
grab_stock_path = find_single_file(INPUT_GRAB_STOCK_PATH, '.csv')
with open(grab_stock_path, 'rb') as f:
    stock_raw_bytes = f.read()

stock_line_term = detect_line_terminator(stock_raw_bytes)
stock_rows = list(csv.reader(io.StringIO(stock_raw_bytes.decode('utf-8'))))

stock_header = stock_rows[STOCK_HEADER_ROW]
stock_sku_idx = stock_header.index(STOCK_SKU_COL)
stock_value_idx = stock_header.index(STOCK_VALUE_COL)
stock_name_idx = stock_header.index(STOCK_NAME_COL) if STOCK_NAME_COL in stock_header else None


#===================================================================================================================================================
# MAIN PROCESSING  ::  PASS 1 - MENU (PRICE)
#===================================================================================================================================================
# log trackers (price side)
p_no_sku_df = pd.DataFrame(columns=["SKU", "Name", "Reason"])
p_format_sku_df = pd.DataFrame(columns=["SKU", "Name", "Reason"])
p_duplicate_sku_df = pd.DataFrame(columns=["SKU", "Name", "Reason"])
p_bad_sku_df = pd.DataFrame(columns=["SKU", "Name", "Reason"])
p_bad_category_df = pd.DataFrame(columns=["SKU", "Name", "Category"])
p_capped_df = pd.DataFrame(columns=["SKU", "Name", "Price_Change"])
p_updated_tracker_df = pd.DataFrame(columns=["SKU", "Name", "Price_Change"])
p_num_stays = 0
p_num_capped = 0

sku_first_occurrence = {}

for i in range(MENU_FIRST_DATA_ROW, len(menu_rows)):
    row = menu_rows[i]
    if len(row) <= max(menu_sku_idx, menu_price_idx, menu_cat_idx):
        continue  # malformed / short line, leave as-is

    raw_sku = (row[menu_sku_idx] or '').strip()
    name = row[menu_name_idx] if menu_name_idx is not None and menu_name_idx < len(row) else ''

    # empty SKU  -> leave price unchanged, just log
    if raw_sku == '' or pd.isna(raw_sku):
        p_no_sku_df.loc[len(p_no_sku_df)] = [raw_sku, name, "empty SKU"]
        continue

    # bad format -> leave price unchanged, just log
    if not re.match(SKU_REGEX, raw_sku):
        p_format_sku_df.loc[len(p_format_sku_df)] = [raw_sku, name, "format error"]
        continue

    # duplicate  -> leave price unchanged, just log
    if raw_sku in sku_first_occurrence:
        p_duplicate_sku_df.loc[len(p_duplicate_sku_df)] = [raw_sku, name, "duplicate SKU"]
        continue
    sku_first_occurrence[raw_sku] = i

    base_sku, multiplier = split_sku(raw_sku)

    matching_row = avo_df[avo_df["SKU"] == base_sku]
    if matching_row.empty or matching_row.iloc[0]["TSKU"] == "D":
        p_bad_sku_df.loc[len(p_bad_sku_df)] = [raw_sku, name, "not found in AVO / discontinued"]
        continue
    avo_row = matching_row.iloc[0]

    # category -> fee  (sub-department string). missing -> highest fee + flag
    category = (row[menu_cat_idx] or '').strip()
    if category in FEE_BY_SUBDEPT:
        cat_fee = FEE_BY_SUBDEPT[category]
    else:
        cat_fee = HIGHEST_FEE
        p_bad_category_df.loc[len(p_bad_category_df)] = [raw_sku, name, category]

    fee = cat_fee + FREE_ONGKIR_FEE
    price_now = calc_price(avo_row, multiplier, fee)

    # read the price currently written in the file
    try:
        old_price = float(row[menu_price_idx])
    except (ValueError, TypeError):
        old_price = None

    # CAP the move: the new price must stay within -MAX_PRICE_CHANGE .. +MAX_PRICE_CHANGE of the old price.
    # floor the upper bound / ceil the lower bound so we never push PAST the allowed band after rounding.
    capped = False
    if old_price is not None and old_price > 0:
        upper_cap = math.floor(old_price * (1 + MAX_PRICE_CHANGE))   # never exceed +X%
        lower_cap = math.ceil(old_price * (1 - MAX_PRICE_CHANGE))    # never drop below -X%
        if price_now > upper_cap:
            price_now = float(upper_cap)
            capped = True
        elif price_now < lower_cap:
            price_now = float(lower_cap)
            capped = True

    old_display = row[menu_price_idx]

    if capped:
        p_num_capped += 1
        p_capped_df.loc[len(p_capped_df)] = [raw_sku, name, f"{old_display} -> {int(price_now)}"]
        print(f"MAXED PRICE - SKU {raw_sku} ({name}) capped at {int(price_now)}. REUPLOAD AGAIN")

    if old_price is None or old_price != price_now:
        # write the new (possibly capped) price back, as an integer (Grab prices are whole numbers)
        row[menu_price_idx] = str(int(price_now))
        note = f"{old_display} -> {int(price_now)}" + (" (CAPPED)" if capped else "")
        p_updated_tracker_df.loc[len(p_updated_tracker_df)] = [raw_sku, name, note]
    else:
        p_num_stays += 1


# loud heads-up if any price hit the cap
if p_num_capped > 0:
    print("=" * 70)
    print(f"  MAXED PRICE: {p_num_capped} item(s) hit the +/-{int(MAX_PRICE_CHANGE * 100)}% cap. REUPLOAD AGAIN")
    print("=" * 70)


#===================================================================================================================================================
# MAIN PROCESSING  ::  PASS 2 - STOCK
#===================================================================================================================================================
# log trackers (stock side)
s_no_sku_df = pd.DataFrame(columns=["SKU", "Name", "Reason"])
s_format_sku_df = pd.DataFrame(columns=["SKU", "Name", "Reason"])
s_duplicate_sku_df = pd.DataFrame(columns=["SKU", "Name", "Reason"])
s_bad_sku_df = pd.DataFrame(columns=["SKU", "Name", "Reason"])
s_updated_tracker_df = pd.DataFrame(columns=["SKU", "Name", "Stock_Change"])
s_num_stays = 0

stock_first_occurrence = {}


def _zero_stock(row):
    if ZERO_STOCK_ON_SKU_ERROR and stock_value_idx < len(row):
        row[stock_value_idx] = "0"


for i in range(STOCK_FIRST_DATA_ROW, len(stock_rows)):
    row = stock_rows[i]
    if len(row) <= max(stock_sku_idx, stock_value_idx):
        continue  # malformed / short line, leave as-is

    raw_sku = (row[stock_sku_idx] or '').strip()
    name = row[stock_name_idx] if stock_name_idx is not None and stock_name_idx < len(row) else ''
    old_display = row[stock_value_idx]

    # empty SKU -> zero stock + log
    if raw_sku == '' or pd.isna(raw_sku):
        _zero_stock(row)
        s_no_sku_df.loc[len(s_no_sku_df)] = [raw_sku, name, "empty SKU"]
        continue

    # bad format -> zero stock + log
    if not re.match(SKU_REGEX, raw_sku):
        _zero_stock(row)
        s_format_sku_df.loc[len(s_format_sku_df)] = [raw_sku, name, "format error"]
        continue

    # duplicate -> zero stock on BOTH this row and the first occurrence + log
    if raw_sku in stock_first_occurrence:
        _zero_stock(row)
        first_i = stock_first_occurrence[raw_sku]
        _zero_stock(stock_rows[first_i])
        s_duplicate_sku_df.loc[len(s_duplicate_sku_df)] = [raw_sku, name, "duplicate SKU"]
        continue
    stock_first_occurrence[raw_sku] = i

    base_sku, multiplier = split_sku(raw_sku)

    matching_row = avo_df[avo_df["SKU"] == base_sku]
    if matching_row.empty or matching_row.iloc[0]["TSKU"] == "D":
        _zero_stock(row)
        s_bad_sku_df.loc[len(s_bad_sku_df)] = [raw_sku, name, "not found in AVO / discontinued"]
        continue
    avo_row = matching_row.iloc[0]

    stock_now = calc_stock(avo_row, multiplier)

    try:
        old_stock = int(float(old_display))
    except (ValueError, TypeError):
        old_stock = None

    if old_stock is None or old_stock != stock_now:
        row[stock_value_idx] = str(stock_now)
        s_updated_tracker_df.loc[len(s_updated_tracker_df)] = [raw_sku, name, f"{old_display} -> {stock_now}"]
    else:
        s_num_stays += 1


#===================================================================================================================================================
# OUTPUT DELETION
#===================================================================================================================================================
if OUTPUT_DELETION:
    for f in os.listdir(OUTPUT_PATH):
        os.remove(os.path.join(OUTPUT_PATH, f))


#===================================================================================================================================================
# WRITE OUTPUTS
#===================================================================================================================================================
timestamp = time.strftime("%d-%m-%Y_%H-%M-%S")

# --- 1) rebuild the menu csv bytes (only Price cells changed; everything else identical) ---
menu_buf = io.StringIO()
menu_writer = csv.writer(menu_buf, lineterminator=menu_line_term, quoting=csv.QUOTE_MINIMAL)
menu_writer.writerows(menu_rows)
new_menu_bytes = menu_buf.getvalue().encode('utf-8')

# --- 2) rebuild the zip: keep the menu csv under its ORIGINAL name, every other entry byte-for-byte ---
zip_filename = f"{GRAB_PRICE_OUTPUT_FILE_NAME}_{timestamp}.zip"
zip_out_path = os.path.join(OUTPUT_PATH, zip_filename)
with zipfile.ZipFile(zip_out_path, 'w', zipfile.ZIP_DEFLATED) as zout:
    # write the (updated) menu csv first, preserving its name + compression type
    zout.writestr(menu_info, new_menu_bytes)
    for zi, data in other_entries:
        zout.writestr(zi, data)

# --- 3) write the stock csv (only Current Stock cells changed; everything else identical) ---
stock_buf = io.StringIO()
stock_writer = csv.writer(stock_buf, lineterminator=stock_line_term, quoting=csv.QUOTE_MINIMAL)
stock_writer.writerows(stock_rows)
new_stock_bytes = stock_buf.getvalue().encode('utf-8')

stock_filename = f"{GRAB_STOCK_OUTPUT_FILE_NAME}_{timestamp}.csv"
stock_out_path = os.path.join(OUTPUT_PATH, stock_filename)
with open(stock_out_path, 'wb') as f:
    f.write(new_stock_bytes)


#===================================================================================================================================================
# LOG OUTPUTTING
#===================================================================================================================================================
log_filename = f"{LOG_OUTPUT_FILE_NAME}_{timestamp}.txt"
log_path = os.path.join(LOG_PATH, log_filename)

log_content = f"""===========================================================
LOG FILE - Grab Merchant Updater
Date: {timestamp}
===========================================================

PRICE (menu) SUMMARY:
{len(p_no_sku_df):7} empty SKU on menu
{len(p_format_sku_df):7} format error SKU on menu
{len(p_duplicate_sku_df):7} duplicate SKU on menu
{len(p_bad_sku_df):7} SKUs on menu not found on AVO system
{len(p_bad_category_df):7} SKU items with a category not found in fee.xlsx (used highest fee)
{len(p_capped_df):7} SKU items whose price change was CAPPED at +/-{int(MAX_PRICE_CHANGE * 100)}%
{len(p_updated_tracker_df):7} SKU items with price updated
{p_num_stays:7} SKU items price stayed

STOCK SUMMARY:
{len(s_no_sku_df):7} empty SKU on stock
{len(s_format_sku_df):7} format error SKU on stock
{len(s_duplicate_sku_df):7} duplicate SKU on stock
{len(s_bad_sku_df):7} SKUs on stock not found on AVO system
{len(s_updated_tracker_df):7} SKU items with stock updated
{s_num_stays:7} SKU items stock stayed

================================================================================================================================
PRICE (menu) TABLES
================================================================================================================================
{format_table(p_no_sku_df, "EMPTY SKU TABLE (menu)")}

{format_table(p_format_sku_df, "FORMAT ERROR SKU TABLE (menu)")}

{format_table(p_duplicate_sku_df, "DUPLICATE SKU TABLE (menu)")}

{format_table(p_bad_sku_df, "BAD SKU TABLE (menu - not found on AVO system)")}

{format_table(p_bad_category_df, "BAD CATEGORY TABLE (menu - category missing in fee.xlsx, used highest fee)")}

{format_table(p_capped_df, "CAPPED PRICE TABLE (menu - change limited to the MAX_PRICE_CHANGE band)")}

{format_table(p_updated_tracker_df, "PRICE UPDATES TABLE")}

================================================================================================================================
STOCK TABLES
================================================================================================================================
{format_table(s_no_sku_df, "EMPTY SKU TABLE (stock)")}

{format_table(s_format_sku_df, "FORMAT ERROR SKU TABLE (stock)")}

{format_table(s_duplicate_sku_df, "DUPLICATE SKU TABLE (stock)")}

{format_table(s_bad_sku_df, "BAD SKU TABLE (stock - not found on AVO system)")}

{format_table(s_updated_tracker_df, "STOCK UPDATES TABLE")}

================================================================================================================================
End of log file
"""

with open(log_path, "w", encoding="utf-8") as file:
    file.write(log_content)


#===================================================================================================================================================
# INPUT DELETION
#===================================================================================================================================================
if INPUT_DELETION:
    os.remove(grab_zip_path)
    os.remove(grab_stock_path)
    os.remove(avo_input_file_path)
    os.remove(avo_promo_file_path)


####################################################################################################################################################
####################################################################################################################################################
# end of code