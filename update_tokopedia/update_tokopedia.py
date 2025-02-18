###   ___  ___  ________  ________  ________  _________  _______  _________  ________  ___  __    ________  ________  _______   ________  ___  ________      ________  ___    ___ 
###  |\  \|\  \|\   __  \|\   ___ \|\   __  \|\___   ___\\  ___ \|\___   ___\\   __  \|\  \|\  \ |\   __  \|\   __  \|\  ___ \ |\   ___ \|\  \|\   __  \    |\   __  \|\  \  /  /|
###  \ \  \\\  \ \  \|\  \ \  \_|\ \ \  \|\  \|___ \  \_\ \   __/\|___ \  \_\ \  \|\  \ \  \/  /|\ \  \|\  \ \  \|\  \ \   __/|\ \  \_|\ \ \  \ \  \|\  \   \ \  \|\  \ \  \/  / /
###   \ \  \\\  \ \   ____\ \  \ \\ \ \   __  \   \ \  \ \ \  \_|/__  \ \  \ \ \  \\\  \ \   ___  \ \  \\\  \ \   ____\ \  \_|/_\ \  \ \\ \ \  \ \   __  \   \ \   ____\ \    / / 
###    \ \  \\\  \ \  \___|\ \  \_\\ \ \  \ \  \   \ \  \ \ \  \_|\ \  \ \  \ \ \  \\\  \ \  \\ \  \ \  \\\  \ \  \___|\ \  \_|\ \ \  \_\\ \ \  \ \  \ \  \ __\ \  \___|\/  /  /  
###     \ \_______\ \__\    \ \_______\ \__\ \__\   \ \__\ \ \_______\  \ \__\ \ \_______\ \__\\ \__\ \_______\ \__\    \ \_______\ \_______\ \__\ \__\ \__\\__\ \__\ __/  / /    
###      \|_______|\|__|     \|_______|\|__|\|__|    \|__|  \|_______|   \|__|  \|_______|\|__| \|__|\|_______|\|__|     \|_______|\|_______|\|__|\|__|\|__\|__|\|__||\___/ /     
###                                                                                                                                                                \|___|/                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
### 
###  update_tokopedia.py: Updates tokopedia price and stock based on AVO system
### 
###  input:  csv file from AVO, tokopedia mass update "Informasi Penjualan"
###  output: tokopedia mass update file
### 
###  This code is owned by CV. MAXI RAYA
###  Made by Eden Aristo Tingkir
###  For fixes, contact me at eden.aristo@gmail.com
###
####################################################################################################################################################
####################################################################################################################################################

#===================================================================================================================================================
# IMPORTS
#===================================================================================================================================================
import os
import pandas as pd
from openpyxl import load_workbook
import math
import numpy as np
import time
from datetime import datetime
from tabulate import tabulate
import re
import zipfile
from pathlib import Path
import shutil

#===================================================================================================================================================
# GLOBAL VARIABLES
#===================================================================================================================================================
INPUT_TOKOPEDIA_STOK_PATH = "INPUT/TOKOPEDIA_STOK"
INPUT_TOKOPEDIA_PROMO_PATH = "INPUT/TOKOPEDIA_PROMO"

INPUT_AVO_PROMO_PATH = "INPUT/AVO_PROMO"
INPUT_AVO_MASTER_PATH = "INPUT/AVO_MASTER"

SETTINGS_FEE_PATH = "SETTINGS/fee.xlsx"

HIGHEST_FEE = 0.08
STOCK_DIVISOR = 3
MAX_ENTRIES = 499

LOG_OUTPUT_FILE_NAME = 'tokopedia_logfile'
LOG_PATH = 'LOGS'

TOKOPEDIA_OUTPUT_FILE_NAME = 'tokopedia_output'
OUTPUT_PATH = 'OUTPUT'

OUTPUT_DELETION = True
INPUT_DELETION = False

SHEET_NAME = 'Ubah-Stok Lokasi Harga-Shop Adm'
TEMPLATE_SUBJECT = "EDT_PRICE_SHOP_ADMIN_HASH_7E998F126A0CD785FA34_FLT_ALL_T_9B774CD2BE82D1E3ACC1_2"


#===================================================================================================================================================
# HELPER FUNCTIONS
#===================================================================================================================================================
def format_table(df, table_name):
    return f"\n{table_name}\n" + ("-" * len(table_name)) + "\n" + (tabulate(df, headers="keys", tablefmt="grid")) + "\n"

#===================================================================================================================================================
# AVO MASTER DATA INPUTTING
#===================================================================================================================================================
#List all files in the folder 
files = os.listdir(INPUT_AVO_MASTER_PATH)

# Filter for .csv files
input_avo_master_files = [f for f in files if f.endswith('.csv')]

# Check if there is exactly 1 CSV file
avo_input_file_path = ''
if len(input_avo_master_files) == 1:
    avo_input_file_path = os.path.join(INPUT_AVO_MASTER_PATH, input_avo_master_files[0])
    avo_df = pd.read_csv(avo_input_file_path, header=0, skiprows=[0, 1, 2, 3], dtype={0:str, 5:int, 6:float, 7:float, 8:float})
    
    avo_df.columns = avo_df.columns.str.strip()  # Strip whitespace from column names
else:
    raise SystemExit("AVO_MASTER -> Error: There must be exactly 1 CSV file in the folder.")

avo_df.rename(columns={"Sku":"SKU", "Price 1":"Price", "T.Sku":"TSKU"}, inplace=True)

# Change the price 2 and price 3 into per piece price
avo_df["Price/pcs 2"] = avo_df["Price 2"] / avo_df["Tail 1"]
avo_df["Price/pcs 3"] = avo_df["Price 3"] / avo_df["Tail 2"]

# Change the 0 Price to NaN
avo_df["Price/pcs 2"] = avo_df["Price/pcs 2"].replace(0, np.nan)
avo_df["Price/pcs 3"] = avo_df["Price/pcs 3"].replace(0, np.nan)

#===================================================================================================================================================
# AVO PROMO DATA INPUTTING
#===================================================================================================================================================
# List all files in the folder 
files = os.listdir(INPUT_AVO_PROMO_PATH)

# Filter for .csv files
input_avo_promo_files = [f for f in files if f.endswith('.csv')]

# Check if there is exactly 1 CSV file
avo_promo_file_path = ''
if len(input_avo_promo_files) == 1:
    avo_promo_file_path = os.path.join(INPUT_AVO_PROMO_PATH, input_avo_promo_files[0])
    avo_promo_df = pd.read_csv(avo_promo_file_path, header=0, skiprows=[0, 1, 2], dtype={1:str, 3:float, 6:float})
    
    avo_promo_df.columns = avo_promo_df.columns.str.strip()  # Strip whitespace from column names
else:
    raise SystemExit("AVO_PROMO -> Error: There must be exactly 1 CSV file in the folder.")

avo_promo_df.rename(columns={"Sku":"SKU", "Price/pcs":"Price Awal", "Price/pcs.1":"Price Akhir", "Awal":"Waktu Awal", "Akhir": "Waktu Akhir"}, inplace=True)

avo_promo_df["Waktu Awal"] = pd.to_datetime(avo_promo_df["Waktu Awal"], format="%m/%d/%Y")
avo_promo_df["Waktu Akhir"] = pd.to_datetime(avo_promo_df["Waktu Akhir"], format="%m/%d/%Y")
avo_promo_df["Waktu Akhir"] = avo_promo_df["Waktu Akhir"].apply(lambda x: x.replace(hour=23, minute=59, second=59)) # end of night

now = datetime.now()

# Filter rows where now is within the time range
filtered_promo_df = avo_promo_df[(avo_promo_df["Waktu Awal"] <= now) & (avo_promo_df["Waktu Akhir"] >= now)]

# Filter rows where the discounted price is 0
filtered_promo_df = filtered_promo_df[filtered_promo_df["Price Akhir"] != 0]

# Put the promo price into avo_df
avo_df = avo_df.merge(filtered_promo_df[['SKU', 'Price Akhir']], on='SKU', how='left')

# Rename the column
avo_df.rename(columns={'Price Akhir': 'Discount Price'}, inplace=True)

#===================================================================================================================================================
# TOKOPEDIA STOK DATA INPUTTING
#===================================================================================================================================================
# Get list of zip files
zip_files = [f for f in os.listdir(INPUT_TOKOPEDIA_STOK_PATH) if f.endswith('.zip')]

# Ensure one zip file
if len(zip_files) != 1:
    raise SystemExit("TOKOPEDIA_STOK -> Error: There must be exactly 1 ZIP file in the folder.")

# Get the zip file path
zip_path = os.path.join(INPUT_TOKOPEDIA_STOK_PATH, zip_files[0])

# Extract zip files
extract_path = os.path.join(INPUT_TOKOPEDIA_STOK_PATH, "extracted_files")
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)
    
# Get the first folder inside the zip
extracted_folders = [f for f in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, f))]
excel_folder_path = os.path.join(extract_path, extracted_folders[0])  # Assume only one folder

# Dataframe list to store all dataframes
dataframes = []

# Initialize output file
toped_output_df = pd.DataFrame()

# Read all excel files in the folder
for file in os.listdir(excel_folder_path):
    file_path = os.path.join(excel_folder_path, file)
    if os.path.isfile(file_path) and (file.endswith('.xls') or file.endswith('.xlsx')):
        df = pd.read_excel(file_path, header=0, skiprows=[0,2], engine='openpyxl', dtype={"Product ID":str, "Harga (Rp)*": float})
        toped_output_df = pd.read_excel(file_path, header=None, engine='openpyxl')
        dataframes.append(df)
        
# Ensure there are Excel files found
if not dataframes:
    raise ValueError("TOKOPEDIA_STOK -> Error: No Excel files found in the folder.")

# Combine Dataframes
toped_stok_df = pd.concat(dataframes, ignore_index=True)

toped_stok_df = toped_stok_df[toped_stok_df["Lokasi"] == "Maxi Karang Jati"]


#===================================================================================================================================================
# TOKOPEDIA PROMO DATA INPUTTING
#===================================================================================================================================================
# Get list of zip files
zip_files = [f for f in os.listdir(INPUT_TOKOPEDIA_PROMO_PATH) if f.endswith('.zip')]

# Ensure one zip file
if len(zip_files) != 1:
    raise SystemExit("TOKOPEDIA_PROMO -> Error: There must be exactly 1 ZIP file in the folder.")

# Get the zip file path
zip_path = os.path.join(INPUT_TOKOPEDIA_PROMO_PATH, zip_files[0])

# Extract zip files
extract_path = os.path.join(INPUT_TOKOPEDIA_PROMO_PATH, "extracted_files")
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)
    
# Get the first folder inside the zip
extracted_folders = [f for f in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, f))]
excel_folder_path = os.path.join(extract_path, extracted_folders[0])  # Assume only one folder

# Dataframe list to store all dataframes
dataframes = []

# Read all excel files in the folder
for file in os.listdir(excel_folder_path):
    file_path = os.path.join(excel_folder_path, file)
    if os.path.isfile(file_path) and (file.endswith('.xls') or file.endswith('.xlsx')):
        df = pd.read_excel(file_path, header=0, skiprows=[0,2], engine='openpyxl', dtype={"ID Produk":str, "Nama SKU": str})
        dataframes.append(df)
        
# Ensure there are Excel files found
if not dataframes:
    raise ValueError("TOKOPEDIA_PROMO -> Error: No Excel files found in the folder.")

# Combine Dataframes
toped_promo_df = pd.concat(dataframes, ignore_index=True)

avo_df.rename(columns={'Price Akhir': 'Discount Price'}, inplace=True)
toped_promo_df.rename(columns={'Nama SKU': 'SKU', "ID Produk":'Product ID'}, inplace=True)



# Put SKU in toped_stok_df
toped_stok_df = toped_stok_df.merge(toped_promo_df[["Product ID", "SKU"]], on="Product ID", how="left")



# Settings Data Processing
# read the settings fee
settings_fee_df = pd.read_excel(SETTINGS_FEE_PATH, header=0, engine='openpyxl', dtype={"SKU": str, "fee": float})

# Remove duplicates from settings
settings_fee_df.drop_duplicates(subset="SKU", keep='first', inplace=True)

#Help fee merge by making a merge key
toped_stok_df['merge_key'] = toped_stok_df['SKU'].str.extract(r'(\d+)(?:x\d+)?')[0]
settings_fee_df['merge_key'] = settings_fee_df['SKU'].str.extract(r'(\d+)(?:x\d+)?')[0]

# put fee into toped_stok_df
toped_stok_df = toped_stok_df.merge(settings_fee_df[['merge_key', "fee"]], on='merge_key', how='left')

# drop the merge key
toped_stok_df.drop(columns=['merge_key'], inplace=True)



# Rename the Price and Stock columns
toped_stok_df.rename(columns={'Harga (Rp)*': 'Price', 'Stok Utama*': 'Stock'}, inplace=True)

# Reorder the columns on toped_stok_df for cleaner look
cols = list(toped_stok_df.columns)
cols.insert(1, cols.pop(cols.index('SKU')))
toped_stok_df = toped_stok_df[cols]


#===================================================================================================================================================
# MAIN DATA PROCESSING
#===================================================================================================================================================
bad_sku_df = pd.DataFrame(columns=toped_stok_df.columns)  # Rows where SKU exists in tokopedia but not in AVO
no_sku_df = pd.DataFrame(columns=toped_stok_df.columns)  # Rows where SKU is missing in tokopedia
format_sku_df = pd.DataFrame(columns=toped_stok_df.columns)  # Rows where SKU has a format error
duplicate_sku_df = pd.DataFrame(columns=toped_stok_df.columns)  # Rows where SKU is duplicated
updated_tracker_df = pd.DataFrame(columns=["SKU", "Name", "Price_Change", "Stock_Change"])    # items that has been updated both price and stock

bad_category_df = pd.DataFrame(columns=toped_stok_df.columns)  # Rows where category dont have fee category
num_stays = 0
rows_to_remove = []

updated_toped_stok_df = toped_stok_df.copy()
sku_first_occurence = {} # keep track of first occurence of sku, detect duplicates

# Iterate through each row in Tokopedia
for index, row in toped_stok_df.iterrows():
    
    curr_sku = row["SKU"]
    
    # Check if SKU is missing (NaN or empty)
    if pd.isna(curr_sku) or curr_sku =='':
        no_sku_df = pd.concat([no_sku_df, row.to_frame().T], ignore_index=True) # Append
        updated_toped_stok_df.at[index, "Stock"] = 0 # Make Stock 0
        continue
    
    # Check if SKU have wrong format
    if not re.match(r'^\d+(x\d+)?$', curr_sku):
        format_sku_df = pd.concat([format_sku_df, row.to_frame().T], ignore_index=True) # Append entire row
        updated_toped_stok_df.at[index, "Stock"] = 0 # Make Stock 0
        continue
    
    # Check for SKU duplicates
    if curr_sku in sku_first_occurence:
        duplicate_sku_df = pd.concat([duplicate_sku_df, row.to_frame().T], ignore_index=True) # Append entire row
        # if duplicate, set to 0 for this and the first occurence
        first_index = sku_first_occurence[curr_sku]
        updated_toped_stok_df.at[index, "Stock"] = 0 # Make stock 0
        updated_toped_stok_df.at[first_index, "Stock"] = 0 # Make stock 0
        continue
    else:
        # if not 0, store first occurence
        sku_first_occurence[curr_sku] = index
    
    # Separate SKU and Item multiplier
    multiplier = 1
    if 'x' in curr_sku:
        parts = curr_sku.split('x')
        curr_sku = parts[0]
        multiplier = int(parts[1])
    else:
        multiplier = 1
    
    # Find the matching SKU in avo_df
    matching_row = avo_df[avo_df["SKU"] == curr_sku]
    
    # If SKU is not found in avo_df, add to bad_sku_df
    if matching_row.empty or (not matching_row.empty and matching_row.iloc[0]["TSKU"] == "D"):
        bad_sku_df = pd.concat([bad_sku_df, row.to_frame().T], ignore_index=True) # Append entire row
        updated_toped_stok_df.at[index, "Stock"] = 0
        continue
    
    # Check for tail price
    tail = 1
    if multiplier >= matching_row.iloc[0]["Tail 2"] and pd.notna(matching_row.iloc[0]['Price/pcs 3']):
        tail = 3
    elif multiplier >= matching_row.iloc[0]['Tail 1'] and pd.notna(matching_row.iloc[0]['Price/pcs 2']):
        tail = 2
    else:
        tail = 1
        
    # GET PRICE AND STOCK
    avo_price = 0
    # if there is a discount price
    if pd.notna(matching_row.iloc[0]["Discount Price"]):
        if tail == 2: # discount price and price 2
            avo_price = min(matching_row.iloc[0]["Discount Price"], matching_row.iloc[0]["Price/pcs 2"])
        elif tail == 3: # discount price and price 3
            avo_price = min(matching_row.iloc[0]["Discount Price"], matching_row.iloc[0]["Price/pcs 3"])
        else: # just discount price
            avo_price = matching_row.iloc[0]["Discount Price"]
    elif tail == 2: # only price 2
        avo_price = matching_row.iloc[0]["Price/pcs 2"]
    elif tail == 3: # only price 3
        avo_price = matching_row.iloc[0]["Price/pcs 3"]
    else:
        avo_price = matching_row.iloc[0]["Price"]
    # Now stock
    avo_stock = matching_row.iloc[0]["Stock"]
    
    # Price and stock correction for boxed items
    avo_price = avo_price * multiplier
    avo_stock = avo_stock // multiplier
    
    # Current values in Tokopedia
    toped_price = row["Price"]
    toped_stock = row["Stock"]
    
    # Track updates
    price_change = None
    stock_change = None
    
    # If fee is missing, add to bad_category_df and set fee to highest
    if pd.isna(row["fee"]) or row["fee"] == '':
        bad_category_df = pd.concat([bad_category_df, row.to_frame().T], ignore_index=True) # Append entire row
        row["fee"] = HIGHEST_FEE
        # dont skip this row, its okay with wrong fee as long as we put it as highest fee
        
    # If price is different, format change
    price_now = float(math.ceil(avo_price / (1 - row["fee"])))
    if toped_price != price_now:
        price_change = f"{toped_price} -> {price_now}" # record price change
        updated_toped_stok_df.at[index, "Price"] = price_now # update price
        
    # If stock is different, format change
    stock_now = max(avo_stock // STOCK_DIVISOR, 0)
    if toped_stock != stock_now:
        stock_change = f"{toped_stock} -> {stock_now}" # record stock change
        updated_toped_stok_df.at[index, "Stock"] = stock_now # update stock
        
    if price_change or stock_change:
        updated_tracker_df = pd.concat([
            updated_tracker_df,
            pd.DataFrame([{
                "SKU": curr_sku,
                "Name": row["Nama Produk"],
                "Price_Change": price_change if price_change else "",
                "Stock_Change": stock_change if stock_change else ""
            }])
        ], ignore_index=True)
        continue
    
    num_stays = num_stays + 1
    
    # add this row to the rows to remove
    rows_to_remove.append(index)
    
# Remove rows that are not updated
updated_toped_stok_df.drop(rows_to_remove, inplace=True)


#===================================================================================================================================================
# OUTPUT DELETION
#===================================================================================================================================================
# Delete all past output files
if OUTPUT_DELETION:
    files = os.listdir(OUTPUT_PATH)
    for f in files:
        os.remove(os.path.join(OUTPUT_PATH, f))

#===================================================================================================================================================
# TOKOPEDIA OUTPUTTING
#===================================================================================================================================================
# Timestamp
timestamp = time.strftime("%d-%m-%Y_%H-%M-%S")

# Make a new excel template dataframe
toped_output_df = toped_output_df.iloc[:3]
updated_toped_stok_df = updated_toped_stok_df.drop(columns=['fee', 'SKU'])

# Split the dataframe into chunks of 500s
chunks = np.array_split(updated_toped_stok_df, np.ceil(len(updated_toped_stok_df) / MAX_ENTRIES))

for i, chunk in enumerate(chunks):
    # Combine toped_output_df (first 2 rows) with the current chunk
    final_output = pd.concat([toped_output_df, pd.DataFrame(chunk.values)], ignore_index=True)

    filename = f"{TOKOPEDIA_OUTPUT_FILE_NAME} ({i+1})_{timestamp}.xlsx"
    
    # Generate full file path
    path = os.path.join(OUTPUT_PATH, filename)

    # Save to Excel
    final_output.to_excel(path, index=False, header=False)
    
    # PROPERTIES CORRECTION FOR TOKOPEDIA TEMPLATE DETECTION
    
    # Load the workbook
    wb = load_workbook(path)
    
    # Modify document properties
    props = wb.properties
    props.subject = TEMPLATE_SUBJECT

    # Get the only sheet (assuming there's just one sheet)
    sheet = wb.active

    # Rename the sheet
    sheet.title = SHEET_NAME

    # Make the sheet active (just in case Tokopedia expects it)
    wb.active = 0

    # Save the file
    wb.save(path)

    

#===================================================================================================================================================
# LOG OUTPUTTING
#===================================================================================================================================================
# make the file and path
filename = f"{LOG_OUTPUT_FILE_NAME}_{timestamp}.txt"
path = os.path.join(LOG_PATH, filename)

log_content = f"""===========================================================
LOG FILE - Tokopedia Updater
Date: {timestamp}
===========================================================

LOG SUMMARY:
{len(no_sku_df):7} empty SKU on Tokopedia
{len(format_sku_df):7} format error SKU on Tokopedia
{len(duplicate_sku_df):7} duplicate SKU on Tokopedia
{len(bad_sku_df):7} SKUs on Tokopedia that is not found on AVO system
{len(bad_category_df):7} SKU items that is missing fee category

{len(updated_tracker_df):7} SKU items updated (price or stock)
{num_stays:7} SKU items stayed (price or stock)


================================================================================================================================
================================================================================================================================

{format_table(no_sku_df, "EMPTY SKU TABLE (on Tokopedia)")}

{format_table(format_sku_df, "FORMAT ERROR SKU TABLE (on Tokopedia)")}

{format_table(duplicate_sku_df, "DUPLICATE SKU TABLE (SKUs on Tokopedia that is duplicated)")}

{format_table(bad_sku_df, "BAD SKU TABLE (SKUs on Tokopedia that is not found on AVO system)")}

{format_table(bad_category_df, "BAD CATEGORY TABLE (SKU items that is missing fee category)")}

{format_table(updated_tracker_df, "UPDATES TABLE")}

{format_table(updated_toped_stok_df, "FINAL UPDATED TABLE")}

================================================================================================================================
End of log file
"""

# Write Log to File
with open(path, "w") as file:
    file.write(log_content)
    
    
#===================================================================================================================================================
# INPUT DELETION
#===================================================================================================================================================
if INPUT_DELETION:
    tokopedia_promo_folder = Path(INPUT_TOKOPEDIA_PROMO_PATH)
    tokopedia_stok_folder = Path(INPUT_TOKOPEDIA_STOK_PATH)
    
    # Delete inside of promo
    for item in tokopedia_promo_folder.iterdir():
        if item.is_file() or item.is_symlink():
            item.unlink()  # Delete file or symlink
        elif item.is_dir():
            shutil.rmtree(item)  # Delete directory
    
    # Delete inside of stok
    for item in tokopedia_promo_folder.iterdir():
        if item.is_file() or item.is_symlink():
            item.unlink()  # Delete file or symlink
        elif item.is_dir():
            shutil.rmtree(item)  # Delete directory
    
    os.remove(avo_input_file_path)
    os.remove(avo_promo_file_path)
    
    
####################################################################################################################################################
####################################################################################################################################################
# end of code
