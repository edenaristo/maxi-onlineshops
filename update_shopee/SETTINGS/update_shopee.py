###  ██╗   ██╗██████╗ ██████╗  █████╗ ████████╗███████╗        ███████╗██╗  ██╗ ██████╗ ██████╗ ███████╗███████╗   ██████╗ ██╗   ██╗
###  ██║   ██║██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝██╔════╝        ██╔════╝██║  ██║██╔═══██╗██╔══██╗██╔════╝██╔════╝   ██╔══██╗╚██╗ ██╔╝
###  ██║   ██║██████╔╝██║  ██║███████║   ██║   █████╗          ███████╗███████║██║   ██║██████╔╝█████╗  █████╗     ██████╔╝ ╚████╔╝ 
###  ██║   ██║██╔═══╝ ██║  ██║██╔══██║   ██║   ██╔══╝          ╚════██║██╔══██║██║   ██║██╔═══╝ ██╔══╝  ██╔══╝     ██╔═══╝   ╚██╔╝  
###  ╚██████╔╝██║     ██████╔╝██║  ██║   ██║   ███████╗███████╗███████║██║  ██║╚██████╔╝██║     ███████╗███████╗██╗██║        ██║   
###   ╚═════╝ ╚═╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝╚══════╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚═╝     ╚══════╝╚══════╝╚═╝╚═╝        ╚═╝   
### 
###  update_shopee.py: Updates shopee price and stock based on AVO system
### 
###  input:  csv file from AVO, shopee mass update "Informasi Penjualan"
###  output: shopee mass update file
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
import sys
import pandas as pd
from openpyxl import load_workbook
import math
import numpy as np
import time
from datetime import datetime
from tabulate import tabulate
import re


#===================================================================================================================================================
# GLOBAL VARIABLES
#===================================================================================================================================================
SETTINGS_FEE_PATH = 'SETTINGS/fee.xlsx'
SETTINGS_CATEGORY_PATH = 'SETTINGS/kode kategori.xlsx'

INPUT_AVO_MASTER_PATH = 'INPUT/AVO_MASTER'
INPUT_AVO_PROMO_PATH = 'INPUT/AVO_PROMO'
INPUT_SHOPEE_PENJUALAN_PATH = 'INPUT/SHOPEE_INFORMASI PENJUALAN'
INPUT_SHOPEE_CATEGORY_PATH = 'INPUT/SHOPEE_INFORMASI DIKIRIM DALAM'

OUTPUT_PATH = 'OUTPUT'
SHOPEE_OUTPUT_FILE_NAME = 'shopee_output'

LOG_PATH = "LOGS"
LOG_OUTPUT_FILE_NAME = 'shopee_logfile'

STOCK_DIVISOR = 3
HIGHEST_FEE = 0.08

INPUT_DELETION = False # Set to True to delete input files after processing
OUTPUT_DELETION = True # Set to True to delete output files before outputting


#===================================================================================================================================================
# HELPER FUNCTIONS
#===================================================================================================================================================
def format_table(df, table_name):
    return f"\n{table_name}\n" + ("-" * len(table_name)) + "\n" + (tabulate(df, headers="keys", tablefmt="grid")) + "\n"


#===================================================================================================================================================
# PATH PROCESSING
#===================================================================================================================================================
# Get the directory where the EXE (or script) is running
if getattr(sys, 'frozen', False):  
    BASE_DIR = os.path.dirname(sys.executable)  # Running as an .exe
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Running as a .py script

# Use relative paths instead of absolute ones
file_path = os.path.join(BASE_DIR, "config.json")


#===================================================================================================================================================
# DATA LOADING & INPUTTING
#===================================================================================================================================================
# 1) AVO DATA PROCESSING

# List all files in the folder 
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


# ------------------------------------------------------------------------------------------------------------
# 2) SHOPEE UPDATE DATA PROCESSING

# List all files in the folder
files = os.listdir(INPUT_SHOPEE_PENJUALAN_PATH)

# Filter for .xlsx files
input_shopee_penjualan_files = [f for f in files if f.endswith('.xlsx')]

# Check if there is exactly 1 XLSX file
shopee_input_penjualan_file_path = ''
if len(input_shopee_penjualan_files) == 1:
    shopee_input_penjualan_file_path = os.path.join(INPUT_SHOPEE_PENJUALAN_PATH, input_shopee_penjualan_files[0])
    shopee_df = pd.read_excel(shopee_input_penjualan_file_path, header=0, skiprows=[0, 1, 3, 4, 5], engine='openpyxl', dtype={"SKU": str, "Stok": int, "Harga": float, "SKU Induk": str})
    shopee_output_df = pd.read_excel(shopee_input_penjualan_file_path, engine='openpyxl', header=None)

else:
    raise SystemExit("SHOPEE_INFORMASI PENJUALAN -> Error: There must be exactly 1 XLSX file in the folder.")
    
    
shopee_df.rename(columns={"Stok":"Stock", "Harga":"Price"}, inplace=True)

# ------------------------------------------------------------------------------------------------------------
# 3) SHOPEE CATEGORY UPDATE DATA PROCESSING

# List all files in the folder
files = os.listdir(INPUT_SHOPEE_CATEGORY_PATH)

# Filter for .xlsx files
input_shopee_category_files = [f for f in files if f.endswith('.xlsx')]

# Check if there is exactly 1 XLSX file
shopee_input_category_file_path = ''
if len(input_shopee_penjualan_files) == 1:
    shopee_input_category_file_path = os.path.join(INPUT_SHOPEE_CATEGORY_PATH, input_shopee_category_files[0])
    shopee_category_df = pd.read_excel(shopee_input_category_file_path, header=0, skiprows=[0, 1, 3, 4, 5], engine='openpyxl')
    shopee_output_df = pd.read_excel(shopee_input_penjualan_file_path, engine='openpyxl', header=None)

else:
    raise SystemExit("SHOPEE_INFORMASI DIKIRIM DALAM -> Error: There must be exactly 1 XLSX file in the folder.")

# extract category id
shopee_category_df["category_id"] = shopee_category_df["Kategori"].str.extract(r'(\d+)').astype(str)

# Combine category information into shopee_df
shopee_df = shopee_df.merge(
    shopee_category_df[["Kode Produk", "Kode Variasi", "category_id"]],
    on=["Kode Produk", "Kode Variasi"],  # Ensure matching on both columns
    how="left"  # Keeps all rows from shopee_df
)

# Rename "category_id" to "category" for consistency
shopee_df.rename(columns={"category_id": "category"}, inplace=True)

# ------------------------------------------------------------------------------------------------------------
# 4) SETTINGS DATA PROCESSING

# Load settings data
settings_category_df = pd.read_excel(SETTINGS_CATEGORY_PATH, header=0, engine='openpyxl', dtype={"kategori id": str, "fee kategori": str})
settings_fee_df = pd.read_excel(SETTINGS_FEE_PATH, header=0, engine='openpyxl', dtype={"kategori": str, "fee": float})

# Delete duplicate from fee
settings_category_df.drop_duplicates(subset="kategori id", keep='first', inplace=True)

# Merge settings data into shopee_df
shopee_df["fee category"] = shopee_df["category"].map(settings_category_df.set_index("kategori id")["fee kategori"])
shopee_df["fee"] = shopee_df["fee category"].map(settings_fee_df.set_index("kategori")["fee"])

# ------------------------------------------------------------------------------------------------------------
# 5) AVO PROMO DATA PROCESSING

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
# MAIN DATA PROCESSING SECTION
#===================================================================================================================================================
# 1) DATA UPDATE PROCESSING

# tracker for logs
bad_sku_df = pd.DataFrame(columns=shopee_df.columns)  # Rows where SKU exists in Shopee but not in AVO
no_sku_df = pd.DataFrame(columns=shopee_df.columns)  # Rows where SKU is missing in Shopee
duplicate_sku_df = pd.DataFrame(columns=shopee_df.columns)  # Rows where SKU is duplicated
conflict_sku_df = pd.DataFrame(columns=shopee_df.columns)  # Rows where SKU has a conflict 
format_sku_df = pd.DataFrame(columns=shopee_df.columns)  # Rows where SKU has a format error
updated_tracker_df = pd.DataFrame(columns=["SKU", "Name", "Price_Change", "Stock_Change"])    # items that has been updated both price and stock

bad_category_df = pd.DataFrame(columns=shopee_df.columns)  # Rows where category dont have fee category
num_stays = 0

updated_shopee_df = shopee_df.copy()  # keep track of final shopee dataframe after updates
sku_first_occurence = {} # keep track of first occurence of sku, detect duplicates

# Iterate through each row in shopee_df
for index, row in shopee_df.iterrows():
    
    # check for conflicting sku
    if pd.notna(row["SKU"]) and row["SKU"] != '' and pd.notna(row["SKU Induk"]) and row["SKU Induk"] != '':
        conflict_sku_df = pd.concat([conflict_sku_df, row.to_frame().T], ignore_index=True) # Append entire row
        updated_shopee_df.at[index, "Stock"] = 0 # Make stock 0
        continue # skip this row
    
    # use the non empty sku
    if pd.notna(row['SKU']) and row['SKU'] != '': 
        shopee_sku = row['SKU']
    elif pd.notna(row['SKU Induk']) and row['SKU Induk'] != '':
        shopee_sku = row['SKU Induk']
    else: # both doesnt exist
        shopee_sku = np.nan

    # Check if SKU is missing (NaN or empty)
    if pd.isna(shopee_sku) or shopee_sku == "":
        no_sku_df = pd.concat([no_sku_df, row.to_frame().T], ignore_index=True) # Append entire row
        updated_shopee_df.at[index, "Stock"] = 0 # Make stock 0
        continue  # Skip this row
    
    # Check if SKU have wrong format
    if not re.match(r'^\d+(x\d+)?$', shopee_sku):
        format_sku_df = pd.concat([format_sku_df, row.to_frame().T], ignore_index=True) # Append entire row
        updated_shopee_df.at[index, "Stock"] = 0 # Make stock 0
        continue # SKip this row
    
    # Check for SKU duplicates
    if shopee_sku in sku_first_occurence:
        duplicate_sku_df = pd.concat([duplicate_sku_df, row.to_frame().T], ignore_index=True) # Append entire row
        # if duplicate, set to 0 for this and the first occurence
        first_index = sku_first_occurence[shopee_sku]
        updated_shopee_df.at[index, "Stock"] = 0 # Make stock 0
        updated_shopee_df.at[first_index, "Stock"] = 0 # Make stock 0
        continue
    else:
        # if not 0, store first occurence
        sku_first_occurence[shopee_sku] = index

    # Separate the SKU and the item number
    multiplier = 1
    if "x" in shopee_sku:
        parts = shopee_sku.split("x")
        shopee_sku = parts[0]
        multiplier = int(parts[1])
    else:
        multiplier = 1
    
    # Find the matching SKU in avo_df
    matching_row = avo_df[avo_df["SKU"] == shopee_sku]

    # If no matching SKU found in avo_df, or found but tsku is D add to bad_sku_df
    if matching_row.empty or (not matching_row.empty and matching_row.iloc[0]["TSKU"] == "D"):
        bad_sku_df = pd.concat([bad_sku_df, row.to_frame().T], ignore_index=True)  # Append entire row
        updated_shopee_df.at[index, "Stock"] = 0 # Make stock 0
        continue  # Skip this row

    # Check for tail price
    tail = 1
    if multiplier >= matching_row.iloc[0]["Tail 2"] and pd.notna(matching_row.iloc[0]["Price/pcs 3"]):
        tail = 3
    elif multiplier >= matching_row.iloc[0]["Tail 1"] and pd.notna(matching_row.iloc[0]["Price/pcs 2"]):
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
    
    avo_stock = matching_row.iloc[0]["Stock"]  
    
    # Price and stock correction for boxed items
    avo_price = avo_price * multiplier
    avo_stock = avo_stock // multiplier
    
    # Current values in Shopee
    shopee_price = row["Price"]
    shopee_stock = row["Stock"]

    # Track updates
    price_change = None
    stock_change = None

    # If fee is missing, add to bad_category_df and set fee to highest
    if pd.isna(row["fee"]):
        bad_category_df = pd.concat([bad_category_df, row.to_frame().T], ignore_index=True)  # Append entire row
        row["fee"] = HIGHEST_FEE
        # dont skip this row
    
    # If price is different, format change
    price_now = float(math.ceil(avo_price / (1 - row["fee"])))
    if shopee_price != price_now:
        price_change = f"{shopee_price} -> {price_now}"
        updated_shopee_df.at[index, "Price"] = price_now  # update price
    
    # If stock is different, format change
    stock_now = max(avo_stock // STOCK_DIVISOR, 0)
    if shopee_stock != stock_now:
        stock_change = f"{shopee_stock} -> {stock_now}"
        updated_shopee_df.at[index, "Stock"] = stock_now # Update stock

    # If any updates occurred, add row to updated_tracker_df
    if price_change or stock_change:
        updated_tracker_df = pd.concat([
            updated_tracker_df,
            pd.DataFrame([{
                "SKU": shopee_sku,
                "Name": row["Nama Produk"],  # Adjust column name if needed
                "Price_Change": price_change if price_change else "",
                "Stock_Change": stock_change if stock_change else ""
            }])
        ], ignore_index=True)
        continue
        
    num_stays = num_stays+1

#===================================================================================================================================================
# SHOPEE UPDATE OUTPUTTING
#===================================================================================================================================================
# Make a new excel template dataframe, ready to be put into file
offset = 6
shopee_output_df.loc[offset:offset+len(updated_shopee_df)-1, 6] = updated_shopee_df['Price'].values
shopee_output_df.loc[offset:offset+len(updated_shopee_df)-1, 7] = updated_shopee_df['Stock'].values


# Make a new excel file, output
# put time
timestamp = time.strftime("%d-%m-%Y_%H-%M-%S")

# make the file and path
filename=  f"{SHOPEE_OUTPUT_FILE_NAME}_{timestamp}.xlsx"
path = os.path.join(OUTPUT_PATH, filename)

# Delete all past output files
if OUTPUT_DELETION:
    files = os.listdir(OUTPUT_PATH)
    for f in files:
        os.remove(os.path.join(OUTPUT_PATH, f))

# finally, write the file
shopee_output_df.to_excel(path, index=False, header=False)

#===================================================================================================================================================
# LOG OUTPUTTING
#===================================================================================================================================================
# make the file and path
filename = f"{LOG_OUTPUT_FILE_NAME}_{timestamp}.txt"
path = os.path.join(LOG_PATH, filename)

log_content = f"""===========================================================
LOG FILE - Shopee Updater
Date: {timestamp}
===========================================================

LOG SUMMARY:
{len(no_sku_df):7} empty SKU on Shopee
{len(conflict_sku_df):7} SKUs on Shopee that conflicted (SKU vs SKU Induk)
{len(format_sku_df):7} format error SKU on Shopee
{len(duplicate_sku_df):7} duplicate SKUs found on Shopee
{len(bad_sku_df):7} SKUs on Shopee that is not found on AVO system
{len(bad_category_df):7} SKU items that is missing fee category

{len(updated_tracker_df):7} SKU items updated (price or stock)
{num_stays:7} SKU items stayed (price or stock)

================================================================================================================================
================================================================================================================================

{format_table(no_sku_df, "EMPTY SKU TABLE (on Shopee)")}

{format_table(conflict_sku_df, "CONFLICTING SKU TABLE (SKUs on Shopee that conflicted SKU vs SKU Induk)")}

{format_table(format_sku_df, "FORMAT ERROR SKU TABLE (on Shopee)")}

{format_table(duplicate_sku_df, "DUPLICATE SKU TABLE (SKUs on Shopee that is duplicated)")}

{format_table(bad_sku_df, "BAD SKU TABLE (SKUs on Shopee that is not found on AVO system)")}

{format_table(bad_category_df, "BAD CATEGORY TABLE (SKU items that is missing fee category)")}

{format_table(updated_tracker_df, "UPDATES TABLE")}

{format_table(updated_shopee_df, "FINAL UPDATED TABLE")}

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
    os.remove(shopee_input_penjualan_file_path)
    os.remove(shopee_input_category_file_path)
    os.remove(avo_input_file_path)
    os.remove(avo_promo_file_path)


####################################################################################################################################################
####################################################################################################################################################
# end of code