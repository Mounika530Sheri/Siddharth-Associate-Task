#!/usr/bin/env python3
"""
process_trade_data.py
Comprehensive pipeline to read 'Sample Data 2.xlsx', parse & clean GOODS DESCRIPTION,
produce summaries (Year, HSN, Model, Supplier), charts, and write 'Trade_Analysis_YourName.xlsx'.

No external lookup file required — contains an internal HSN fallback mapping that you can expand.
"""

import os
import re
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


INPUT_FILENAME = "Sample Data 2.xlsx"
OUTPUT_XLSX = "Trade_Analysis_YourName.xlsx"
CHART_DIR = "output_charts"


os.makedirs(CHART_DIR, exist_ok=True)


def safe_year(val):
    try:
        return pd.to_datetime(val, errors="coerce").year
    except:
        return np.nan

def parse_goods_description(text):
    """
    Parse model, qty, unit, unit_price_usd, currency if present from GOODS DESCRIPTION string.
    Returns dict with keys: parsed_model, qty, unit, unit_price_usd, currency
    """
    res = {'parsed_model': np.nan, 'qty': np.nan, 'unit': np.nan, 'unit_price_usd': np.nan, 'currency': np.nan}
    if not isinstance(text, str):
        return res
    s = text.strip()
    up = s.upper()

    
    m_model = re.search(r"^([A-Z0-9\-\/]+(?:\s+[A-Z0-9\-\/]+){0,3})\s*(?:\(|QTY|/|USD|CNY|INR)", up)
    if m_model:
        res['parsed_model'] = m_model.group(1).strip()
    else:
        
        res['parsed_model'] = " ".join(up.split()[:4])

   
    m_qty = re.search(r"QTY[:\s\-]*([0-9,\.]+)", up)
    if not m_qty:
        m_qty = re.search(r"([0-9,]+)\s*(PCS|PIECES|SETS|SET|KGS|KG)\b", up)
    if m_qty:
        try:
            res['qty'] = float(m_qty.group(1).replace(',', ''))
        except:
            res['qty'] = np.nan

  
    m_unit = re.search(r"\b(PCS|PIECES|SETS|SET|KGS|KG|NOS)\b", up)
    if m_unit:
        res['unit'] = m_unit.group(1)

    
   
    m_price = re.search(r"(USD|CNY|INR|/USD|USD:|CNY:)\s*([0-9]+(?:\.[0-9]+)?)", up)
    if not m_price:
       
        m_price = re.search(r"(USD)?\s*([0-9]+\.[0-9]+)\s*(?:PER|/)\s*(PCS|SET|SETS|KGS|KG)?", up)
    if m_price:
       
        if m_price.group(1) and m_price.group(1).strip() in ("USD", "CNY", "INR", "/USD", "USD:", "CNY:"):
            cur = m_price.group(1).replace("/", "").replace(":", "")
            res['currency'] = cur
            try:
                res['unit_price_usd'] = float(m_price.group(2))
            except:
                res['unit_price_usd'] = np.nan
        else:
            
            try:
                res['unit_price_usd'] = float(m_price.group(2))
               
            except:
                res['unit_price_usd'] = np.nan

    return res

def safe_numeric(x):
    try:
        return float(x)
    except:
        return np.nan


if not Path(INPUT_FILENAME).exists():
    raise FileNotFoundError(f"Input file '{INPUT_FILENAME}' not found. Put it in the same folder as this script.")

print("Loading Excel:", INPUT_FILENAME)
raw = pd.read_excel(INPUT_FILENAME, sheet_name=0, engine="openpyxl")

raw.columns = [str(c).strip().upper().replace(" ", "_") for c in raw.columns]


if 'HS_CODE' not in raw.columns and 'HS' in raw.columns:
    raw = raw.rename(columns={'HS': 'HS_CODE'})


raw_copy = raw.copy()


hsn_rows = [
    ("73239990", "Table/kitchen or other household articles of iron or steel - other", "Steel", "Household articles", 12),
    ("73231000", "Scouring pads and scouring implements of iron or steel", "Steel", "Scrubber", 12),
    ("73239100", "Table/kitchen articles of iron/steel - cast iron", "Steel", "Cast iron utensils", 12),
    ("7310", "Tanks, casks, drums, cans, boxes and similar containers of iron/steel", "Steel", "Containers", 18),
    ("8215", "Spoons, forks, ladles etc (cutlery)", "Metal", "Cutlery", 12),
   
]
hsn_lookup = pd.DataFrame(hsn_rows, columns=['HS_CODE','HSN_DESCRIPTION','MAIN_CATEGORY','SUB_CATEGORY','GST_Rate'])
hsn_lookup['HS_CODE'] = hsn_lookup['HS_CODE'].astype(str)


df = raw.copy()

for col in ['PORT_CODE','DATE','IEC','HS_CODE','GOODS_DESCRIPTION','TOTAL_VALUE_INR','DUTY_PAID_INR','UNIT_PRICE_USD','QUANTITY','UNIT','TOTAL_VALUE_USD']:
    if col not in df.columns:
        df[col] = np.nan


df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
df = df.dropna(subset=['DATE'])
df['Year'] = df['DATE'].dt.year


for c in ['TOTAL_VALUE_INR','DUTY_PAID_INR','UNIT_PRICE_USD','TOTAL_VALUE_USD','QUANTITY']:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')


print("Parsing GOODS_DESCRIPTION to extract model, qty, unit, unit price...")
parsed = df['GOODS_DESCRIPTION'].fillna('').astype(str).apply(parse_goods_description)
parsed_df = pd.DataFrame(parsed.tolist()).rename(columns={
    'parsed_model': 'PARSED_MODEL',
    'qty': 'PARSED_QTY',
    'unit': 'PARSED_UNIT',
    'unit_price_usd':'PARSED_UNITPRICE_USD',
    'currency':'PARSED_CURRENCY'
})
df = pd.concat([df.reset_index(drop=True), parsed_df.reset_index(drop=True)], axis=1)

df['QUANTITY_FINAL'] = df['QUANTITY']
mask_q = (df['QUANTITY_FINAL'].isna()) | (df['QUANTITY_FINAL'] == 0)
df.loc[mask_q, 'QUANTITY_FINAL'] = df.loc[mask_q, 'PARSED_QTY']

df['UNIT_PRICE_USD_FINAL'] = df['UNIT_PRICE_USD']
mask_p = df['UNIT_PRICE_USD_FINAL'].isna()
df.loc[mask_p, 'UNIT_PRICE_USD_FINAL'] = df.loc[mask_p, 'PARSED_UNITPRICE_USD']


df['HS_CODE'] = df['HS_CODE'].astype(str).str.strip().replace('nan', np.nan)


df['HS_CODE_KEY'] = df['HS_CODE'].fillna('').astype(str)


df = df.merge(hsn_lookup.rename(columns={'HS_CODE':'HS_CODE_KEY'}), on='HS_CODE_KEY', how='left')

no_match_mask = df['HSN_DESCRIPTION'].isna()
if no_match_mask.any():
    df.loc[no_match_mask, 'HS_CODE_6'] = df.loc[no_match_mask, 'HS_CODE'].astype(str).str[:6]
    
    if 'HS_CODE' in hsn_lookup.columns:
        
        hsn_lookup['HS6'] = hsn_lookup['HS_CODE'].astype(str).str[:6]
        df = df.merge(hsn_lookup[['HS6','HSN_DESCRIPTION','MAIN_CATEGORY','SUB_CATEGORY','GST_Rate']].rename(columns={'HS6':'HS_CODE_6'}), on='HS_CODE_6', how='left', suffixes=('','_6'))
        
        for col in ['HSN_DESCRIPTION','MAIN_CATEGORY','SUB_CATEGORY','GST_Rate']:
            df[col] = df[col].fillna(df.get(col + '_6'))
       
        df.drop(columns=['HS_CODE_6'], inplace=True, errors='ignore')


df['HSN_DESCRIPTION'] = df['HSN_DESCRIPTION'].fillna("Unknown HSN")
df['MAIN_CATEGORY'] = df['MAIN_CATEGORY'].fillna("Unknown")
df['SUB_CATEGORY'] = df['SUB_CATEGORY'].fillna("Unknown")
df['GST_Rate'] = df['GST_Rate'].fillna(np.nan)


df['TOTAL_VALUE_INR'] = pd.to_numeric(df['TOTAL_VALUE_INR'], errors='coerce')
df['DUTY_PAID_INR'] = pd.to_numeric(df['DUTY_PAID_INR'], errors='coerce')
df['GrandTotal_INR'] = df[['TOTAL_VALUE_INR','DUTY_PAID_INR']].sum(axis=1, skipna=True)


df['Duty_pct'] = np.where(df['TOTAL_VALUE_INR']>0, df['DUTY_PAID_INR']/df['TOTAL_VALUE_INR'], np.nan)


df['Duty_per_unit_INR'] = np.where(df['QUANTITY_FINAL']>0, df['DUTY_PAID_INR']/df['QUANTITY_FINAL'], np.nan)


df['UNIT_PRICE_INR_COMPUTED'] = np.where(df['QUANTITY_FINAL']>0, df['TOTAL_VALUE_INR']/df['QUANTITY_FINAL'], np.nan)


avg_duty_pct = df['Duty_pct'].mean(skipna=True)
df['Duty_Exception'] = np.where(df['Duty_pct'] > avg_duty_pct, True, False)

print("Computing summaries...")


year_summary = df.groupby('Year', as_index=False).agg(
    Total_Value_INR = ('TOTAL_VALUE_INR','sum'),
    Total_Duty_INR = ('DUTY_PAID_INR','sum'),
    GrandTotal_INR = ('GrandTotal_INR','sum'),
    Total_Qty = ('QUANTITY_FINAL','sum')
).sort_values('Year')
year_summary['YoY_GrandTotal_pct'] = year_summary['GrandTotal_INR'].pct_change()


hsn_summary = df.groupby('HS_CODE', as_index=False).agg(
    HSN_Description = ('HSN_DESCRIPTION', lambda x: x.dropna().mode().iloc[0] if len(x.dropna())>0 else "Unknown"),
    Total_Value_INR = ('TOTAL_VALUE_INR','sum'),
    Total_Duty_INR = ('DUTY_PAID_INR','sum'),
    GrandTotal_INR = ('GrandTotal_INR','sum'),
    Total_Qty = ('QUANTITY_FINAL','sum')
).sort_values('GrandTotal_INR', ascending=False)

overall_grand_total = hsn_summary['GrandTotal_INR'].sum()
hsn_summary['Pct_Contribution'] = np.where(overall_grand_total>0, hsn_summary['GrandTotal_INR']/overall_grand_total, 0)
hsn_summary['Rank'] = hsn_summary['Pct_Contribution'].rank(method='first', ascending=False).astype(int)


top_n = 25
top25 = hsn_summary.head(top_n).copy()
others = hsn_summary.iloc[top_n:].copy()
if not others.empty:
    others_row = pd.DataFrame([{
        'HS_CODE': 'OTHERS',
        'HSN_Description': 'OTHERS',
        'Total_Value_INR': others['Total_Value_INR'].sum(),
        'Total_Duty_INR': others['Total_Duty_INR'].sum(),
        'GrandTotal_INR': others['GrandTotal_INR'].sum(),
        'Total_Qty': others['Total_Qty'].sum(),
        'Pct_Contribution': others['GrandTotal_INR'].sum()/overall_grand_total if overall_grand_total>0 else 0,
        'Rank': 999
    }])
else:
    others_row = pd.DataFrame([])


df['PARSED_MODEL'] = df['PARSED_MODEL'].fillna('UNKNOWN_MODEL')
model_summary = df.groupby(['Year','PARSED_MODEL'], as_index=False).agg(
    Sum_Qty = ('QUANTITY_FINAL','sum'),
    Avg_UnitPrice_USD = ('UNIT_PRICE_USD_FINAL','mean'),
    Total_Value_INR = ('TOTAL_VALUE_INR','sum'),
    GrandTotal_INR = ('GrandTotal_INR','sum')
).sort_values(['Year','GrandTotal_INR'], ascending=[True,False])


df['SUPPLIER_IEC'] = df['IEC'].fillna('UNKNOWN_IEC').astype(str)
supplier_summary = df.groupby(['Year','SUPPLIER_IEC'], as_index=False).agg(
    Total_Value_INR = ('TOTAL_VALUE_INR','sum'),
    Total_Qty = ('QUANTITY_FINAL','sum'),
    GrandTotal_INR = ('GrandTotal_INR','sum')
).sort_values(['Year','GrandTotal_INR'], ascending=[True,False])

latest_year = int(df['Year'].dropna().max()) if not df['Year'].dropna().empty else None
active_suppliers_latest = supplier_summary[supplier_summary['Year']==latest_year] if latest_year else pd.DataFrame()


print("Generating charts...")


plt.figure(figsize=(8,4))
plt.plot(year_summary['Year'], year_summary['GrandTotal_INR'], marker='o')
plt.title('Year-wise Grand Total (INR)')
plt.xlabel('Year')
plt.ylabel('Grand Total INR')
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
chart1 = os.path.join(CHART_DIR, 'yearly_grandtotal.png')
plt.savefig(chart1)
plt.close()


top10 = hsn_summary.head(10)
if not top10.empty:
    plt.figure(figsize=(6,6))
    labels = top10['HS_CODE'].astype(str)
    plt.pie(top10['Pct_Contribution'], labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title('Top 10 HSN contribution')
    plt.tight_layout()
    chart2 = os.path.join(CHART_DIR, 'hsn_top10_pie.png')
    plt.savefig(chart2)
    plt.close()
else:
    chart2 = None


if latest_year:
    models_latest = model_summary[model_summary['Year']==latest_year].nlargest(10, 'GrandTotal_INR')
    if not models_latest.empty:
        plt.figure(figsize=(8,5))
        plt.barh(models_latest['PARSED_MODEL'].astype(str), models_latest['GrandTotal_INR'])
        plt.gca().invert_yaxis()
        plt.title(f'Top models by GrandTotal ({latest_year})')
        plt.xlabel('Grand Total INR')
        plt.tight_layout()
        chart3 = os.path.join(CHART_DIR, f'top_models_{latest_year}.png')
        plt.savefig(chart3)
        plt.close()
    else:
        chart3 = None
else:
    chart3 = None


top_suppliers = supplier_summary.groupby('SUPPLIER_IEC', as_index=False)['GrandTotal_INR'].sum().nlargest(5,'GrandTotal_INR')['SUPPLIER_IEC'].tolist()
if top_suppliers:
    pivot_sup = df[df['SUPPLIER_IEC'].isin(top_suppliers)].pivot_table(index='Year', columns='SUPPLIER_IEC', values='GrandTotal_INR', aggfunc='sum', fill_value=0)
    pivot_sup.plot(kind='line', marker='o', figsize=(10,5))
    plt.title('Top suppliers trend (GrandTotal INR)')
    plt.ylabel('Grand Total INR')
    plt.tight_layout()
    chart4 = os.path.join(CHART_DIR, 'supplier_trends_top5.png')
    plt.savefig(chart4)
    plt.close()
else:
    chart4 = None


print("Writing Excel workbook:", OUTPUT_XLSX)
with pd.ExcelWriter(OUTPUT_XLSX, engine='xlsxwriter') as writer:
  
    raw_copy.to_excel(writer, sheet_name='Raw Data', index=False)

    
    cleaned_cols = [
        'PORT_CODE','DATE','Year','IEC','HS_CODE','HSN_DESCRIPTION','MAIN_CATEGORY','SUB_CATEGORY',
        'GOODS_DESCRIPTION','PARSED_MODEL','PARSED_QTY','PARSED_UNIT','PARSED_UNITPRICE_USD','PARSED_CURRENCY',
        'QUANTITY','QUANTITY_FINAL','UNIT','UNIT_PRICE_USD','UNIT_PRICE_USD_FINAL','UNIT_PRICE_INR_COMPUTED',
        'TOTAL_VALUE_INR','DUTY_PAID_INR','GrandTotal_INR','Duty_pct','Duty_per_unit_INR','Duty_Exception','SUPPLIER_IEC'
    ]
    
    cleaned_cols = [c for c in cleaned_cols if c in df.columns]
    df[cleaned_cols].to_excel(writer, sheet_name='Cleaned Data', index=False)

 
    hsn_lookup.to_excel(writer, sheet_name='Lookup Tables', index=False)

    
    year_summary.to_excel(writer, sheet_name='Year Summary', index=False)

   
    hsn_summary.to_excel(writer, sheet_name='HSN Summary', index=False)

 
    top25.to_excel(writer, sheet_name='HSN Top25', index=False)
    if not others_row.empty:
     
        startrow = len(top25) + 3
        others_row.to_excel(writer, sheet_name='HSN Top25', index=False, header=True, startrow=startrow)

    model_summary.to_excel(writer, sheet_name='Model Summary', index=False)

    supplier_summary.to_excel(writer, sheet_name='Supplier Summary', index=False)

 
    if not active_suppliers_latest.empty:
        active_suppliers_latest.to_excel(writer, sheet_name=f'Active Suppliers {latest_year}', index=False)

 
    workbook = writer.book
    charts_ws = workbook.add_worksheet('Charts')
    writer.sheets['Charts'] = charts_ws
    row = 1
    col = 1
    for ch in [chart1, chart2, chart3, chart4]:
        if ch and Path(ch).exists():
            charts_ws.insert_image(row, col, ch, {'x_scale':0.8, 'y_scale':0.8})
            row += 25 

   
    notes = [
        ["Notes & Assumptions"],
        ["Source file:", INPUT_FILENAME],
        ["Parsing rules: GOODS_DESCRIPTION parsed heuristically for QTY, UNIT, UNIT PRICE, MODEL."],
        ["HSN lookup: internal fallback mapping used; expand 'hsn_lookup' list inside the script for better coverage."],
        [f"Average Duty % used for exception flagging: {avg_duty_pct:.4f}"],
        ["GrandTotal_INR calculated as TOTAL_VALUE_INR + DUTY_PAID_INR"],
        ["All numeric aggregates are computed from the 'Cleaned Data' tab outputs."]
    ]
    notes_df = pd.DataFrame(notes)
    notes_df.to_excel(writer, sheet_name='Notes', header=False, index=False)

print("Done. Output workbook written to:", OUTPUT_XLSX)
print("Charts saved in folder:", CHART_DIR)
