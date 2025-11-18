Trade Analysis Project (2017–2025)
Import/Export Business Intelligence | Python + Excel

This project analyzes import trade data for a company dealing in steel-based household and kitchen products (cutlery, scrubbers, baskets, etc.).
It simulates real-world business intelligence (BI) work, covering trade trends, cost structures, HSN classification, and category-level insights.

Project Overview

The goal of this assignment is to:

✔ Clean and process raw import trade data
✔ Parse complex product descriptions
✔ Map HSN codes to categories
✔ Compute year-wise and HSN-wise insights
✔ Analyze duty structures & unit economics
✔ Create pivot tables and visual dashboards
✔ Generate visual charts using Python

Both Excel and Python are used in the analysis.

Repository Structure
trade_analysis_project/
│
├── process_trade_data.py         # Python script for cleaning & chart generation
├── Sample Data 2.xlsx            # Provided raw trade data (2017–2025)
├── Trade_Analysis_YourName.xlsx  # Final analysis workbook with all sheets
│
└── output_charts/                # Auto-generated charts by Python
        ├── hsn_code_frequency.png
        └── monthly_trend.png

Data Processing (Python)

The process_trade_data.py script performs:

✔ Data Cleaning

Standardizes column names

Converts "DATE" field to proper datetime

Handles missing and inconsistent data

✔ HSN Summary

Groups data by HS CODE

Counts frequency of shipments

✔ Monthly Trends

Aggregates imports month-wise

Generates trend line charts

✔ Chart Generation

Creates visual insights automatically and saves them in output_charts/.

Excel Workbook Highlights

The final Excel workbook includes:

1️ Raw Data Sheet

Exact copy of provided sample

No modifications

2️ Cleaned Data Sheet

Parsed GOODS DESCRIPTION

Extracted:

Model Name

Model Number

Capacity

Qty per item

Unit price (INR & USD)

Computed:

Grand Total (INR)

Duty %

Per-unit duty

3️ Year-wise Summary

Total Value

Duty Paid

Grand Total

YoY Growth %

Trend charts

4️ HSN-wise Summary

Value, Duty, Grand Total

% contribution

Top 25 HSN codes

“Others” grouped

5️ Category Mapping

Main Category (Steel, Metal, etc.)

Sub-category (Scrubber, Basket, Hanger, etc.)

6️ Model & Supplier Analysis

Model-wise quantities

Per unit price comparison

Duty anomalies

Supplier trend analysis

7️ Charts Dashboard

5+ visual charts

HSN distribution

Year-wise trend

Category contribution

 How to Run the Python Script
Prerequisites

Install required libraries:

pip install pandas matplotlib openpyxl

Run Script
python process_trade_data.py


Charts will be saved in:

output_charts/
