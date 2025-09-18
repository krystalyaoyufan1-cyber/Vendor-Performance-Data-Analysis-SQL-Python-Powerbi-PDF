import sqlite3
import pandas as pd 
import logging
import ingestion_db
from ingestion_db import ingest_db


logging.basicConfig(
    filename="log/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode='w'
)

def creat_vendor_summary(conn):
    '''This function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data '''
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS 
    SELECT
        VendorNumber,
        SUM(FreightCost) AS TotalFreightCost,
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS(
            SELECT
            p.VendorNumber,
            p.VendorName,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity,
            ss.TotalSalesDollars,
            ss.TotalExciseTax,
            fs.FreightCost
            FROM PurchaseSummary ps
            LEFT JOIN SalesSummary ss ON ps.VendorNumber = ss.VendorNumber
            AND ps.Brand = ss.Brand
            LEFT JOIN FreightSummary fs
            ON ps.VendorNumber = fs.VendorNumber
            ORDER BY ps.TotalPurchaseDollars DESC""", conn),

    return vendor_sales_summary


def clean_data(df):
    '''This function will clean the data '''
    # changing datatype to float
    df['Volume'] = df['Volume'].astype('float')
    
    # filling missing value with 0
    df.fillna(0,inplace = True)

    # removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    # creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    return df

if __name__ == "__main__":
    # creating database connection
    conn = sqlite3.connect('vendor_sales.db')

    logging.info('Creating Vendor Summary Table....')
    summary_df = creat_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning Data....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    ingest_db(clean_df, conn)
    logging.info('Completed')