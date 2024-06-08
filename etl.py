import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
import logging

def extract_bank_data(url):
    # Function to extract data from the given URL
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the table under the heading "By Market Capitalization"
    table = soup.find('table', {'class': 'wikitable'})
    
    # Parse the table into a DataFrame
    df = pd.read_html(str(table))[0]
    df.columns = ['Rank', 'Name', 'Market_Cap_Billion_USD']
    return df

def transform_data(df, exchange_rate_file):
    # Function to transform data by adding columns for GBP, EUR, and INR
    # Load currency exchange rates from CSV
    exchange_rates = pd.read_csv(exchange_rate_file)
    usd_to_gbp = exchange_rates.loc[exchange_rates['Currency'] == 'GBP', 'Rate'].values[0]
    usd_to_eur = exchange_rates.loc[exchange_rates['Currency'] == 'EUR', 'Rate'].values[0]
    usd_to_inr = exchange_rates.loc[exchange_rates['Currency'] == 'INR', 'Rate'].values[0]
    
    # Calculate market capitalization in GBP, EUR, and INR
    df['Market_Cap_Billion_GBP'] = (df['Market_Cap_Billion_USD'] * usd_to_gbp).round(2)
    df['Market_Cap_Billion_EUR'] = (df['Market_Cap_Billion_USD'] * usd_to_eur).round(2)
    df['Market_Cap_Billion_INR'] = (df['Market_Cap_Billion_USD'] * usd_to_inr).round(2)
    
    return df

def save_to_csv(df, output_file):
    # Function to save DataFrame to a CSV file
    df.to_csv(output_file, index=False)

def save_to_db(df, db_file):
    # Function to save DataFrame to an SQL database
    conn = sqlite3.connect(db_file)
    df.to_sql('top_10_banks', conn, if_exists='replace', index=False)
    conn.close()

def run_query(db_file, query):
    # Function to run queries on the database table
    conn = sqlite3.connect(db_file)
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result

def execute_custom_query(db_file, query):
    # Function to execute custom query on the database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result

def setup_logging(log_file):
    # Function to set up logging
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def log(message):
    # Function to log messages
    logging.info(message)

def main():
    # Main function to orchestrate the ETL process
    url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
    exchange_rate_file = 'exchange_rates.csv'  # Specify full path if the file is in a different directory
    output_file = 'top_10_banks.csv'
    db_file = 'top_10_banks.db'
    log_file = 'data_processing.log'

    # Set up logging
    setup_logging(log_file)

    # Extract data
    log('Data extraction started')
    bank_df = extract_bank_data(url)
    log('Data extraction completed')

    # Transform data
    log('Data transformation started')
    transformed_df = transform_data(bank_df, exchange_rate_file)
    log('Data transformation completed')

    # Save to CSV
    log('Saving to CSV started')
    save_to_csv(transformed_df, output_file)
    log('Saving to CSV completed')

    # Save to DB
    log('Saving to DB started')
    save_to_db(transformed_df, db_file)
    log('Saving to DB completed')

    # Run queries
    log('Running queries started')
    query_london = "SELECT Name, Market_Cap_Billion_GBP FROM top_10_banks"
    query_berlin = "SELECT Name, Market_Cap_Billion_EUR FROM top_10_banks"
    query_delhi = "SELECT Name, Market_Cap_Billion_INR FROM top_10_banks"

    log(run_query(db_file, query_london).to_string())
    log(run_query(db_file, query_berlin).to_string())
    log(run_query(db_file, query_delhi).to_string())
    log('Running queries completed')

    # Example of executing a custom query
    log('Running custom query')
    custom_query = "SELECT Name, Market_Cap_Billion_USD FROM top_10_banks WHERE Market_Cap_Billion_USD > 100"
    custom_query_result = execute_custom_query(db_file, custom_query)
    log(f'Custom query result: {custom_query_result}')

if __name__ == "__main__":
    main()
