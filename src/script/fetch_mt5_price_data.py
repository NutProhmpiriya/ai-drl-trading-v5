import sys
import os
from datetime import datetime
import pandas as pd
import MetaTrader5 as mt5
import pytz

def fetch_and_save_data():
    # Initialize MT5
    if not mt5.initialize():
        print("Failed to initialize MT5")
        return
        
    # Create data directory if it doesn't exist
    data_dir = os.path.join('src', 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    
    # Set timezone
    timezone = pytz.timezone("Etc/UTC")
    
    # Fetch 2023 data
    start_date_2023 = datetime(2023, 1, 1, tzinfo=timezone)
    end_date_2023 = datetime(2023, 12, 31, 23, 59, tzinfo=timezone)
    print("Fetching 2023 data...")
    
    rates_2023 = mt5.copy_rates_range("USDJPY", mt5.TIMEFRAME_M5, start_date_2023, end_date_2023)
    if rates_2023 is not None:
        data_2023 = pd.DataFrame(rates_2023)
        data_2023['time'] = pd.to_datetime(data_2023['time'], unit='s')
        output_file_2023 = os.path.join(data_dir, 'USDJPY_5M_2023.csv')
        data_2023.to_csv(output_file_2023, index=False)
        print(f"Successfully saved 2023 data to {output_file_2023}")
        print(f"Shape of 2023 data: {data_2023.shape}")
    else:
        print("Failed to fetch 2023 data")
    
    # Fetch 2024 data
    start_date_2024 = datetime(2024, 1, 1, tzinfo=timezone)
    end_date_2024 = datetime.now(timezone)
    print("\nFetching 2024 data...")
    
    rates_2024 = mt5.copy_rates_range("USDJPY", mt5.TIMEFRAME_M5, start_date_2024, end_date_2024)
    if rates_2024 is not None:
        data_2024 = pd.DataFrame(rates_2024)
        data_2024['time'] = pd.to_datetime(data_2024['time'], unit='s')
        output_file_2024 = os.path.join(data_dir, 'USDJPY_5M_2024.csv')
        data_2024.to_csv(output_file_2024, index=False)
        print(f"Successfully saved 2024 data to {output_file_2024}")
        print(f"Shape of 2024 data: {data_2024.shape}")
    else:
        print("Failed to fetch 2024 data")
    
    # Shutdown MT5
    mt5.shutdown()

if __name__ == "__main__":
    fetch_and_save_data()
