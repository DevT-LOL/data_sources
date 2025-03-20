import asyncio
import json
import csv
import os
import time
from datetime import datetime, timedelta, timezone
from websockets import connect, ConnectionClosed
from termcolor import cprint
import logging
import threading
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# List of symbols to monitor
symbols = ['btcusdt', 'ethusdt', 'solusdt', 'xrpusdt', 'bnbusdt', 'dogeusdt']
# Create display names by removing 'usdt' from each symbol and capitalizing
display_names = [symbol.replace('usdt', '').upper() for symbol in symbols]
websocket_url_base = 'wss://fstream.binance.com/ws/'

# Shared resources
data_lock = asyncio.Lock()
csv_lock = asyncio.Lock()

# Store the latest funding rates for all symbols
# This will be continuously updated by the WebSocket streams
current_rates = {
    name: None for name in display_names
}

# CSV file configuration
CSV_FILENAME = 'funding_rates.csv'

# Data collection control
COLLECTION_INTERVAL = 60  # seconds

async def setup_csv_file():
    """Initialize the CSV file with headers if it doesn't exist."""
    headers = ['Time (UTC)'] + display_names
    
    # Check if file exists and create with headers if it doesn't
    if not os.path.exists(CSV_FILENAME):
        async with csv_lock:
            with open(CSV_FILENAME, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                logger.info(f"Created new CSV file: {CSV_FILENAME} with UTC timestamps")

async def write_to_csv(timestamp, rates):
    """Write the current funding rates to the CSV file."""
    # Skip if we don't have complete data yet
    if None in rates.values():
        logger.warning("Incomplete data, skipping CSV write")
        return
        
    async with csv_lock:
        with open(CSV_FILENAME, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Create a row with time and all funding rates
            row = [
                timestamp,
                *[f"{rates[name]:.2f}%" for name in display_names]
            ]
            writer.writerow(row)
            logger.info(f"Wrote to CSV: {row}")

async def continuous_funding_stream(symbol, display_name):
    """
    Continuously monitor the funding rate for a specific symbol.
    This function runs independently of the collection timer
    and just keeps the current_rates dictionary updated.
    
    Args:
        symbol (str): The trading pair symbol (e.g., 'btcusdt')
        display_name (str): The display name for the symbol (e.g., 'BTC')
    """
    websocket_url = f'{websocket_url_base}{symbol}@markPrice'
    
    while True:
        try:
            logger.info(f"Connecting to {websocket_url}")
            async with connect(websocket_url) as websocket:
                while True:
                    # Continuously receive data from WebSocket
                    message = await websocket.recv()
                    data = json.loads(message)
                    
                    # Update the current rates dictionary with this new data
                    funding_rate = float(data['r'])
                    yearly_funding_rate = (funding_rate * 3 * 365) * 100
                    
                    async with data_lock:
                        current_rates[display_name] = yearly_funding_rate
                        
                        # Log the update for debugging (optional)
                        logger.debug(f"Updated {display_name} rate to {yearly_funding_rate:.2f}%")
                    
        except ConnectionClosed as e:
            logger.error(f"Connection closed for {symbol}: {e}")
            await asyncio.sleep(5)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for {symbol}: {e}")
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Unexpected error for {symbol}: {e}")
            await asyncio.sleep(5)

async def scheduled_data_collection():
    """
    Timer that collects and records data at precisely the start of each minute.
    This function runs separately from the WebSocket streams and just takes
    snapshots of the current_rates at regular intervals.
    """
    while True:
        # Calculate time until the next minute starts (at :00 seconds) using UTC
        now = datetime.now(timezone.utc)
        # Calculate seconds until the next minute
        seconds_to_next_minute = 60 - now.second
        
        if seconds_to_next_minute == 60:  # We're exactly at :00
            seconds_to_next_minute = 0
            
        # Account for microseconds too, for precision
        microseconds_to_wait = seconds_to_next_minute * 1_000_000 - now.microsecond
        wait_time = microseconds_to_wait / 1_000_000  # Convert back to seconds (float)
        
        logger.info(f"Waiting {wait_time:.2f} seconds until next data collection at xx:xx:00 UTC")
        await asyncio.sleep(wait_time)
        
        # Get the current minute for this collection cycle (xx:xx:00 format)
        collection_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        collection_time_str = collection_time.strftime('%H:%M:%S')
        
        # Take a snapshot of the current rates under the lock
        snapshot = {}
        async with data_lock:
            snapshot = current_rates.copy()
            
            # Print the current rates to the console with color coding
            for name, rate in snapshot.items():
                if rate is not None:
                    # Determine display colors based on funding rate
                    if rate > 50:
                        text_color, back_color = 'black', 'on_red'
                    elif rate > 30:
                        text_color, back_color = 'black', 'on_yellow'
                    elif rate > 5:
                        text_color, back_color = 'black', 'on_cyan'
                    elif rate < -10:
                        text_color, back_color = 'black', 'on_green'
                    else:
                        text_color, back_color = 'black', 'on_light_green'
                    
                    # Display the funding rate
                    cprint(f'{name} funding: {rate:.2f}%', text_color, back_color)
        
        # Write the snapshot to CSV
        await write_to_csv(collection_time_str, snapshot)
        
        # Wait until near the next minute to avoid triggering multiple times within the same minute
        # We're aiming for exactly 60 seconds between collections, but leaving a buffer
        await asyncio.sleep(55)

async def main():
    """
    Main function to start monitoring funding rates for all symbols.
    """
    logger.info("Starting Binance funding rate monitor")
    
    # Initialize the CSV file
    await setup_csv_file()
    
    # Create background tasks for continuous WebSocket streams
    websocket_tasks = [continuous_funding_stream(symbol, display_name) 
                       for symbol, display_name in zip(symbols, display_names)]
    
    # Create the scheduled collection task
    collection_task = scheduled_data_collection()
    
    # Run all tasks concurrently
    await asyncio.gather(collection_task, *websocket_tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program terminated by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")