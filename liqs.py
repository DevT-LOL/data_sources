import asyncio
import json
import os
import sys
import traceback
import time
from datetime import datetime
import pytz
from websockets import connect, ConnectionClosed
from termcolor import cprint

# WebSocket URL for Binance liquidation data
websocket_url = 'wss://fstream.binance.com/ws/!forceOrder@arr'

# List of tokens you want to filter for (add your preferred tokens here)
TOKENS_TO_WATCH = ['BTC', 'ETH', 'SOL', 'XRP', 'AVAX', 'BNB']

# Create a dedicated data directory for all files
DATA_DIR = 'binance_liquidation_data'
os.makedirs(DATA_DIR, exist_ok=True)

# Create directory for token-specific files
TOKEN_DIR = os.path.join(DATA_DIR, 'token_liquidations')
os.makedirs(TOKEN_DIR, exist_ok=True)

# Main CSV file for all liquidations
main_file = os.path.join(DATA_DIR, 'binance_all_liquidations.csv')

# Create main file if it doesn't exist
if not os.path.isfile(main_file):
    with open(main_file, 'w') as f:
        f.write(",".join([
            'symbol', 'side', 'order_type', 'time_in_force',
            'original_quantity', 'price', 'average_price', 'order_status',
            'order_last_filled_quantity', 'order_filled_accumulated_quantity',
            'order_trade_time', 'usd_size'
        ]) + "\n")

# Create separate files for each token
for token in TOKENS_TO_WATCH:
    token_file = os.path.join(TOKEN_DIR, f'{token}_liquidations.csv')
    if not os.path.isfile(token_file):
        with open(token_file, 'w') as f:
            f.write(",".join([
                'symbol', 'side', 'order_type', 'time_in_force',
                'original_quantity', 'price', 'average_price', 'order_status',
                'order_last_filled_quantity', 'order_filled_accumulated_quantity',
                'order_trade_time', 'usd_size', 'time_est'
            ]) + "\n")

# Log file for tracking restarts and errors
log_file = os.path.join(DATA_DIR, 'binance_bot_log.txt')

def log_message(message):
    """Write message to log file and print to console"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    
    with open(log_file, 'a') as f:
        f.write(log_entry)
    
    cprint(message, 'cyan')

async def process_liquidation(order_data):
    """Process a single liquidation event"""
    # Extract symbol and remove USDT suffix
    full_symbol = order_data['s']
    symbol = full_symbol.replace('USDT', '')
    
    # Check if this token is in our watch list
    base_token = None
    for token in TOKENS_TO_WATCH:
        if symbol.startswith(token):
            base_token = token
            break
    
    # Skip if not in our watch list
    if base_token is None:
        return
    
    side = order_data['S']
    timestamp = int(order_data['T'])
    
    # Handle potential missing or invalid data
    try:
        filled_quantity = float(order_data['z'])
        price = float(order_data['p'])
        usd_size = filled_quantity * price
    except (KeyError, ValueError):
        log_message(f"Warning: Invalid data in liquidation for {symbol}")
        return
    
    if usd_size < 3000:
        return

    # Format timestamp
    est = pytz.timezone('US/Eastern')
    time_est = datetime.fromtimestamp(timestamp / 1000, est).strftime('%H:%M:%S')
    
    # Print significant liquidations to console

    liquidation_type = "L LIQ" if side == "SELL" else 'S LIQ'
    output = f"{liquidation_type} {base_token} {time_est} ${usd_size:.0f}"
    color = 'green' if side == "SELL" else 'red'
    attrs = ['bold'] if usd_size > 10000 else []
    
    cprint(output, 'white', f'on_{color}', attrs=attrs)
    print('')
    
    # Prepare data for CSV writing
    msg_values = [
        full_symbol,
        order_data.get('S', ''),
        order_data.get('o', ''),
        order_data.get('f', ''),
        order_data.get('q', ''),
        order_data.get('p', ''),
        order_data.get('ap', ''),
        order_data.get('X', ''),
        order_data.get('l', ''),
        order_data.get('z', ''),
        order_data.get('T', ''),
        str(usd_size)
    ]
    
    # Write to main file
    try:
        with open(main_file, 'a') as f:
            trade_info = ','.join(map(str, msg_values)) + '\n'
            f.write(trade_info)
    except Exception as e:
        log_message(f"Error writing to main file: {str(e)}")
    
    # Write to token-specific file
    try:
        token_file = os.path.join(TOKEN_DIR, f'{base_token}_liquidations.csv')
        with open(token_file, 'a') as f:
            # Add the formatted time for token-specific files
            token_msg_values = msg_values + [time_est]
            trade_info = ','.join(map(str, token_msg_values)) + '\n'
            f.write(trade_info)
    except Exception as e:
        log_message(f"Error writing to token file: {str(e)}")

async def binance_liquidation(uri):
    """Connect to Binance websocket and process liquidation events"""
    backoff = 1  # Initial backoff time in seconds
    max_backoff = 60  # Maximum backoff time
    
    while True:
        try:
            log_message(f"Connecting to Binance WebSocket: {uri}")
            async with connect(uri) as websocket:
                log_message(f"WebSocket connected successfully. Filtering for tokens: {', '.join(TOKENS_TO_WATCH)}")
                backoff = 1  # Reset backoff on successful connection
                
                while True:
                    try:
                        msg = await websocket.recv()
                        data = json.loads(msg)
                        if 'o' in data:
                            await process_liquidation(data['o'])
                    except ConnectionClosed:
                        log_message("WebSocket connection closed unexpectedly. Reconnecting...")
                        break
                    except json.JSONDecodeError:
                        log_message(f"Error decoding JSON: {msg[:100]}...")
                        continue
                    except Exception as e:
                        log_message(f"Error processing message: {str(e)}")
                        continue
        
        except Exception as e:
            log_message(f"Connection error: {str(e)}")
            log_message(f"Reconnecting in {backoff} seconds...")
            
            # Implement exponential backoff with jitter
            await asyncio.sleep(backoff + (0.1 * backoff * (asyncio.get_event_loop().time() % 1)))
            backoff = min(backoff * 2, max_backoff)  # Double backoff time, but cap it

async def heartbeat():
    """Send heartbeat signals to log file to confirm the bot is still running"""
    while True:
        try:
            log_message("Heartbeat: Bot is running")
            await asyncio.sleep(3600)  # Heartbeat every hour
        except Exception as e:
            log_message(f"Heartbeat error: {str(e)}")
            await asyncio.sleep(60)

async def main():
    """Main function to run the bot with auto-restart capabilities"""
    log_message("Starting Binance Liquidation Bot")
    
    try:
        # Run heartbeat and main tasks concurrently
        await asyncio.gather(
            heartbeat(),
            binance_liquidation(websocket_url)
        )
    except Exception as e:
        log_message(f"Critical error in main loop: {str(e)}")
        log_message("Stack trace: " + traceback.format_exc())
        log_message("Restarting entire bot in 5 seconds...")
        await asyncio.sleep(5)
        # Instead of restarting here, we'll let the outer loop handle it

if __name__ == "__main__":
    # Outer loop to ensure the bot always restarts
    while True:
        try:
            # Check for keyboard interrupt cleanly
            asyncio.run(main())
        except KeyboardInterrupt:
            log_message("Script terminated by keyboard interrupt. Exiting...")
            sys.exit(0)
        except Exception as e:
            log_message(f"Fatal error in bot: {str(e)}")
            log_message("Stack trace: " + traceback.format_exc())
            log_message("Restarting bot in 10 seconds...")
            time.sleep(10)