import os
import time
import hmac
import hashlib
import json
import threading
import concurrent.futures  # <-- Added for concurrent execution
import requests
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from dotenv import load_dotenv

# Delta Exchange client dependencies
from delta_rest_client import DeltaRestClient, OrderType

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
CORS(app)

# Dynamically map accounts using environment variables for both exchanges
ACCOUNTS_DB = {
    "1": {
        "delta_baseurl": os.getenv("ACC1_DELTA_BASEURL", "https://api.india.delta.exchange"),
        "delta_apikey": os.getenv("ACC1_DELTA_KEY"),
        "delta_apisecret": os.getenv("ACC1_DELTA_SEC"),
        "coindcx_apikey": os.getenv("ACC1_COINDCX_KEY"),
        "coindcx_apisecret": os.getenv("ACC1_COINDCX_SEC")
    },
    "2": {
        "delta_baseurl": os.getenv("ACC2_DELTA_BASEURL", "https://api.india.delta.exchange"),
        "delta_apikey": os.getenv("ACC2_DELTA_KEY"),
        "delta_apisecret": os.getenv("ACC2_DELTA_SEC"),
        "coindcx_apikey": os.getenv("ACC2_COINDCX_KEY"),
        "coindcx_apisecret": os.getenv("ACC2_COINDCX_SEC")
    },
    "3": {
        "delta_baseurl": os.getenv("ACC3_DELTA_BASEURL", "https://api.india.delta.exchange"),
        "delta_apikey": os.getenv("ACC3_DELTA_KEY"),
        "delta_apisecret": os.getenv("ACC3_DELTA_SEC"),
        "coindcx_apikey": os.getenv("ACC3_COINDCX_KEY"),
        "coindcx_apisecret": os.getenv("ACC3_COINDCX_SEC")
    }
}

def place_delta_order(delta_baseurl, delta_apikey, delta_apisecret, symbol, size, side, order_type="market_order", price=None, leverage=1):
    """Places an order on Delta Exchange using delta_rest_client."""
    try:
        delta_client = DeltaRestClient(
            base_url=delta_baseurl,
            api_key=delta_apikey,
            api_secret=delta_apisecret
        )
        
        # Format symbol for Delta Exchange
        formatted_symbol = f"{symbol.upper()}USD"
        
        # Get Product ID dynamically
        product = delta_client.get_product(formatted_symbol)
        if not product or "id" not in product:
            return {"error": f"Delta product not found for symbol {formatted_symbol}"}
        
        product_id = product["id"]
        
        # Optional: Set leverage if provided
        try:
            delta_client.set_leverage(product_id, int(leverage))
        except Exception:
            pass # Ignore if leverage is already set or not supported for product type

        # Convert order type to enum
        if order_type.lower() == "market_order":
            ot = OrderType.MARKET
            price = None  # market order doesn't require price
        else:
            ot = OrderType.LIMIT
            if price is None:
                return {"error": "Limit order requires limit_price"}
        
        # Build order request
        params = {
            "product_id": product_id,
            "size": int(size),
            "side": side.lower(),
            "order_type": ot,
        }
        
        if price is not None:
            params["limit_price"] = str(price)

        # Send to Delta Exchange API
        response = delta_client.place_order(**params)
        return response

    except Exception as e:
        return {"error": str(e)}

def place_dcx_order(api_key, api_secret, side, symbol, order_type, price, total_quantity, leverage):
    """Places a futures order on CoinDCX."""
    try:
        secret_bytes = bytes(api_secret, encoding='utf-8')
        timeStamp = int(round(time.time() * 1000))
        
        # Format symbol for CoinDCX
        pair = f"B-{symbol.upper()}_USDT"

        body = {
            "timestamp": timeStamp,
            "order": {
                "side": side.lower(),
                "pair": pair,
                "order_type": order_type.lower(),
                "price": float(price) if price else 0,
                "stop_price": float(price) if price else 0,
                "total_quantity": float(total_quantity),
                "margin_currency_short_name": "INR",
                "leverage": int(leverage),
                "notification": "no_notification",
                "time_in_force": "good_till_cancel",
                "hidden": False,
                "post_only": False,
            }
        }

        json_body = json.dumps(body, separators=(',', ':'))
        signature = hmac.new(secret_bytes, json_body.encode(), hashlib.sha256).hexdigest()
        
        url = "https://api.coindcx.com/exchange/v1/derivatives/futures/orders/create"
        headers = {
            'Content-Type': 'application/json',
            'X-AUTH-APIKEY': api_key,
            'X-AUTH-SIGNATURE': signature
        }

        response = requests.post(url, data=json_body, headers=headers)
        try:
            return response.json()
        except Exception:
            return {"error": "Invalid Response", "raw": response.text}
            
    except Exception as e:
        return {"error": str(e)}

def trading_job(data):
    """Background worker executing batch rounds with a 0.5s delay for Delta Exchange."""
    account_id = str(data.get("account_id"))
    
    if account_id not in ACCOUNTS_DB:
        print(f"Error: Invalid account ID {account_id}")
        return
        
    creds = ACCOUNTS_DB[account_id]
    
    symbol = data.get("symbol")            # e.g., "SIREN"
    side = data.get("side")                # "buy" or "sell"
    order_type = data.get("order_type", "market_order")
    price = data.get("price", None)
    leverage = data.get("leverage", 1)
    
    delta_qty = data.get("delta_quantity")
    coindcx_qty = data.get("coindcx_quantity")
    
    interval = int(data.get("time_interval"))
    batch = int(data.get("batch"))

    for round_num in range(1, batch + 1):
        print(f"--- [Account {account_id}] Starting Round {round_num} of {batch} (0.5s Delta Delay) ---")
        
        # Helper function to handle Delta with a 0.5 second delay
        def delayed_delta_order():
            time.sleep(0.5) # 0.5 second delay
            return place_delta_order(
                delta_baseurl=creds["delta_baseurl"],
                delta_apikey=creds["delta_apikey"],
                delta_apisecret=creds["delta_apisecret"],
                symbol=symbol,
                size=delta_qty,
                side=side,
                order_type=order_type,
                price=price,
                leverage=leverage
            )

        # Use ThreadPoolExecutor to fire CoinDCX immediately and Delta with 0.5s delay
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_coindcx = None
            future_delta = None

            # Submit CoinDCX task immediately if key & qty are available
            if creds["coindcx_apikey"] and creds["coindcx_apisecret"] and coindcx_qty:
                future_coindcx = executor.submit(
                    place_dcx_order,
                    api_key=creds["coindcx_apikey"],
                    api_secret=creds["coindcx_apisecret"],
                    side=side,
                    symbol=symbol,
                    order_type=order_type,
                    price=price,
                    total_quantity=coindcx_qty,
                    leverage=leverage
                )
            
            # Submit Delta Exchange task with the 0.5s delay built-in
            if creds["delta_apikey"] and creds["delta_apisecret"] and delta_qty:
                future_delta = executor.submit(delayed_delta_order)
            
            # Gather responses as they finish
            if future_coindcx:
                ccx_res = future_coindcx.result()
                # print(f"CoinDCX Response (Round {round_num}):", ccx_res)
                
            if future_delta:
                delta_res = future_delta.result()
                # print(f"Delta Response (Round {round_num}):", delta_res)
            
        if round_num < batch:
            time.sleep(interval)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/start-trading', methods=['POST'])
def start_trading():
    req_data = request.json
    
    required_fields = ["account_id", "symbol", "time_interval", "batch", "side"]
    for field in required_fields:
        if field not in req_data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    if str(req_data.get("account_id")) not in ACCOUNTS_DB:
        return jsonify({"error": "Invalid account selection"}), 400

    # Run trading job in a background thread so the HTTP request doesn't block/timeout
    t = threading.Thread(target=trading_job, args=(req_data,))
    t.start()

    return jsonify({
        "status": "success",
        "message": f"Simultaneous trading batch started using Account {req_data.get('account_id')} for {req_data.get('batch')} rounds."
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)