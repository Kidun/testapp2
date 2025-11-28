"""
Data collection functions from 3 different sources
"""
import requests
import time
import random
from typing import Dict, Tuple
from datetime import datetime


def collect_weather_data() -> Tuple[Dict, float, bool]:
    """
    Source 1: Collect weather data
    Returns: (data_dict, response_time, success)
    """
    start_time = time.time()
    try:
        # Using Open-Meteo API (free, no API key required)
        # Moscow coordinates: 55.7558, 37.6173
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 55.7558,
            "longitude": 37.6173,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m"
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        current = data.get('current', {})

        result = {
            'temperature': current.get('temperature_2m', None),
            'humidity': current.get('relative_humidity_2m', None),
            'wind_speed': current.get('wind_speed_10m', None)
        }

        response_time = time.time() - start_time
        return result, response_time, True

    except Exception as e:
        print(f"Error collecting weather data: {e}")
        response_time = time.time() - start_time
        return {
            'temperature': None,
            'humidity': None,
            'wind_speed': None
        }, response_time, False


def collect_currency_rates() -> Tuple[Dict, float, bool]:
    """
    Source 2: Collect currency exchange rates
    Returns: (data_dict, response_time, success)
    """
    start_time = time.time()
    try:
        # Using exchangerate-api.com (free tier available)
        url = "https://api.exchangerate-api.com/v4/latest/RUB"

        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()
        rates = data.get('rates', {})

        # Convert to rates against RUB (invert the rates)
        result = {
            'usd_rate': 1 / rates.get('USD', 0.01) if rates.get('USD') else None,
            'eur_rate': 1 / rates.get('EUR', 0.01) if rates.get('EUR') else None,
            'gbp_rate': 1 / rates.get('GBP', 0.01) if rates.get('GBP') else None,
            'cny_rate': 1 / rates.get('CNY', 0.01) if rates.get('CNY') else None
        }

        response_time = time.time() - start_time
        return result, response_time, True

    except Exception as e:
        print(f"Error collecting currency rates: {e}")
        response_time = time.time() - start_time
        return {
            'usd_rate': None,
            'eur_rate': None,
            'gbp_rate': None,
            'cny_rate': None
        }, response_time, False


def collect_crypto_prices() -> Tuple[Dict, float, bool]:
    """
    Source 3: Collect cryptocurrency and stock market data
    Returns: (data_dict, response_time, success)
    """
    start_time = time.time()
    try:
        # Using CoinGecko API (free, no API key required)
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            'ids': 'bitcoin,ethereum',
            'vs_currencies': 'usd'
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        # Generate a mock stock index value (since real APIs require keys)
        # In production, use real API like Alpha Vantage or Yahoo Finance
        stock_index = random.uniform(3800, 4200)  # Mock S&P 500 range

        result = {
            'btc_price': data.get('bitcoin', {}).get('usd', None),
            'eth_price': data.get('ethereum', {}).get('usd', None),
            'stock_index': stock_index
        }

        response_time = time.time() - start_time
        return result, response_time, True

    except Exception as e:
        print(f"Error collecting crypto prices: {e}")
        response_time = time.time() - start_time
        return {
            'btc_price': None,
            'eth_price': None,
            'stock_index': None
        }, response_time, False


def collect_all_data() -> Dict:
    """
    Collect data from all sources and aggregate results
    This function can be called in parallel tasks
    """
    weather_data, weather_time, weather_success = collect_weather_data()
    currency_data, currency_time, currency_success = collect_currency_rates()
    crypto_data, crypto_time, crypto_success = collect_crypto_prices()

    # Count failed requests
    failed_requests = sum([
        not weather_success,
        not currency_success,
        not crypto_success
    ])

    # Combine all data
    combined_data = {
        'parse_time': datetime.utcnow(),
        **weather_data,
        **currency_data,
        **crypto_data,
        'failed_requests': failed_requests,
        'source1_response_time': weather_time,
        'source2_response_time': currency_time,
        'source3_response_time': crypto_time,
        'source1_success': weather_success,
        'source2_success': currency_success,
        'source3_success': crypto_success
    }

    return combined_data


if __name__ == '__main__':
    # Test data collection
    print("Testing data collection...")
    data = collect_all_data()
    print("Collected data:")
    for key, value in data.items():
        print(f"  {key}: {value}")
