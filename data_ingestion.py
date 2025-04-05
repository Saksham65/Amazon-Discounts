# data_ingestion.py
import os
import requests
import json
from dotenv import load_dotenv
from urllib.parse import urlencode

load_dotenv()

api_key = os.environ.get("RAINFOREST_API_KEY", "")
base_url = os.environ.get("RAINFOREST_BASE_URL", "https://api.rainforestapi.com/request")

base_params = {
    "api_key": api_key,
    "type": "deals",
    "amazon_domain": "amazon.in"
}

def get_url(params):
    query_parameters = {**base_params, **params}
    encoded_parameters = urlencode(query_parameters)
    return f"{base_url}?{encoded_parameters}"

def send_request(data_dir, params):
    response = requests.get(get_url(params))

    if response.status_code == 200:
        data = response.json()
        deals_results = data.get('deals_results', [])

        os.makedirs(data_dir, exist_ok=True)
        with open(os.path.join(data_dir, "rainforest_discounts.jsonl"), 'w') as file:
            for deal in deals_results:
                deal['deal_price'] = deal.get('deal_price', {}).get('value', '')
                deal['old_price'] = deal.get('list_price', {}).get('value', '')
                deal['currency'] = deal.get('list_price', {}).get('currency', '')
                doc_object = {"doc": str(deal)}
                file.write(json.dumps(doc_object) + '\n')
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")

if __name__ == "__main__":
    params = {
        "category_id": "2619525011"  # Specify the category ID you need
        # ... any other params
    }
    data_dir = "./data"

    send_request(data_dir, params)
