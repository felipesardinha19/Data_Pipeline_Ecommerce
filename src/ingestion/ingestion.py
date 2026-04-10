import json
import requests_cache
from retry_requests import retry
from datetime import datetime

# criar sessão com cache e retry
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

url = 'https://dummyjson.com/products'
def fetch_data():
    try:
        url = 'https://dummyjson.com/products'
        response = retry_session.get(url)
        response.raise_for_status()  # Verificar se a resposta foi bem-sucedida
        data = response.json()
        print("Dados obtidos com sucesso!")

        return data['products']  # Retornar apenas a lista de produtos
    
    except requests_cache.exceptions.CacheError as e:
        print(f"Erro de cache: {e}")

def save_raw_data(data):
    file_name = f"products_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"

    file_path = f"data/raw/{file_name}"

    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Dados salvos em {file_path}")

save_raw_data(fetch_data())