import pandas as pd
from pathlib import Path
from datetime import datetime

def get_latest_raw_file():
    pasta = Path('C:\\Users\\felip\\OneDrive\\Documentos\\Data_Pipeline_Ecommerce\\data\\raw')   
    
    arquivos = list(pasta.glob('*.json'))
    data_recent = max(arquivos, key=lambda f: f.stat().st_mtime)

    return data_recent

def transform_data(data):
    df_bruto = pd.read_json(data) 
    df = df_bruto.copy()

    df = df.drop(columns=['tags','dimensions', 'warrantyInformation',
                        'returnPolicy', 'meta', 'images', 'thumbnail',])

    df.to_parquet(f'C:\\Users\\felip\\OneDrive\\Documentos\\Data_Pipeline_Ecommerce\\data\\trusted\\products_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.parquet', index=False)
    return df

if __name__== "__main__":
    transform_data(get_latest_raw_file())