import re
import asyncio
import traceback
import aiohttp
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from typing import List, Tuple, Optional
from urllib.parse import urlparse
from src.database.models import Sociedad
from tqdm import tqdm

async def async_validate_url(url: str, session: aiohttp.ClientSession) -> Tuple[int, str]:
    if not url:
        return None, 'Empty URL'
    
    try:
        if not url.startswith(('http://', 'https://')):
            url = f'https://{url}'
        
        async with session.get(url, timeout=5, ssl=False) as response:
            return response.status, response.reason
    except asyncio.TimeoutError:
        return 408, 'Timeout'
    except Exception as e:
        return 500, str(e)

def clean_url(url: str) -> Optional[str]:
    """
    Clean URL by removing protocol prefix and keeping only the www domain part
    Returns None if URL is invalid or doesn't contain www
    """
    if not url or pd.isna(url):
        return None
    
    try:
        # Remove leading/trailing whitespace and convert to lowercase
        url = url.strip().lower()
        
        # Ensure proper protocol prefix for parsing
        if not url.startswith(('http://', 'https://')):
            url = f'http://{url}'
        
        parsed = urlparse(url)
        
        # Ensure netloc exists and is valid
        if not parsed.netloc:
            return None
            
        # Extract domain from netloc
        domain = parsed.netloc
        
        # If domain starts with www, return it as is
        if domain.startswith('www.'):
            return domain
        # If domain doesn't start with www, add it
        elif '.' in domain:  # Ensure it's a valid domain
            return f'www.{domain}'
        
        return None
    except Exception:
        return None

async def process_url_batch(urls_batch: List[Tuple], session: aiohttp.ClientSession) -> List[dict]:
    """
    Process a batch of URLs asynchronously with error handling
    """
    results = []
    
    for cod_infotel, url in urls_batch:
        url_limpia = clean_url(url)
        if url_limpia:
            try:
                status_code, status_mensaje = await async_validate_url(url_limpia, session)
                results.append({
                    'cod_infotel': cod_infotel,
                    'url_limpia': url_limpia,
                    'status_code': status_code,
                    'status_mensaje': status_mensaje
                })
            except Exception as e:
                results.append({
                    'cod_infotel': cod_infotel,
                    'url_limpia': url_limpia,
                    'status_code': 500,
                    'status_mensaje': str(e)
                })
    
    return results

async def process_urls_chunk(chunk: List[Tuple], session: aiohttp.ClientSession) -> List[dict]:
    tasks = []
    for cod_infotel, url in chunk:
        url_limpia = clean_url(url)
        if url_limpia:
            tasks.append(async_validate_url(url_limpia, session))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [
        {
            'cod_infotel': cod_infotel,
            'url_limpia': clean_url(url),
            'status_code': status[0] if isinstance(status, tuple) else 500,
            'status_mensaje': status[1] if isinstance(status, tuple) else str(status)
        }
        for (cod_infotel, url), status in zip(chunk, results)
        if clean_url(url)
    ]

async def async_process_urls(engine, db_name: str, chunk_size: int = 20):
    connector = aiohttp.TCPConnector(limit=100, force_close=True, ssl=False)
    timeout = aiohttp.ClientTimeout(total=10)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        connection = engine.connect()
        
        try:
            query = text(f"SELECT COD_INFOTEL, URL FROM {db_name}.empresas WHERE URL_EXISTS = 1")
            results = list(connection.execute(query))
            total_urls = len(results)
            
            with tqdm(total=total_urls, desc="Processing URLs") as pbar:
                for i in range(0, total_urls, chunk_size):
                    chunk = results[i:i + chunk_size]
                    chunk_results = await process_urls_chunk(chunk, session)
                    
                    if chunk_results:
                        update_query = text(f"""
                            UPDATE {db_name}.empresas 
                            SET URL_LIMPIA = :url_limpia,
                                URL_STATUS = :status_code,
                                URL_STATUS_MENSAJE = :status_mensaje
                            WHERE COD_INFOTEL = :cod_infotel
                        """)
                        connection.execute(update_query, chunk_results)
                        connection.commit()
                    
                    pbar.update(len(chunk))
                    await asyncio.sleep(0.1)  # Evitar sobrecarga
            
        finally:
            connection.close()   
                
def process_urls(engine, db_name: str, df: pd.DataFrame):
    """
    Main function to process URLs
    """
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Insertar registros en lote
        valid_columns = {col.name for col in Sociedad.__table__.columns}
        filtered_records = [
            {k: v for k, v in record.items() if k in valid_columns}
            for record in df.to_dict('records')
        ]
        
        session.bulk_insert_mappings(Sociedad, filtered_records)
        session.commit()
        print(f"Successfully inserted {len(filtered_records)} records.")
        
        # Procesar URLs de forma asíncrona
        asyncio.run(async_process_urls(engine, db_name))
        
    except Exception as e:
        print(f"Error processing URLs: {e}")
        session.rollback()
    finally:
        session.close()