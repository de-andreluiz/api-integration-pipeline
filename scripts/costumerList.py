import requests
import json
import logging
import time
import os
import re
import pyodbc
import warnings
from datetime import datetime
from dotenv import load_dotenv

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

load_dotenv()

server = os.getenv('server')
database = os.getenv('database')
username = os.getenv('username')
password = os.getenv('password')
COMPLEO_API_TOKEN = os.getenv('COMPLEO_API_TOKEN')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler()])

table_name = 'api_listaClientes'
url = "https://api.compleo.app/public/customerlist/100056"

body_template = {
  "fields": [
    "id",
    "code",
    "name"
  ],
  "sort": {
    "field": "lastUpdatedAt",
    "order": "desc"
  },
  "pagination": {
    "currentPage": 1,
    "pageSize": 50
  }
}

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {COMPLEO_API_TOKEN}"
}

def clean_value(value):
    if not value:
        return ''
    clean_text = re.sub(r'<[^>]+>', '', str(value)).strip()
    return clean_text

def save_to_database(data, table_name, cursor, is_first_page=True, processed_ids=None):
    try:
        if processed_ids is None:
            processed_ids = set()
        
        if is_first_page:
            try:
                drop_table_query = f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
                cursor.execute(drop_table_query)
                cursor.commit()
                logging.info(f"Tabela {table_name} descartada para recriação")
                
                create_table_query = f"""
                CREATE TABLE {table_name} (
                    id NVARCHAR(100),
                    codigoDoCliente NVARCHAR(100),
                    nomeDoDepartamento NVARCHAR(500)
                )
                """
                cursor.execute(create_table_query)
                cursor.commit()
                logging.info(f"Tabela {table_name} criada com sucesso")
                
                try:
                    index_query = f"CREATE INDEX idx_customer_id ON {table_name} (id)"
                    cursor.execute(index_query)
                    cursor.commit()
                    logging.info(f"Índice criado para a coluna id")
                except Exception as e:
                    logging.warning(f"Erro ao criar índice: {e}")
                
                try:
                    cursor.execute(f"SELECT TOP 0 * FROM {table_name}")
                    columns = [column[0] for column in cursor.description]
                    logging.info(f"Colunas da nova tabela: {columns}")
                except Exception as e:
                    logging.warning(f"Não foi possível verificar estrutura da tabela: {e}")
            except Exception as e:
                logging.warning(f"Erro ao configurar tabela: {e}")
        
        customers = []
        
        if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
            customers = data['data']
            total_records = data.get('totalFiltered', 0) or data.get('total', 0)
            current_page = data.get('pagination', {}).get('currentPage', 1)

        elif isinstance(data, list):
            customers = data
            logging.info(f"Lista direta de {len(customers)} clientes recebida")
        else:
            logging.error(f"Formato de dados inesperado: {type(data)}")
            return False
            
        count_inserted = 0
        count_skipped = 0

        for customer in customers:
            def safe_value(val):
                if val is None:
                    return None
                if isinstance(val, dict):
                    return json.dumps(val, ensure_ascii=False)[:500]
                return str(val)[:500]
            
            customer_id = safe_value(customer.get('id'))
            
            if customer_id in processed_ids:
                count_skipped += 1
                continue
                
            try:
                check_query = f"SELECT COUNT(*) FROM {table_name} WHERE id = ?"
                cursor.execute(check_query, (customer_id,))
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    count_skipped += 1
                    continue
            except Exception as e:
                logging.warning(f"Erro ao verificar duplicidade para cliente {customer_id}: {e}")
            
            insert_query = f"""
            INSERT INTO {table_name} (
                id,
                codigoDoCliente,
                nomeDoDepartamento
            ) VALUES (?, ?, ?)
            """
            
            try:
                cursor.execute(insert_query, (
                    customer_id,
                    safe_value(customer.get('code')),
                    safe_value(customer.get('name'))
                ))
                
                processed_ids.add(customer_id)
                
                count_inserted += 1
                
                if count_inserted % 10 == 0:
                    cursor.commit()
            except Exception as e:
                logging.error(f"Erro ao inserir cliente {customer_id}: {e}")
                try:
                    logging.error(f"Cliente problemático: {json.dumps(customer, ensure_ascii=False)[:500]}")
                except:
                    logging.error("Não foi possível serializar o cliente problemático")
                continue
            
        cursor.commit()
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]

        except Exception as e:
            logging.warning(f"Não foi possível obter contagem total: {e}")

        return True
    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        logging.error(f"Erro ao salvar dados de clientes: {error_type} - {error_message}")
        try:
            cursor.rollback()
        except:
            logging.error("Não foi possível fazer rollback")
        return False

def make_api_request_with_retry(session, url, body, max_retries=3, retry_delay=3):
    if 'pagination' in body and 'offset' in body['pagination']:
        del body['pagination']['offset']
    
    current_page = body.get('pagination', {}).get('currentPage', 1)
    logging.info(f"Solicitando página {current_page} com body: {json.dumps(body['pagination'])}")
    logging.debug(f"[DIAGNÓSTICO] URL da requisição: {url}")
    logging.debug(f"[DIAGNÓSTICO] Body completo: {json.dumps(body)}")
    
    for attempt in range(1, max_retries + 1):
        try:
            logging.debug(f"[DIAGNÓSTICO] Tentativa {attempt} - Iniciando requisição HTTP")
            response = session.post(url, json=body, headers=headers, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, dict) and 'pagination' in data:
                    returned_page = data.get('pagination', {}).get('currentPage', 0)
                    if returned_page != current_page:
                        logging.warning(f"API retornou página {returned_page} quando solicitamos página {current_page}")
                
                return True, data
            elif response.status_code == 429:
                wait_time = retry_delay * attempt * 2
                logging.warning(f"Rate limit atingido. Aguardando {wait_time} segundos antes de tentar novamente.")
                time.sleep(wait_time)
            else:
                logging.error(f"Erro na requisição: Status {response.status_code} - Tentativa {attempt}/{max_retries}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    return False, f"Falha após {max_retries} tentativas. Último status: {response.status_code}"
        except requests.RequestException as e:
            logging.error(f"Exceção na requisição: {e} - Tentativa {attempt}/{max_retries}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                return False, f"Falha após {max_retries} tentativas. Exceção: {str(e)}"
    
    return False, f"Falha após {max_retries} tentativas"

def process_clientes(session, cursor, json_dir, processed_ids):
    logging.info("Iniciando processamento de clientes")
    
    request_body = body_template.copy()
    request_body["pagination"]["currentPage"] = 1
    request_body["pagination"]["pageSize"] = 50
    
    success, data = make_api_request_with_retry(session, url, request_body)
    
    total_processed = 0
    current_page = 1
    max_pages = 1000
    
    if success:
        total_records = data.get('totalFiltered', 0) or data.get('total', 0)
        page_size = data.get('pageSize', 50) or 50
        total_pages = (total_records + page_size - 1) // page_size
        
        logging.info(f"Total de {total_records} clientes, {total_pages} páginas")
        
        output_path = os.path.join(json_dir, f"{table_name}_page{current_page}.json")
        with open(output_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        
        if not save_to_database(data, table_name, cursor, is_first_page=True, processed_ids=processed_ids):
            return 0
        
        customers_page1 = data.get('data', [])
        customers_count = len(customers_page1)
        for customer in customers_page1:
            if customer.get('id'):
                total_processed += 1
        

        
        has_more_pages = customers_count > 0 and current_page < total_pages
        
        while has_more_pages and current_page < max_pages:
            current_page += 1
            request_body["pagination"]["currentPage"] = current_page
            
            success, data = make_api_request_with_retry(session, url, request_body)
            
            if success:
                output_path = os.path.join(json_dir, f"{table_name}_page{current_page}.json")
                with open(output_path, "w", encoding="utf-8") as json_file:
                    json.dump(data, json_file, ensure_ascii=False, indent=4)
                
                if save_to_database(data, table_name, cursor, is_first_page=False, processed_ids=processed_ids):
                    customers = data.get('data', [])
                    customers_count = len(customers)
                    
                    page_processed = 0
                    for customer in customers:
                        if customer.get('id'):
                            page_processed += 1
                    
                    total_processed += page_processed

                    
                    has_more_pages = customers_count > 0 and current_page < total_pages
                else:
                    logging.error(f"Erro ao salvar dados da página {current_page}")
                    break
            else:
                logging.error(f"Erro na requisição da página {current_page}: {data}")
                break
            
            time.sleep(0.5)
        
        try:
            cursor.execute(f"""
                SELECT id, COUNT(*) as count 
                FROM {table_name} 
                GROUP BY id 
                HAVING COUNT(*) > 1
            """)
            duplicates = cursor.fetchall()
            duplicates_count = len(duplicates)
            
            if duplicates_count > 0:
                logging.warning(f"Verificação final: {duplicates_count} IDs de clientes duplicados encontrados")
                logging.info(f"Removendo duplicidades para {duplicates_count} IDs")
                for dup_id, count in duplicates:
                    try:
                        cursor.execute(f"""
                            WITH cte AS (
                                SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY (SELECT 1)) as rn
                                FROM {table_name}
                                WHERE id = ?
                            )
                            DELETE FROM cte WHERE rn > 1
                        """, (dup_id,))
                        cursor.commit()
                    except Exception as e:
                        logging.error(f"Erro ao remover duplicidade para ID {dup_id}: {e}")
        except Exception as e:
            logging.error(f"Erro ao verificar duplicidades: {e}")
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT COUNT(DISTINCT id) FROM {table_name}")
            unique_codes = cursor.fetchone()[0]
            
            logging.info(f"Processamento de clientes concluído: {total_count} registros inseridos ({unique_codes} IDs únicos) em {current_page} páginas")
            
            return total_count
        except Exception as e:
            logging.error(f"Erro ao gerar relatório final: {e}")
            return total_processed
    else:
        error_message = f"Erro na requisição inicial de clientes: {data}"
        logging.error(error_message)
        return 0



def process_clientes_compleo():
    start_time = time.time()
    start_datetime = datetime.now()
    total_processed = 0
    logging.info(f"Início da execução: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        json_dir = os.path.join(os.path.dirname(script_dir), "Jsons", "listaClientes")
        
        os.makedirs(json_dir, exist_ok=True)
        logging.info(f"Diretório para arquivos JSON: {json_dir}")
        
        clean_json_files(json_dir)
        
        processed_ids = set()
        
        with requests.Session() as session:
            conn_str = (
                f'DRIVER={{ODBC Driver 18 for SQL Server}};'
                f'SERVER={server};'
                f'DATABASE={database};'
                f'UID={username};'
                f'PWD={password};'
                f'TrustServerCertificate=yes;'
            )
            
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            total_processed = process_clientes(
                session=session,
                cursor=cursor,
                json_dir=json_dir,
                processed_ids=processed_ids
            )
            
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                total_count = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT COUNT(DISTINCT id) FROM {table_name}")
                unique_codes = cursor.fetchone()[0]
                
                cursor.execute(f"""
                    SELECT id, COUNT(*) as count 
                    FROM {table_name} 
                    GROUP BY id 
                    HAVING COUNT(*) > 1
                """)
                duplicates = cursor.fetchall()
                
                duplicate_info = ""
                if duplicates:
                    duplicate_info = f"\nATENÇÃO: Encontrados {len(duplicates)} IDs de clientes duplicados"
                
                integrity_message = (
                    f"Verificação de integridade:\n"
                    f"- Total de registros: {total_count}\n"
                    f"- IDs únicos: {unique_codes}\n"
                    f"- Diferença: {total_count - unique_codes} registros"
                    f"{duplicate_info}"
                )
                
                logging.info(integrity_message)
                
                if duplicates:
                    logging.warning(f"Atenção: {len(duplicates)} IDs de clientes duplicados encontrados")
            except Exception as e:
                logging.error(f"Erro ao realizar verificação de integridade: {e}")
            
            conn.close()
            
        clean_json_files(json_dir)

    except Exception as e:
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        execution_time = time.time() - start_time
        minutes, seconds = divmod(execution_time, 60)
        error_message = (f"Erro geral no processamento: {e} "
                         f"Arquivo: listaClientes.py, Hora: {error_time}, "
                         f"Tempo de execução: {int(minutes)} minutos e {seconds:.2f} segundos.")
        logging.error(error_message)
        return total_processed

    end_time = time.time()
    end_datetime = datetime.now()
    logging.info(f"Término da execução: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")

    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time, 60)
    logging.info(f"Tempo total de execução: {int(minutes)} minutos e {seconds:.2f} segundos.")
    
    return total_processed

def clean_json_files(json_dir):
    try:
        logging.info(f"Iniciando limpeza de arquivos JSON em {json_dir}")
        count_removed = 0
        
        for filename in os.listdir(json_dir):
            if filename.startswith(f"{table_name}_page") and filename.endswith(".json"):
                file_path = os.path.join(json_dir, filename)
                os.remove(file_path)
                count_removed += 1
        
        logging.info(f"Limpeza concluída: {count_removed} arquivos JSON removidos")
        
        if count_removed > 0:
            logging.info(f"{count_removed} arquivos JSON temporários foram removidos após processamento completo")
            
    except Exception as e:
        logging.error(f"Erro ao limpar arquivos JSON: {e}")

if __name__ == "__main__":
    print("Iniciando processamento de clientes da Compleo...")
    total_inseridos = process_clientes_compleo()
    print(f"Processamento concluído! {total_inseridos} clientes inseridos.")

