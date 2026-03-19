import requests
import json
import logging
import time
import os
import re
import pyodbc
from datetime import datetime, timedelta
import shutil
from dotenv import load_dotenv

load_dotenv()

server = os.getenv('server')
database = os.getenv('database')
username = os.getenv('username')
password = os.getenv('password')
COMPLEO_API_TOKEN = os.getenv('COMPLEO_API_TOKEN')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler()])

table_name = 'api_Moviment'
url = "https://api.compleo.app/public/applicanthistory/100056"

body_template = {
    "fields": [
        "stage",
        "oldStage",
        "jobData",
        "companyId",
        "operationDate",
        "createdByUser",
        "createdAt",
        "lastUpdatedByUser",
        "lastUpdatedAt",
        "type",
        "applicantCode",
        "comment"
    ],
    "sort": {
        "field": "operationDate",
        "order": "asc"
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

def save_to_database(data, table_name, cursor, is_first_page=True, processed_ids=None, total_inserted=0):

    try:
        if processed_ids is None:
            processed_ids = set()
            
        if is_first_page:
            try:
                check_table_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
                cursor.execute(check_table_query)
                table_exists = cursor.fetchone()[0] > 0
                
                if not table_exists:
                    logging.info(f"Tabela {table_name} não existe, será criada")
                
                create_table_query = f"""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' and xtype='U')
                CREATE TABLE {table_name} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    estagio_json NVARCHAR(MAX),
                    estagio_nome NVARCHAR(255),
                    idEstagio NVARCHAR(100),
                    estagioRotulo NVARCHAR(255),
                    estagioTipoValor NVARCHAR(100),
                    estagioAnterior NVARCHAR(MAX),
                    estagioAnteriorNome NVARCHAR(255),
                    idEstagioAnterior NVARCHAR(100),
                    estagioAnteriorRotulo NVARCHAR(255),
                    estagioAnteriorTipoValor NVARCHAR(100),
                    dadosVaga NVARCHAR(MAX),
                    tituloVaga NVARCHAR(255),
                    codigoVaga NVARCHAR(50),
                    idEmpresa NVARCHAR(255),
                    dataOperacao DATETIME,
                    criadoporUsuario NVARCHAR(255),
                    dataCriacao DATETIME,
                    ultimaAtualizacaoUsuario NVARCHAR(255),
                    dataUltimaAtualizacao DATETIME,
                    tipo NVARCHAR(255),
                    codigoCandidato NVARCHAR(100),
                    dataCaptura DATETIME DEFAULT GETDATE(),
                    periodo NVARCHAR(20),
                    Comentarios NVARCHAR(MAX)
                )
                """
                cursor.execute(create_table_query)
                cursor.commit()
                logging.info(f"Tabela {table_name} criada com sucesso")
                
                index_query = f"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_codigoCandidato' AND object_id = OBJECT_ID('{table_name}')) CREATE INDEX idx_codigoCandidato ON {table_name} (codigoCandidato)"
                cursor.execute(index_query)
                cursor.commit()
                logging.info(f"Índice criado para a coluna codigoCandidato")
                
                cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
                columns = [row[0] for row in cursor.fetchall()]
                logging.info(f"Colunas da nova tabela: {columns}")
            except Exception as e:
                logging.warning(f"Erro ao configurar tabela: {e}")
        
        movements = data.get('data', [])
        period = data.get('period', '')
        total_items = len(movements)
        
        current_page = data.get('pagination', {}).get('currentPage', 1)
        

        
        count_inserted = 0
        count_skipped = 0

        for movement in movements:
            def safe_value(val):
                if val is None:
                    return None
                if isinstance(val, dict):
                    return json.dumps(val, ensure_ascii=False)[:200]
                return str(val)[:200]
            
            def extract_stage_fields(stage_data):
                if not stage_data or not isinstance(stage_data, dict):
                    return None, None, None, None
                
                name = stage_data.get('name')
                stage_id = stage_data.get('id')
                label_name = stage_data.get('labelName')
                
                type_value = None
                if 'type' in stage_data and isinstance(stage_data['type'], dict):
                    type_value = stage_data['type'].get('value')
                
                return name, stage_id, label_name, type_value
            
            def extract_job_fields(job_data):
                if not job_data or not isinstance(job_data, dict):
                    return None, None
                
                title = job_data.get('title')
                job_code = job_data.get('jobCode')
                
                return title, job_code
            
            applicant_code = safe_value(movement.get('applicantCode'))
            operation_date = movement.get('operationDate', '')
            movement_type = safe_value(movement.get('type', ''))
            
            unique_movement_id = f"{applicant_code}_{operation_date}_{movement_type}"
            
            if unique_movement_id in processed_ids:
                count_skipped += 1
                continue
                
            try:
                check_query = f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE codigoCandidato = ? AND dataOperacao = ? AND tipo = ?
                """
                cursor.execute(check_query, (applicant_code, operation_date, movement_type))
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    count_skipped += 1
                    continue
            except Exception as e:
                logging.warning(f"Erro ao verificar duplicidade para movimentação {unique_movement_id}: {e}")
            
            try:
                stage_data = movement.get('stage')
                stage_name, stage_id, stage_labelName, stage_type_value = extract_stage_fields(stage_data)
                
                oldStage_data = movement.get('oldStage')
                oldStage_name, oldStage_id, oldStage_labelName, oldStage_type_value = extract_stage_fields(oldStage_data)
                
                job_data = movement.get('jobData')
                job_title, job_code = extract_job_fields(job_data)
                
                comment = safe_value(movement.get('comment'))
                
                insert_query = f"""
                INSERT INTO {table_name} (
                    estagio_json,
                    estagio_nome,
                    idEstagio,
                    estagioRotulo,
                    estagioTipoValor,
                    estagioAnterior,
                    estagioAnteriorNome,
                    idEstagioAnterior,
                    estagioAnteriorRotulo,
                    estagioAnteriorTipoValor,
                    dadosVaga,
                    tituloVaga,
                    codigoVaga,
                    idEmpresa,
                    dataOperacao,
                    criadoporUsuario,
                    dataCriacao,
                    ultimaAtualizacaoUsuario,
                    dataUltimaAtualizacao,
                    tipo,
                    codigoCandidato,
                    periodo,
                    Comentarios
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                try:
                    cursor.execute(insert_query, (
                        safe_value(stage_data),
                        stage_name,
                        stage_id,
                        stage_labelName,
                        stage_type_value,
                        safe_value(oldStage_data),
                        oldStage_name,
                        oldStage_id,
                        oldStage_labelName,
                        oldStage_type_value,
                        safe_value(job_data),
                        job_title,
                        job_code,
                        safe_value(movement.get('companyId')),
                        movement.get('operationDate'),
                        safe_value(movement.get('createdByUser')),
                        movement.get('createdAt'),
                        safe_value(movement.get('lastUpdatedByUser')),
                        movement.get('lastUpdatedAt'),
                        safe_value(movement.get('type')),
                        applicant_code,
                        period,
                        comment
                    ))
                    
                    processed_ids.add(unique_movement_id)
                    count_inserted += 1
                    
                    if count_inserted % 10 == 0 and count_inserted < total_items:
                        cursor.commit()
                except Exception as e:
                    logging.error(f"Erro ao inserir movimentação {unique_movement_id}: {e}")
                    continue
            except Exception as e:
                logging.error(f"Erro ao extrair campos do estágio: {e}")
                continue
            

        
        cursor.commit()
        return True, count_inserted
    except Exception as e:
        logging.error(f"Erro ao salvar dados: {e}")
        try:
            cursor.rollback()
        except:
            logging.error("Não foi possível fazer rollback")
        return False, 0

def process_period(period, session, cursor, json_dir, is_first_period, processed_ids):
    period_name = period["name"]
    period_dir = os.path.join(json_dir, period_name)
    
    try:
        current_period_date = datetime.strptime(period_name.split("to")[0], "%Y-%m-%d")
        current_month = current_period_date.month
        current_year = current_period_date.year
    except Exception as e:
        logging.warning(f"Não foi possível extrair mês/ano do período {period_name}: {e}")
        current_month = None
        current_year = None
    
    if os.path.exists(period_dir):
        try:
            for file in os.listdir(period_dir):
                if file.endswith('.json'):
                    os.remove(os.path.join(period_dir, file))
            logging.info(f"Diretório {period_dir} limpo antes de iniciar processamento")
        except Exception as e:
            logging.warning(f"Erro ao limpar diretório {period_dir}: {e}")
    
    os.makedirs(period_dir, exist_ok=True)
    logging.info(f"Iniciando processamento do período {period_name}")
    
    period_body = {
        "lastUpdatedAtFrom": period["lastUpdatedAtFrom"],
        "lastUpdatedAtTo": period["lastUpdatedAtTo"],
        "fields": body_template["fields"],
        "sort": {
            "field": "operationDate",
            "order": "asc"
        },
        "pagination": {
            "currentPage": 1,
            "pageSize": 50
        }
    }
    
    logging.info(f"Body completo da requisição: {json.dumps(period_body)}")
    
    success, data = make_api_request_with_retry(session, url, period_body)
    
    if not success:
        logging.warning(f"Erro ao processar período {period_name}. Pulando.")
        shutil.rmtree(period_dir, ignore_errors=True)
        return 0, None
    
    total_processed = 0
    total_inserted = 0
    current_page = 1
    empty_pages_count = 0
    max_empty_pages = 5
    max_pages = 200
    keep_json = False
    
    if success:
        if isinstance(data, dict):
            data['period'] = period_name
        
        total_records = min(data.get('totalFiltered', 0) or data.get('total', 0), 10000)
        page_size = data.get('pageSize', 50) or 50
        total_pages = min((total_records + page_size - 1) // page_size, max_pages)
        
        movements_count = len(data.get('data', []))
        first_page_has_data = movements_count > 0
        
        if not first_page_has_data and total_records > 0:
            logging.warning(f"API reporta {total_records} registros, mas a primeira página está vazia. Possível inconsistência.")
            empty_pages_count += 1
            
            sample_pages = [5, 10, 20, 50] if total_pages > 50 else [5, 10, 20]
            found_data = False
            
            for sample_page in sample_pages:
                if sample_page >= total_pages:
                    continue
                    
                period_body["pagination"]["currentPage"] = sample_page
                sample_success, sample_data = make_api_request_with_retry(session, url, period_body)
                
                if sample_success and isinstance(sample_data, dict):
                    sample_count = len(sample_data.get('data', []))
                    if sample_count > 0:
                        found_data = True
                        logging.info(f"Dados encontrados na página de amostra {sample_page} ({sample_count} registros)")
                        break
            
            if not found_data:
                logging.warning(f"Nenhum dado encontrado nas páginas de amostra para o período {period_name}. Abandonando processamento.")
                shutil.rmtree(period_dir, ignore_errors=True)
                
                if current_month is not None and current_year is not None:
                    skip_to_next_month = True
                    next_month = current_month + 1 if current_month < 12 else 1
                    next_year = current_year if current_month < 12 else current_year + 1
                    logging.warning(f"Pulando para o próximo mês: {next_month}/{next_year}")
                    return 0, (next_month, next_year)
                
                return 0, None
        
        period_body["pagination"]["currentPage"] = 1
        
        logging.info(f"Período {period_name}: Total de {total_records} registros, {total_pages} páginas")
        
        if total_records > 80000 and movements_count == 0:
            logging.error(f"⚠️ PROBLEMA CONHECIDO DA API: Reporta {total_records} registros mas retorna array vazio.")
            logging.error("Este é um erro conhecido da API Compleo. Recomendamos contatar o suporte.")
            
            error_report_path = os.path.join(os.path.dirname(json_dir), "api_error_report.txt")
            try:
                with open(error_report_path, "a") as errfile:
                    errfile.write(f"{datetime.now()} - Período {period_name}: API reporta {total_records} registros mas retorna array vazio\n")
            except:
                pass
            
            if current_month is not None and current_year is not None:
                skip_to_next_month = True
                next_month = current_month + 1 if current_month < 12 else 1
                next_year = current_year if current_month < 12 else current_year + 1
                logging.warning(f"Pulando para o próximo mês: {next_month}/{next_year}")
                return 0, (next_month, next_year)
        
        if movements_count > 0:
            output_path = os.path.join(period_dir, f"movimentacoes_compleo_page{current_page}.json")
            with open(output_path, "w", encoding="utf-8") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
        else:
            output_path = None
        
        success, count_inserted = save_to_database(data, table_name, cursor, is_first_page=is_first_period, processed_ids=processed_ids, total_inserted=total_inserted)
        
        if output_path and os.path.exists(output_path) and not keep_json:
            try:
                os.remove(output_path)
                if count_inserted > 0:
                    logging.info(f"Arquivo JSON da página {current_page} removido após processamento")
            except Exception as e:
                logging.warning(f"Não foi possível remover o arquivo JSON {output_path}: {e}")
        
        if not success:
            shutil.rmtree(period_dir, ignore_errors=True)
            return 0, None
            
        total_inserted += count_inserted
        total_processed += count_inserted
        
        if count_inserted == 0:
            empty_pages_count += 1
        else:
            empty_pages_count = 0
        

        
        if empty_pages_count >= 3 and total_processed == 0:
            logging.warning(f"Primeiras {empty_pages_count} páginas vazias e nenhum registro processado. Interrompendo.")
            shutil.rmtree(period_dir, ignore_errors=True)
            
            if current_month is not None and current_year is not None:
                skip_to_next_month = True
                next_month = current_month + 1 if current_month < 12 else 1
                next_year = current_year if current_month < 12 else current_year + 1
                logging.warning(f"Pulando para o próximo mês: {next_month}/{next_year}")
                return 0, (next_month, next_year)
            
            return 0, None
            
        while current_page < total_pages and current_page < max_pages and empty_pages_count < max_empty_pages:
            current_page += 1
            period_body["pagination"]["currentPage"] = current_page
            
            success, data = make_api_request_with_retry(session, url, period_body)
            
            if not success:
                logging.warning(f"Atingido limite de paginação na página {current_page}. Processados {total_processed} registros até aqui.")
                break
            
            if success:
                if isinstance(data, dict):
                    data['period'] = period_name
                
                page_has_data = len(data.get('data', [])) > 0
                
                if page_has_data:
                    output_path = os.path.join(period_dir, f"movimentacoes_compleo_page{current_page}.json")
                    with open(output_path, "w", encoding="utf-8") as json_file:
                        json.dump(data, json_file, ensure_ascii=False, indent=4)
                else:
                    output_path = None
                
                success, count_inserted = save_to_database(data, table_name, cursor, is_first_page=False, processed_ids=processed_ids, total_inserted=total_inserted)
                
                if output_path and os.path.exists(output_path) and not keep_json:
                    try:
                        os.remove(output_path)
                        if count_inserted > 0:
                            logging.info(f"Arquivo JSON da página {current_page} removido após processamento")
                    except Exception as e:
                        logging.warning(f"Não foi possível remover o arquivo JSON {output_path}: {e}")
                
                if success:
                    total_inserted += count_inserted
                    total_processed += count_inserted
                    
                    if count_inserted == 0:
                        empty_pages_count += 1
                        if empty_pages_count % 5 == 0:
                            logging.info(f"{empty_pages_count} páginas vazias consecutivas detectadas")
                    else:
                        empty_pages_count = 0
                    
                    if empty_pages_count >= max_empty_pages:
                        logging.warning(f"Detectadas {max_empty_pages} páginas vazias consecutivas. Interrompendo processamento.")
                        
                        if current_month is not None and current_year is not None:
                            skip_to_next_month = True
                            next_month = current_month + 1 if current_month < 12 else 1
                            next_year = current_year if current_month < 12 else current_year + 1
                            logging.warning(f"Pulando para o próximo mês: {next_month}/{next_year}")
                            break
                    

                else:
                    break
            else:
                break
            
            time.sleep(0.5)
            
        if empty_pages_count >= max_empty_pages:
            logging.info(f"Processamento interrompido após {empty_pages_count} páginas vazias consecutivas")
            
            if current_month is not None and current_year is not None and total_processed == 0:
                next_month = current_month + 1 if current_month < 12 else 1
                next_year = current_year if current_month < 12 else current_year + 1
                logging.warning(f"Nenhum dado processado neste período e atingido limite de páginas vazias. Pulando para o próximo mês: {next_month}/{next_year}")
                return total_processed, (next_month, next_year)
                
        elif current_page >= max_pages:
            logging.info(f"Processamento interrompido ao atingir o limite de {max_pages} páginas")
        
        try:
            remaining_files = [f for f in os.listdir(period_dir) if f.endswith('.json')]
            
            if remaining_files:
                for file in remaining_files:
                    try:
                        os.remove(os.path.join(period_dir, file))
                    except:
                        pass
                
                logging.info(f"Removidos {len(remaining_files)} arquivos JSON remanescentes do diretório {period_dir}")
        except Exception as e:
            logging.warning(f"Erro ao verificar arquivos remanescentes: {e}")
            
        if total_processed == 0:
            try:
                shutil.rmtree(period_dir, ignore_errors=True)
                logging.info(f"Diretório {period_dir} removido pois não tinha dados")
            except Exception as e:
                logging.warning(f"Erro ao remover diretório vazio {period_dir}: {e}")
        
        logging.info(f"Processamento do período {period_name} concluído: {total_processed} registros inseridos.")
        
        return total_processed, None
    else:
        shutil.rmtree(period_dir, ignore_errors=True)
        return 0, None

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
            response = session.post(url, json=body, headers=headers)
            logging.debug(f"[DIAGNÓSTICO] Requisição HTTP completada - Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, dict) and 'pagination' in data:
                    returned_page = data.get('pagination', {}).get('currentPage', 0)
                    if returned_page != current_page:
                        logging.warning(f"API retornou página {returned_page} quando solicitamos página {current_page}")
                
                if 'data' in data:
                    logging.info(f"A API retornou {len(data['data'])} registros")
                else:
                    logging.warning("A API não retornou o campo 'data' na resposta")
                
                return True, data
            else:
                error_message = f"Erro na requisição: Status {response.status_code} - Tentativa {attempt}/{max_retries}"
                try:
                    error_data = response.json()
                    error_message += f" - Detalhes: {json.dumps(error_data)}"
                except:
                    error_message += f" - Resposta: {response.text[:200]}"
                
                logging.error(error_message)
                
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

def process_movimentacoes_compleo():
    try:
        start_time = time.time()
        start_datetime = datetime.now()
        logging.info(f"Início da execução: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        
        total_all_periods = 0
        processed_ids = set()
        
        try:
            logging.debug("Iniciando função process_movimentacoes_compleo")
            script_dir = os.path.dirname(os.path.abspath(__file__))
            json_dir = os.path.join(os.path.dirname(script_dir), "Jsons", "listaMovimentacoes")
            
            if os.path.exists(json_dir):
                try:
                    for root, dirs, files in os.walk(json_dir):
                        for file in files:
                            if file.endswith('.json'):
                                os.remove(os.path.join(root, file))
                    logging.info(f"Diretório {json_dir} limpo antes de iniciar")
                except Exception as e:
                    logging.warning(f"Erro ao limpar diretório {json_dir}: {e}")
            else:
                os.makedirs(json_dir, exist_ok=True)
            logging.info(f"Diretório para arquivos JSON: {json_dir}")
            
            periods = []
            
            start_date_env = os.getenv('START_DATE')
            end_date_env = os.getenv('END_DATE')
            period_mode = os.getenv('PERIOD_MODE', 'daily')
            
            if start_date_env and end_date_env:
                start_date = datetime.strptime(start_date_env, "%Y-%m-%d")
                end_date = datetime.strptime(end_date_env, "%Y-%m-%d")
                logging.info(f"Usando datas do .env: {start_date.strftime('%d/%m/%Y')} até {end_date.strftime('%d/%m/%Y')}")
            else:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=2)
                logging.info(f"Usando datas padrão: {start_date.strftime('%d/%m/%Y')} até {end_date.strftime('%d/%m/%Y')}")
            
            current_date = start_date
            
            if period_mode == 'monthly':
                logging.info("Modo MENSAL: Criando períodos mensais")
                while current_date <= end_date:
                    if current_date.month == 12:
                        period_end = datetime(current_date.year, 12, 31)
                    else:
                        next_month = datetime(current_date.year, current_date.month + 1, 1)
                        period_end = next_month - timedelta(days=1)
                    
                    if period_end > end_date:
                        period_end = end_date
                    
                    from_date = current_date.strftime("%Y-%m-%dT00:00:00Z")
                    to_date = period_end.strftime("%Y-%m-%dT23:59:59Z")
                    
                    period_name = f"{current_date.strftime('%Y-%m')}"
                    
                    periods.append({
                        "name": period_name,
                        "lastUpdatedAtFrom": from_date,
                        "lastUpdatedAtTo": to_date
                    })
                    
                    if current_date.month == 12:
                        current_date = datetime(current_date.year + 1, 1, 1)
                    else:
                        current_date = datetime(current_date.year, current_date.month + 1, 1)
                    
            elif period_mode == 'weekly':
                logging.info("Modo SEMANAL: Criando períodos semanais")
                while current_date <= end_date:
                    period_end = min(current_date + timedelta(days=6), end_date)
                    
                    from_date = current_date.strftime("%Y-%m-%dT00:00:00Z")
                    to_date = period_end.strftime("%Y-%m-%dT23:59:59Z")
                    
                    period_name = f"{current_date.strftime('%Y-%m-%d')}to{period_end.strftime('%d')}"
                    
                    periods.append({
                        "name": period_name,
                        "lastUpdatedAtFrom": from_date,
                        "lastUpdatedAtTo": to_date
                    })
                    
                    current_date = period_end + timedelta(days=1)
            else:
                logging.info("Modo DIÁRIO: Criando períodos de 14 dias")
                while current_date < end_date:
                    period_end = current_date + timedelta(days=14)
                    
                    next_month = datetime(current_date.year, current_date.month + 1 if current_date.month < 12 else 1, 1)
                    if period_end.month != current_date.month or period_end.year != current_date.year:
                        period_end = next_month - timedelta(days=1)
                    
                    if period_end > end_date:
                        period_end = end_date
                    
                    from_date = current_date.strftime("%Y-%m-%dT00:00:00Z")
                    to_date = period_end.strftime("%Y-%m-%dT23:59:59Z")
                    
                    period_name = f"{current_date.strftime('%Y-%m-%d')}to{period_end.strftime('%d')}"
                    
                    periods.append({
                        "name": period_name,
                        "lastUpdatedAtFrom": from_date,
                        "lastUpdatedAtTo": to_date
                    })
                    
                    current_date = period_end + timedelta(days=1)
            
            logging.info(f"Gerados {len(periods)} períodos para o intervalo.")
            
            with requests.Session() as session:
                conn_str = (
                    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
                    f'SERVER={server};'
                    f'DATABASE={database};'
                    f'UID={username};'
                    f'PWD={password};'
                    f'TrustServerCertificate=yes;'
                )
                
                logging.debug("Conectando com SQL Server...")
                conn = pyodbc.connect(conn_str)
                
                logging.info("Conexão com o banco de dados estabelecida com sucesso")
                cursor = conn.cursor()
                
                try:
                    drop_table_query = f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
                    cursor.execute(drop_table_query)
                    cursor.commit()
                    logging.info(f"Tabela {table_name} removida para ser recriada com a estrutura atualizada")
                except Exception as e:
                    logging.warning(f"Erro ao tentar dropar tabela: {e}")
                
                try:
                    create_table_query = f"""
                    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' and xtype='U')
                    CREATE TABLE {table_name} (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        estagio_json NVARCHAR(MAX),
                        estagio_nome NVARCHAR(255),
                        idEstagio NVARCHAR(100),
                        estagioRotulo NVARCHAR(255),
                        estagioTipoValor NVARCHAR(100),
                        estagioAnterior NVARCHAR(MAX),
                        estagioAnteriorNome NVARCHAR(255),
                        idEstagioAnterior NVARCHAR(100),
                        estagioAnteriorRotulo NVARCHAR(255),
                        estagioAnteriorTipoValor NVARCHAR(100),
                        dadosVaga NVARCHAR(MAX),
                        tituloVaga NVARCHAR(255),
                        codigoVaga NVARCHAR(50),
                        idEmpresa NVARCHAR(255),
                        dataOperacao DATETIME,
                        criadoporUsuario NVARCHAR(255),
                        dataCriacao DATETIME,
                        ultimaAtualizacaoUsuario NVARCHAR(255),
                        dataUltimaAtualizacao DATETIME,
                        tipo NVARCHAR(255),
                        codigoCandidato NVARCHAR(100),
                        dataCaptura DATETIME DEFAULT GETDATE(),
                        periodo NVARCHAR(20),
                        Comentarios NVARCHAR(MAX)
                    )
                    """
                    cursor.execute(create_table_query)
                    cursor.commit()
                    index_query = f"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_codigoCandidato' AND object_id = OBJECT_ID('{table_name}')) CREATE INDEX idx_codigoCandidato ON {table_name} (codigoCandidato)"
                    cursor.execute(index_query)
                    cursor.commit()
                    logging.info(f"Índice criado para a coluna codigoCandidato")
                    
                    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
                    columns = [row[0] for row in cursor.fetchall()]
                    logging.info(f"Colunas da nova tabela: {columns}")
                except Exception as e:
                    logging.warning(f"Erro ao verificar/criar tabela ou índice: {e}")

                total_all_periods = 0
                period_counts = {}
                
                i = 0
                while i < len(periods):
                    period = periods[i]
                    
                    current_period_date = datetime.strptime(period["name"].split("to")[0], "%Y-%m-%d")
                    
                    if i > 0 and hasattr(process_movimentacoes_compleo, 'skip_to_month_year'):
                        target_month, target_year = process_movimentacoes_compleo.skip_to_month_year
                        
                        if (current_period_date.year < target_year or 
                            (current_period_date.year == target_year and current_period_date.month < target_month)):
                            logging.info(f"Pulando período {period['name']} para ir direto ao mês {target_month}/{target_year}")
                            i += 1
                            continue
                        else:
                            delattr(process_movimentacoes_compleo, 'skip_to_month_year')
                            logging.info(f"Chegamos ao mês alvo {target_month}/{target_year}, continuando processamento normal")
                    
                    period_total, skip_info = process_period(
                        period=period,
                        session=session,
                        cursor=cursor,
                        json_dir=json_dir,
                        is_first_period=(i == 0 and total_all_periods == 0),
                        processed_ids=processed_ids
                    )
                    
                    total_all_periods += period_total
                    period_counts[period["name"]] = period_total
                    
                    if skip_info is not None:
                        target_month, target_year = skip_info
                        logging.warning(f"Definindo próximo período para o mês {target_month}/{target_year}")
                        
                        process_movimentacoes_compleo.skip_to_month_year = (target_month, target_year)
                    
                    i += 1
                    
                    if i < len(periods):
                        logging.info(f"Aguardando 2 segundos antes de iniciar próximo período...")
                        time.sleep(2)
                
                logging.info(f"Estatísticas finais:")
                logging.info(f"- Total de registros inseridos nesta execução: {total_all_periods}")
                logging.info(f"- Códigos de movimentações únicos: {len(processed_ids)}")
                repetidos = total_all_periods - len(processed_ids) if len(processed_ids) > 0 else 0
                logging.info(f"- Movimentações com mesmo código: {repetidos}")
                media = total_all_periods / len(processed_ids) if len(processed_ids) > 0 else 0
                logging.info(f"- Registros por código (média): {media:.1f}")
                
                logging.info(f"Resumo do processamento:")
                for period_name, count in period_counts.items():
                    if count > 0:
                        logging.info(f"- {period_name}: {count} registros")
                logging.info(f"Total geral: {total_all_periods} registros")
                
                try:
                    empty_dirs = []
                    json_files = []
                    
                    for root, dirs, files in os.walk(json_dir):
                        dir_json_files = [os.path.join(root, f) for f in files if f.endswith('.json')]
                        json_files.extend(dir_json_files)
                        
                        if root != json_dir and not dirs and not files:
                            empty_dirs.append(root)
                    
                    for file_path in json_files:
                        try:
                            os.remove(file_path)
                        except Exception as e:
                            logging.warning(f"Não foi possível remover arquivo JSON: {file_path} - {e}")
                    
                    for dir_path in empty_dirs:
                        try:
                            os.rmdir(dir_path)
                        except Exception as e:
                            logging.warning(f"Não foi possível remover diretório vazio: {dir_path} - {e}")
                    
                    if json_files:
                        logging.info(f"Limpeza final: removidos {len(json_files)} arquivos JSON restantes")
                        
                    if empty_dirs:
                        logging.info(f"Limpeza final: removidos {len(empty_dirs)} diretórios vazios")
                        
                except Exception as e:
                    logging.error(f"Erro na limpeza final: {e}")
                
                conn.close()

        except Exception as e:
            logging.error(f"Erro geral no processamento: {e}")
            import traceback
            logging.error(f"Detalhes do erro: {traceback.format_exc()}")

        end_time = time.time()
        end_datetime = datetime.now()
        logging.info(f"Término da execução: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")

        execution_time = end_time - start_time
        minutes, seconds = divmod(execution_time, 60)
        logging.info(f"Tempo total de execução: {int(minutes)} minutos e {seconds:.2f} segundos.")
        
        try:
            return total_all_periods
        except UnboundLocalError:
            logging.warning("Variável total_all_periods não está definida. Retornando 0.")
            return 0
            
    except Exception as e:
        logging.error(f"Erro geral no script: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return 0

if __name__ == "__main__":
    print("Iniciando processamento de movimentações da Compleo...")
    total_inseridos = process_movimentacoes_compleo()
    print(f"Processamento concluído! {total_inseridos} registros inseridos.")
