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

table_name = 'api_criacaoVaga'
url = "https://api.compleo.app/public/joblist/100056"

body_template = {
  "fields": [
    "numberOfPositions",
    "openingDate",
    "openingReason",
    "hiringEndDate",
    "warningDaysBeforeEndDate",
    "dangerDaysBeforeEndDate",
    "category",
    "justForPCD",
    "employmentType",
    "experienceLevel",
    "workingModel",
    "description",
    "customer",
    "contact",
    "branch",
    "mainRecruiter",
    "recruiters",
    "requester",
    "otherRequesters",
    "notes",
    "visibility",
    "highlightOnCareerSite",
    "status"
  ],
  "sort": {
    "field": "openingDate",
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

def generate_periods():
    periods = []
    
    periods.append({
        "name": "2024",
        "openingDateFrom": "2024-01-01T00:00:00Z",
        "openingDateTo": "2024-12-31T23:59:59Z"
    })
    months_2025 = [
        ("2025-01", "2025-01-01T00:00:00Z", "2025-01-31T23:59:59Z"),
        ("2025-02", "2025-02-01T00:00:00Z", "2025-02-28T23:59:59Z"),
        ("2025-03", "2025-03-01T00:00:00Z", "2025-03-31T23:59:59Z"),
        ("2025-04", "2025-04-01T00:00:00Z", "2025-04-30T23:59:59Z"),
        ("2025-05", "2025-05-01T00:00:00Z", "2025-05-31T23:59:59Z"),
        ("2025-06", "2025-06-01T00:00:00Z", "2025-06-30T23:59:59Z"),
        ("2025-07", "2025-07-01T00:00:00Z", "2025-07-31T23:59:59Z"),
        ("2025-08", "2025-08-01T00:00:00Z", "2025-08-31T23:59:59Z"),
        ("2025-09", "2025-09-01T00:00:00Z", "2025-09-30T23:59:59Z"),
        ("2025-10", "2025-10-01T00:00:00Z", "2025-10-31T23:59:59Z"),
        ("2025-11", "2025-11-01T00:00:00Z", "2025-11-30T23:59:59Z"),
        ("2025-12", "2025-12-01T00:00:00Z", "2025-12-31T23:59:59Z")
    ]
    
    for name, start_date, end_date in months_2025:
        periods.append({
            "name": name,
            "openingDateFrom": start_date,
            "openingDateTo": end_date
        })
    
    return periods

periods = generate_periods()

def clean_value(value):
    if not value:
        return ''
    clean_text = re.sub(r'<[^>]+>', '', str(value)).strip()
    clean_text = clean_text.replace('&nbsp;', ' ').replace('&NBSP;', ' ')
    clean_text = clean_text.replace('&amp;', '&').replace('&AMP;', '&')
    clean_text = clean_text.replace('&lt;', '<').replace('&LT;', '<')
    clean_text = clean_text.replace('&gt;', '>').replace('&GT;', '>')
    clean_text = clean_text.replace('&quot;', '"').replace('&QUOT;', '"')
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
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
                    posicoes INT,
                    dataDeAbertura NVARCHAR(100),
                    tipoJustificativa NVARCHAR(500),
                    dataLimiteDeContratacao NVARCHAR(100),
                    diasCorridosAntesDaDataLimiteEstadoDeAtencao INT,
                    diasCorridosAntesDaDataLimiteEstadoDeUrgencia INT,
                    categoria NVARCHAR(500),
                    pcd NVARCHAR(10),
                    tipoDeContratacao NVARCHAR(500),
                    nivelDeExperiencia NVARCHAR(500),
                    modeloDeTrabalho NVARCHAR(500),
                    descricao NVARCHAR(MAX),
                    codigoDoCliente NVARCHAR(100),
                    contatos NVARCHAR(MAX),
                    filialCodigoDaFilial NVARCHAR(100),
                    recrutadorPrincipal NVARCHAR(500),
                    recrutadores NVARCHAR(MAX),
                    solicitante NVARCHAR(500),
                    outrosSolicitantes NVARCHAR(MAX),
                    observacoes NVARCHAR(MAX),
                    candidaturas NVARCHAR(500),
                    destaqueTrabalheConosco NVARCHAR(10),
                    statusDaVaga NVARCHAR(100),
                    periodo NVARCHAR(10)
                )
                """
                cursor.execute(create_table_query)
                cursor.commit()
                logging.info(f"Tabela {table_name} criada com sucesso")
                
                try:
                    index_query = f"CREATE INDEX idx_job_openingDate ON {table_name} (dataDeAbertura)"
                    cursor.execute(index_query)
                    cursor.commit()
                    logging.info(f"Índice criado para a coluna dataDeAbertura")
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
        
        jobs = []
        period = data.get('period', '') if isinstance(data, dict) else ''
        
        if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
            jobs = data['data']
            total_records = data.get('totalFiltered', 0) or data.get('total', 0)
            current_page = data.get('pagination', {}).get('currentPage', 1)

        elif isinstance(data, list):
            jobs = data
            logging.info(f"Lista direta de {len(jobs)} vagas recebida - Período: {period}")
        else:
            logging.error(f"Formato de dados inesperado: {type(data)}")
            return False
            
        count_inserted = 0
        count_skipped = 0

        for job in jobs:
            def safe_value(val):
                if val is None:
                    return None
                if isinstance(val, dict):
                    return json.dumps(val, ensure_ascii=False)[:500]
                return str(val)[:500]
            
            job_key = f"{safe_value(job.get('openingDate'))}_{safe_value(job.get('customer', {}).get('code') if isinstance(job.get('customer'), dict) else job.get('customer'))}"
            
            if job_key in processed_ids:
                count_skipped += 1
                continue
                
            category_value = None
            if 'category' in job and job['category']:
                if isinstance(job['category'], dict):
                    category_value = job['category'].get('label-pt-BR') or job['category'].get('name') or job['category'].get('label')
                else:
                    category_value = safe_value(job['category'])
            
            opening_reason = None
            if 'openingReason' in job and job['openingReason']:
                if isinstance(job['openingReason'], dict):
                    opening_reason = job['openingReason'].get('label-pt-BR') or job['openingReason'].get('name') or job['openingReason'].get('label')
                else:
                    opening_reason = safe_value(job['openingReason'])
            
            employment_type = None
            if 'employmentType' in job and job['employmentType']:
                if isinstance(job['employmentType'], dict):
                    employment_type = job['employmentType'].get('label-pt-BR') or job['employmentType'].get('name') or job['employmentType'].get('label')
                else:
                    employment_type = safe_value(job['employmentType'])
            
            experience_level = None
            if 'experienceLevel' in job and job['experienceLevel']:
                if isinstance(job['experienceLevel'], dict):
                    experience_level = job['experienceLevel'].get('label-pt-BR') or job['experienceLevel'].get('name') or job['experienceLevel'].get('label')
                else:
                    experience_level = safe_value(job['experienceLevel'])
            
            working_model = None
            if 'workingModel' in job and job['workingModel']:
                if isinstance(job['workingModel'], dict):
                    working_model = job['workingModel'].get('label-pt-BR') or job['workingModel'].get('name') or job['workingModel'].get('label')
                else:
                    working_model = safe_value(job['workingModel'])
            
            customer_code = None
            if 'customer' in job and job['customer']:
                if isinstance(job['customer'], dict):
                    customer_code = job['customer'].get('code')
                else:
                    customer_code = safe_value(job['customer'])
            
            contacts_value = None
            if 'contact' in job and job['contact']:
                if isinstance(job['contact'], list):
                    contacts_list = []
                    for c in job['contact']:
                        if isinstance(c, dict):
                            contact_name = c.get('label-pt-BR') or c.get('name') or c.get('label') or ''
                            if contact_name:
                                contacts_list.append(contact_name)
                        else:
                            if c:
                                contacts_list.append(str(c))
                    contacts_value = ', '.join(contacts_list) if contacts_list else None
                elif isinstance(job['contact'], dict):
                    contacts_value = job['contact'].get('label-pt-BR') or job['contact'].get('name') or job['contact'].get('label')
                else:
                    contacts_value = safe_value(job['contact'])
            
            branch_code = None
            if 'branch' in job and job['branch']:
                if isinstance(job['branch'], dict):
                    branch_code = job['branch'].get('code')
                else:
                    branch_code = safe_value(job['branch'])
            
            main_recruiter = None
            if 'mainRecruiter' in job and job['mainRecruiter']:
                if isinstance(job['mainRecruiter'], dict):
                    main_recruiter = job['mainRecruiter'].get('label-pt-BR') or job['mainRecruiter'].get('name') or job['mainRecruiter'].get('label')
                else:
                    main_recruiter = safe_value(job['mainRecruiter'])
            
            recruiters_value = None
            if 'recruiters' in job and job['recruiters']:
                if isinstance(job['recruiters'], list):
                    recruiters_list = []
                    for r in job['recruiters']:
                        if isinstance(r, dict):
                            recruiter_name = r.get('label-pt-BR') or r.get('name') or r.get('label') or ''
                            if recruiter_name:
                                recruiters_list.append(recruiter_name)
                        else:
                            if r:
                                recruiters_list.append(str(r))
                    recruiters_value = ', '.join(recruiters_list) if recruiters_list else None
                else:
                    recruiters_value = safe_value(job['recruiters'])
            
            requester_value = None
            if 'requester' in job and job['requester']:
                if isinstance(job['requester'], dict):
                    requester_value = job['requester'].get('label-pt-BR') or job['requester'].get('name') or job['requester'].get('label')
                else:
                    requester_value = safe_value(job['requester'])
            
            other_requesters_value = None
            if 'otherRequesters' in job and job['otherRequesters']:
                if isinstance(job['otherRequesters'], list):
                    other_requesters_list = []
                    for r in job['otherRequesters']:
                        if isinstance(r, dict):
                            requester_name = r.get('label-pt-BR') or r.get('name') or r.get('label') or ''
                            if requester_name:
                                other_requesters_list.append(requester_name)
                        else:
                            if r:
                                other_requesters_list.append(str(r))
                    other_requesters_value = ', '.join(other_requesters_list) if other_requesters_list else None
                else:
                    other_requesters_value = safe_value(job['otherRequesters'])
            
            visibility_value = None
            if 'visibility' in job and job['visibility']:
                if isinstance(job['visibility'], dict):
                    visibility_value = job['visibility'].get('label-pt-BR') or job['visibility'].get('name')
                else:
                    visibility_value = safe_value(job['visibility'])
            
            insert_query = f"""
            INSERT INTO {table_name} (
                posicoes,
                dataDeAbertura,
                tipoJustificativa,
                dataLimiteDeContratacao,
                diasCorridosAntesDaDataLimiteEstadoDeAtencao,
                diasCorridosAntesDaDataLimiteEstadoDeUrgencia,
                categoria,
                pcd,
                tipoDeContratacao,
                nivelDeExperiencia,
                modeloDeTrabalho,
                descricao,
                codigoDoCliente,
                contatos,
                filialCodigoDaFilial,
                recrutadorPrincipal,
                recrutadores,
                solicitante,
                outrosSolicitantes,
                observacoes,
                candidaturas,
                destaqueTrabalheConosco,
                statusDaVaga,
                periodo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            try:
                description = clean_value(job.get('description'))
                notes = clean_value(job.get('notes'))
                
                cursor.execute(insert_query, (
                    job.get('numberOfPositions'),
                    safe_value(job.get('openingDate')),
                    opening_reason,
                    safe_value(job.get('hiringEndDate')),
                    job.get('warningDaysBeforeEndDate'),
                    job.get('dangerDaysBeforeEndDate'),
                    category_value,
                    'sim' if job.get('justForPCD') else 'não',
                    employment_type,
                    experience_level,
                    working_model,
                    description,
                    customer_code,
                    contacts_value,
                    branch_code,
                    main_recruiter,
                    recruiters_value,
                    requester_value,
                    other_requesters_value,
                    notes,
                    visibility_value,
                    'sim' if job.get('highlightOnCareerSite') else 'não',
                    safe_value(job.get('status')),
                    period
                ))
                
                processed_ids.add(job_key)
                
                count_inserted += 1
                
                if count_inserted % 10 == 0:
                    cursor.commit()
            except Exception as e:
                logging.error(f"Erro ao inserir vaga: {e}")
                try:
                    logging.error(f"Vaga problemática: {json.dumps(job, ensure_ascii=False)[:500]}")
                except:
                    logging.error("Não foi possível serializar a vaga problemática")
                continue
            
        cursor.commit()
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE periodo = '{period}'")
            total_count = cursor.fetchone()[0]

        except Exception as e:
            logging.warning(f"Não foi possível obter contagem total: {e}")

        return True
    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        logging.error(f"Erro ao salvar dados de vagas: {error_type} - {error_message}")
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
                try:
                    error_response = response.text[:500]
                    logging.error(f"Erro na requisição: Status {response.status_code} - Tentativa {attempt}/{max_retries}")
                    logging.error(f"Resposta da API: {error_response}")
                except:
                    logging.error(f"Erro na requisição: Status {response.status_code} - Tentativa {attempt}/{max_retries}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    error_msg = f"Falha após {max_retries} tentativas. Último status: {response.status_code}"
                    try:
                        error_msg += f" - Resposta: {response.text[:200]}"
                    except:
                        pass
                    return False, error_msg
        except requests.RequestException as e:
            logging.error(f"Exceção na requisição: {e} - Tentativa {attempt}/{max_retries}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                return False, f"Falha após {max_retries} tentativas. Exceção: {str(e)}"
    
    return False, f"Falha após {max_retries} tentativas"

def process_period(period, session, cursor, json_dir, is_first_period, processed_ids):
    period_name = period["name"]
    period_dir = os.path.join(json_dir, period_name)
    os.makedirs(period_dir, exist_ok=True)
    logging.info(f"Iniciando processamento do período {period_name}")
    
    period_body = body_template.copy()
    period_body["openingDateFrom"] = period["openingDateFrom"]
    period_body["openingDateTo"] = period["openingDateTo"]
    period_body["pagination"]["currentPage"] = 1
    period_body["pagination"]["pageSize"] = 50
    
    success, data = make_api_request_with_retry(session, url, period_body)
    
    total_processed = 0
    current_page = 1
    max_pages = 200
    
    if success:
        if isinstance(data, dict):
            data['period'] = period_name
        
        total_records = data.get('totalFiltered', 0) or data.get('total', 0)
        page_size = data.get('pageSize', 50) or 50
        total_pages = (total_records + page_size - 1) // page_size
        
        if total_pages > max_pages:
            logging.warning(f"Período {period_name} tem {total_pages} páginas, limitando em {max_pages} páginas")
            total_pages = max_pages
        
        logging.info(f"Período {period_name}: Total de {total_records} vagas, {total_pages} páginas")
        
        output_path = os.path.join(period_dir, f"{table_name}_page{current_page}.json")
        with open(output_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        
        if not save_to_database(data, table_name, cursor, is_first_page=is_first_period, processed_ids=processed_ids):
            return 0
        
        jobs_page1 = data.get('data', [])
        jobs_count = len(jobs_page1)
        for job in jobs_page1:
            if job.get('openingDate'):
                total_processed += 1
        
        logging.info(f"Página {current_page}/{total_pages}: {jobs_count} vagas processadas")
        
        has_more_pages = jobs_count > 0 and current_page < total_pages
        
        while has_more_pages and current_page < max_pages and current_page < total_pages:
            current_page += 1
            period_body["pagination"]["currentPage"] = current_page
            
            if current_page >= max_pages:
                logging.warning(f"Atingindo limite de páginas ({max_pages}) para o período {period_name}")
                break
            
            success, data = make_api_request_with_retry(session, url, period_body)
            
            if success:
                if isinstance(data, dict):
                    data['period'] = period_name
                
                output_path = os.path.join(period_dir, f"{table_name}_page{current_page}.json")
                with open(output_path, "w", encoding="utf-8") as json_file:
                    json.dump(data, json_file, ensure_ascii=False, indent=4)
                
                if save_to_database(data, table_name, cursor, is_first_page=False, processed_ids=processed_ids):
                    jobs = data.get('data', [])
                    jobs_count = len(jobs)
                    
                    page_processed = 0
                    for job in jobs:
                        if job.get('openingDate'):
                            page_processed += 1
                    
                    total_processed += page_processed
                    logging.info(f"Página {current_page}/{total_pages}: {page_processed} vagas processadas (total: {total_processed})")
                    
                    has_more_pages = jobs_count > 0 and current_page < total_pages and current_page < max_pages
                else:
                    logging.error(f"Erro ao salvar dados da página {current_page} do período {period_name}")
                    break
            else:
                logging.error(f"Erro na requisição da página {current_page} do período {period_name}: {data}")
                break
            
            time.sleep(0.5)
        
        if current_page >= max_pages and current_page < total_pages:
            logging.warning(f"Processamento do período {period_name} interrompido no limite de {max_pages} páginas")
            logging.warning(f"Foram processadas {total_processed} registros de um total estimado de {total_records}")
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE periodo = '{period_name}'")
            period_total = cursor.fetchone()[0]
            
            logging.info(f"Processamento do período {period_name} concluído: {period_total} registros inseridos em {current_page} páginas")
            
            return period_total
        except Exception as e:
            logging.error(f"Erro ao gerar relatório final do período {period_name}: {e}")
            return total_processed
    else:
        error_message = f"Erro na requisição inicial do período {period_name}: {data}"
        logging.error(error_message)
        return 0



def process_vagas_compleo():
    total_all_periods = 0
    start_time = time.time()
    start_datetime = datetime.now()
    logging.info(f"Início da execução: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_dir = os.path.join(os.path.dirname(script_dir), "Jsons", "criacaoVagas")
    
    try:
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
            
            logging.info(f"Processando {len(periods)} períodos para busca de vagas criadas")
            
            for i, period in enumerate(periods):
                logging.info(f"Progresso: Processando período {i+1}/{len(periods)} - {period['name']}")
                
                period_total = process_period(
                    period=period,
                    session=session,
                    cursor=cursor,
                    json_dir=json_dir,
                    is_first_period=(i == 0),
                    processed_ids=processed_ids
                )
                total_all_periods += period_total
                
                progress_pct = ((i + 1) / len(periods)) * 100
                logging.info(f"Período {period['name']} concluído: {period_total} registros | Progresso total: {progress_pct:.1f}%")
                
                if i < len(periods) - 1:
                    logging.info(f"Aguardando 0.25 segundos antes de iniciar próximo período...")
                    time.sleep(0.25)
            
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                total_count = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT periodo, COUNT(*) as total FROM {table_name} GROUP BY periodo")
                period_counts = cursor.fetchall() or []
                
                summary = "Resumo do processamento:\n"
                for period_name, count in period_counts:
                    summary += f"- {period_name}: {count} registros\n"
                summary += f"Total geral: {total_all_periods} registros"
                
                logging.info(summary)
            except Exception as e:
                logging.error(f"Erro ao realizar verificação de integridade: {e}")
            
            conn.close()
            
        clean_json_files(json_dir)

    except Exception as e:
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        execution_time = time.time() - start_time
        minutes, seconds = divmod(execution_time, 60)
        error_message = (f"Erro geral no processamento: {e} "
                         f"Arquivo: criacaoVagas.py, Hora: {error_time}, "
                         f"Tempo de execução: {int(minutes)} minutos e {seconds:.2f} segundos.")
        logging.error(error_message)
    
    finally:
        end_time = time.time()
        end_datetime = datetime.now()
        logging.info(f"Término da execução: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")

        execution_time = end_time - start_time
        minutes, seconds = divmod(execution_time, 60)
        logging.info(f"Tempo total de execução: {int(minutes)} minutos e {seconds:.2f} segundos.")
        
        logging.info("Iniciando limpeza dos arquivos JSON...")
        clean_json_files(json_dir)
        logging.info("Limpeza dos arquivos JSON concluída")
    
    return total_all_periods

def clean_json_files(json_dir):
    try:
        logging.info(f"Iniciando limpeza de arquivos JSON em {json_dir}")
        count_removed = 0
        
        for root, dirs, files in os.walk(json_dir):
            for file in files:
                if file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    try:
                        os.remove(file_path)
                        count_removed += 1
                    except Exception as e:
                        logging.warning(f"Erro ao remover arquivo {file_path}: {e}")
        
        empty_dirs = []
        for root, dirs, files in os.walk(json_dir, topdown=False):
            if root != json_dir and not os.listdir(root):
                empty_dirs.append(root)
                
        for empty_dir in empty_dirs:
            try:
                os.rmdir(empty_dir)
                logging.info(f"Diretório vazio removido: {empty_dir}")
            except Exception as e:
                logging.warning(f"Erro ao remover diretório vazio {empty_dir}: {e}")
        
        logging.info(f"Limpeza concluída: {count_removed} arquivos JSON removidos")
            
    except Exception as e:
        logging.error(f"Erro ao limpar arquivos JSON: {e}")

if __name__ == "__main__":
    print("Iniciando processamento de criação de vagas da Compleo...")
    total_inseridos = process_vagas_compleo()
    print(f"Processamento concluído! {total_inseridos} vagas inseridas.")

