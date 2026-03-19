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

table_name = 'api_Vagas'
url = "https://api.compleo.app/public/joblist/100056"

body_template = {
  "fields": [
    "jobCode",
    "jobNumber",
    "status",
    "numberOfPositions",
    "createdAt",
    "lastUpdatedAt",
    "visibility",
    "tags",
    "category",
    "employmentType",
    "justForPCD",
    "openingReason",
    "salaryRange",
    "cf_clienteBranch",
    "cf_clienteCostCenter",
    "cf_clienteJobTitle",
    "cf_clienteTimeShift",
    "hiringEndDate",
    "warningDaysBeforeEndDate",
    "dangerDaysBeforeEndDate",
    "customer",
    "branch",
    "mainRecruiter",
    "requester",
    "location",
    "contact",
    "description",
    "notes"
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
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    codigoDaVaga NVARCHAR(100),
                    status NVARCHAR(100),
                    posicoes INT,
                    dataDeCadastro NVARCHAR(100),
                    ultimaAtualizacao NVARCHAR(100),
                    tipoCandid NVARCHAR(100),
                    descCandid NVARCHAR(400),
                    tags NVARCHAR(MAX),
                    categoria NVARCHAR(500),
                    tipoDeContratacao NVARCHAR(500),
                    pcd NVARCHAR(10),
                    tipoJustificativa NVARCHAR(500),
                    remuneracao NVARCHAR(500),
                    filial NVARCHAR(200),
                    CNPJ NVARCHAR(20),
                    codCC NVARCHAR(50),
                    descCC NVARCHAR(200),
                    codCargo NVARCHAR(50),
                    descCargo NVARCHAR(200),
                    turno NVARCHAR(500),
                    dataLimiteDeContratacao NVARCHAR(100),
                    diasCorridosAntesDaDataLimiteEstadoDeAtencao INT,
                    diasCorridosAntesDaDataLimiteEstadoDeUrgencia INT,
                    departamento NVARCHAR(500),
                    filiais NVARCHAR(500),
                    recrutadorPrincipal NVARCHAR(500),
                    solicitante NVARCHAR(500),
                    pais NVARCHAR(100),
                    UF NVARCHAR(2),
                    estado NVARCHAR(100),
                    cidade NVARCHAR(100),
                    codigoPostal NVARCHAR(50),
                    contatos NVARCHAR(500),
                    JobStatusHistory NVARCHAR(MAX),
                    Descrição NVARCHAR(MAX),
                    Observações NVARCHAR(MAX),
                    total INT,
                    totalFiltered INT,
                    data_captura DATETIME DEFAULT GETDATE()
                )
                """
                cursor.execute(create_table_query)
                cursor.commit()
                logging.info(f"Tabela {table_name} criada com sucesso")
                
                try:
                    index_query = f"CREATE INDEX idx_codigoDaVaga ON {table_name} (codigoDaVaga)"
                    cursor.execute(index_query)
                    cursor.commit()
                    logging.info(f"Índice criado para a coluna codigoDaVaga")
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
        
        if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
            jobs = data['data']
            total_records = data.get('totalFiltered', 0) or data.get('total', 0)
            current_page = data.get('pagination', {}).get('currentPage', 1)

        elif isinstance(data, list):
            jobs = data
            logging.info(f"Lista direta de {len(jobs)} vagas recebida")
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
            
            job_code = safe_value(job.get('jobCode'))
            
            if job_code in processed_ids:
                count_skipped += 1

                continue
                
            try:
                check_query = f"SELECT COUNT(*) FROM {table_name} WHERE codigoDaVaga = ?"
                cursor.execute(check_query, (job_code,))
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    count_skipped += 1

                    continue
            except Exception as e:
                logging.warning(f"Erro ao verificar duplicidade para vaga {job_code}: {e}")
            
            tags_value = None
            if 'tags' in job and job['tags']:
                if isinstance(job['tags'], list):
                    tags_value = json.dumps([tag.get('name', '') for tag in job['tags'] if isinstance(tag, dict)], ensure_ascii=False)
                else:
                    tags_value = safe_value(job['tags'])
            
            category_value = None
            if 'category' in job and job['category']:
                if isinstance(job['category'], dict):
                    category_value = job['category'].get('name')
                else:
                    category_value = safe_value(job['category'])
            
            employment_type = None
            if 'employmentType' in job and job['employmentType']:
                if isinstance(job['employmentType'], dict):
                    employment_type = job['employmentType'].get('name')
                else:
                    employment_type = safe_value(job['employmentType'])
            
            opening_reason = None
            if 'openingReason' in job and job['openingReason']:
                if isinstance(job['openingReason'], dict):
                    opening_reason = job['openingReason'].get('name')
                else:
                    opening_reason = safe_value(job['openingReason'])
            
            salary_range = None
            if 'salaryRange' in job and job['salaryRange']:
                if isinstance(job['salaryRange'], dict):
                    min_value = job['salaryRange'].get('min', '')
                    max_value = job['salaryRange'].get('max', '')
                    currency = job['salaryRange'].get('currency', '')
                    if min_value or max_value:
                        salary_range = f"{currency} {min_value} - {max_value}".strip()
                else:
                    salary_range = safe_value(job['salaryRange'])
            
            customer_value = None
            if 'customer' in job and job['customer']:
                if isinstance(job['customer'], dict):
                    customer_value = job['customer'].get('name')
                else:
                    customer_value = safe_value(job['customer'])
            
            branch_value = None
            if 'branch' in job and job['branch']:
                if isinstance(job['branch'], dict):
                    branch_value = job['branch'].get('name')
                else:
                    branch_value = safe_value(job['branch'])
            
            main_recruiter = None
            if 'mainRecruiter' in job and job['mainRecruiter']:
                if isinstance(job['mainRecruiter'], dict):
                    main_recruiter = job['mainRecruiter'].get('name')
                else:
                    main_recruiter = safe_value(job['mainRecruiter'])
            
            requester_value = None
            if 'requester' in job and job['requester']:
                if isinstance(job['requester'], dict):
                    requester_value = job['requester'].get('name')
                else:
                    requester_value = safe_value(job['requester'])
            
            country = state = city = postal_code = None
            if 'location' in job and job['location']:
                loc = job['location']
                if isinstance(loc, dict):
                    if 'country' in loc and loc['country']:
                        if isinstance(loc['country'], dict):
                            country = loc['country'].get('label')
                        else:
                            country = safe_value(loc['country'])
                    
                    if 'provinceOrState' in loc and loc['provinceOrState']:
                        if isinstance(loc['provinceOrState'], dict):
                            state = loc['provinceOrState'].get('label')
                        else:
                            state = safe_value(loc['provinceOrState'])
                    
                    if 'city' in loc and loc['city']:
                        if isinstance(loc['city'], dict):
                            city = loc['city'].get('label')
                        else:
                            city = safe_value(loc['city'])
                    
                    postal_code = safe_value(loc.get('postalCode'))
                else:
                    logging.warning(f"Campo location não é um dicionário: {type(loc)}")
            
            contact_value = None
            if 'contact' in job and job['contact']:
                if isinstance(job['contact'], dict):
                    contact_value = job['contact'].get('name')
                else:
                    contact_value = safe_value(job['contact'])
            
            visibility_value = None
            if 'visibility' in job and job['visibility']:
                if isinstance(job['visibility'], dict) and 'label-pt-BR' in job['visibility']:
                    visibility_value = job['visibility'].get('label-pt-BR')
                else:
                    visibility_value = safe_value(job['visibility'])
            
            clienteBranch_value = None
            if 'cf_clienteBranch' in job and job['cf_clienteBranch']:
                if isinstance(job['cf_clienteBranch'], dict) and 'label-pt-BR' in job['cf_clienteBranch']:
                    clienteBranch_value = job['cf_clienteBranch'].get('label-pt-BR')
                else:
                    clienteBranch_value = safe_value(job['cf_clienteBranch'])
            
            clienteCostCenter_value = None
            if 'cf_clienteCostCenter' in job and job['cf_clienteCostCenter']:
                if isinstance(job['cf_clienteCostCenter'], dict) and 'label-pt-BR' in job['cf_clienteCostCenter']:
                    clienteCostCenter_value = job['cf_clienteCostCenter'].get('label-pt-BR')
                else:
                    clienteCostCenter_value = safe_value(job['cf_clienteCostCenter'])
            
            clienteJobTitle_value = None
            if 'cf_clienteJobTitle' in job and job['cf_clienteJobTitle']:
                if isinstance(job['cf_clienteJobTitle'], dict) and 'label-pt-BR' in job['cf_clienteJobTitle']:
                    clienteJobTitle_value = job['cf_clienteJobTitle'].get('label-pt-BR')
                else:
                    clienteJobTitle_value = safe_value(job['cf_clienteJobTitle'])
            
            clienteTimeShift_value = None
            if 'cf_clienteTimeShift' in job and job['cf_clienteTimeShift']:
                if isinstance(job['cf_clienteTimeShift'], dict) and 'label-pt-BR' in job['cf_clienteTimeShift']:
                    clienteTimeShift_value = job['cf_clienteTimeShift'].get('label-pt-BR')
                else:
                    clienteTimeShift_value = safe_value(job['cf_clienteTimeShift'])
            
            def extrair_candidaturas(valor):
                if not valor:
                    return None, None
                
                match = re.match(r'([^(]+)\s*\(([^)]+)\)', valor)
                if match:
                    return match.group(1).strip(), match.group(2).strip()
                return valor, None

            def extrair_empresa(valor):
                if not valor:
                    return None, None
                
                match = re.match(r'(.*?)\s*-\s*\(([^)]+)\)', valor)
                if match:
                    return match.group(1).strip(), match.group(2).strip()
                return valor, None

            def extrair_centro_custo(valor):
                if not valor:
                    return None, None
                
                match = re.match(r'(.*?)\s*-\s*\(([^)]+)\)', valor)
                if match:
                    return match.group(2).strip(), match.group(1).strip()
                return None, valor

            def extrair_cargo(valor):
                if not valor:
                    return None, None
                
                match = re.match(r'(.*?)\s*-\s*\(([^)]+)\)', valor)
                if match:
                    return match.group(2).strip(), match.group(1).strip()
                return None, valor

            def extrair_estado(valor):
                if not valor:
                    return None, None
                
                match = re.match(r'(.*?)\s*\(([A-Z]{2})\)', valor)
                if match:
                    return match.group(2).strip(), match.group(1).strip()
                return None, valor

            tipo_candid, desc_candid = extrair_candidaturas(visibility_value)

            filial_value, cnpj_value = extrair_empresa(clienteBranch_value)

            cod_cc, desc_cc = extrair_centro_custo(clienteCostCenter_value)

            cod_cargo, desc_cargo = extrair_cargo(clienteJobTitle_value)

            uf_value, estado_nome = extrair_estado(state)

            insert_query = f"""
            INSERT INTO {table_name} (
                codigoDaVaga,
                status,
                posicoes,
                dataDeCadastro,
                ultimaAtualizacao,
                tipoCandid,
                descCandid,
                tags,
                categoria,
                tipoDeContratacao,
                pcd,
                tipoJustificativa,
                remuneracao,
                filial,
                CNPJ,
                codCC,
                descCC,
                codCargo,
                descCargo,
                turno,
                dataLimiteDeContratacao,
                diasCorridosAntesDaDataLimiteEstadoDeAtencao,
                diasCorridosAntesDaDataLimiteEstadoDeUrgencia,
                departamento,
                filiais,
                recrutadorPrincipal,
                solicitante,
                pais,
                UF,
                estado,
                cidade,
                codigoPostal,
                contatos,
                JobStatusHistory,
                Descrição,
                Observações,
                total,
                totalFiltered
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            try:
                job_status_history = None
                if 'JobStatusHistory' in job and job['JobStatusHistory']:
                    job_status_history = json.dumps(job['JobStatusHistory'], ensure_ascii=False)
                    
                description = clean_value(job.get('description'))
                notes = clean_value(job.get('notes'))
                
                total_value = data.get('total', 0) if isinstance(data, dict) else 0
                total_filtered_value = data.get('totalFiltered', 0) if isinstance(data, dict) else 0
                cursor.execute(insert_query, (
                    job_code,
                    safe_value(job.get('status')),
                    job.get('numberOfPositions'),
                    safe_value(job.get('createdAt')),
                    safe_value(job.get('lastUpdatedAt')),
                    tipo_candid,
                    desc_candid,
                    tags_value,
                    category_value,
                    employment_type,
                    'sim' if job.get('justForPCD') else 'não',
                    opening_reason,
                    salary_range,
                    filial_value,
                    cnpj_value,
                    cod_cc,
                    desc_cc,
                    cod_cargo,
                    desc_cargo,
                    clienteTimeShift_value,
                    safe_value(job.get('hiringEndDate')),
                    job.get('warningDaysBeforeEndDate'),
                    job.get('dangerDaysBeforeEndDate'),
                    customer_value,
                    branch_value,
                    main_recruiter,
                    requester_value,
                    country,
                    uf_value,
                    estado_nome,
                    city,
                    postal_code,
                    contact_value,
                    job_status_history,
                    description,
                    notes,
                    total_value,
                    total_filtered_value
                ))
                
                processed_ids.add(job_code)
                
                count_inserted += 1
                
                if count_inserted % 10 == 0:
                    cursor.commit()
            except Exception as e:
                logging.error(f"Erro ao inserir vaga {job_code}: {e}")
                try:
                    logging.error(f"Vaga problemática: {json.dumps(job, ensure_ascii=False)[:500]}")
                except:
                    logging.error("Não foi possível serializar a vaga problemática")
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

def process_vagas(session, cursor, json_dir, processed_ids):
    logging.info("Iniciando processamento de vagas")
    
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
        
        logging.info(f"Total de {total_records} vagas, {total_pages} páginas")
        
        output_path = os.path.join(json_dir, f"vagas_compleo_page{current_page}.json")
        with open(output_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        
        if not save_to_database(data, table_name, cursor, is_first_page=True, processed_ids=processed_ids):
            return 0
        
        jobs_page1 = data.get('data', [])
        jobs_count = len(jobs_page1)
        for job in jobs_page1:
            if job.get('jobCode'):
                total_processed += 1
        

        
        has_more_pages = jobs_count > 0 and current_page < total_pages
        
        while has_more_pages and current_page < max_pages:
            current_page += 1
            request_body["pagination"]["currentPage"] = current_page
            
            success, data = make_api_request_with_retry(session, url, request_body)
            
            if success:
                output_path = os.path.join(json_dir, f"vagas_compleo_page{current_page}.json")
                with open(output_path, "w", encoding="utf-8") as json_file:
                    json.dump(data, json_file, ensure_ascii=False, indent=4)
                
                if save_to_database(data, table_name, cursor, is_first_page=False, processed_ids=processed_ids):
                    jobs = data.get('data', [])
                    jobs_count = len(jobs)
                    
                    page_processed = 0
                    for job in jobs:
                        if job.get('jobCode'):
                            page_processed += 1
                    
                    total_processed += page_processed

                    
                    has_more_pages = jobs_count > 0 and current_page < total_pages
                else:
                    logging.error(f"Erro ao salvar dados da página {current_page}")
                    break
            else:
                logging.error(f"Erro na requisição da página {current_page}: {data}")
                break
            
            time.sleep(0.5)
        
        try:
            cursor.execute(f"""
                SELECT codigoDaVaga, COUNT(*) as count 
                FROM {table_name} 
                GROUP BY codigoDaVaga 
                HAVING COUNT(*) > 1
            """)
            duplicates = cursor.fetchall()
            duplicates_count = len(duplicates)
            
            if duplicates_count > 0:
                logging.warning(f"Verificação final: {duplicates_count} códigos de vagas duplicados encontrados")
                logging.info(f"Removendo duplicidades para {duplicates_count} códigos de vagas")
                for dup_code, count in duplicates:
                    try:
                        cursor.execute(f"""
                            WITH cte AS (
                                SELECT id, ROW_NUMBER() OVER (PARTITION BY codigoDaVaga ORDER BY id DESC) as rn
                                FROM {table_name}
                                WHERE codigoDaVaga = ?
                            )
                            DELETE FROM cte WHERE rn > 1
                        """, (dup_code,))
                        cursor.commit()
                    except Exception as e:
                        logging.error(f"Erro ao remover duplicidade para código {dup_code}: {e}")
        except Exception as e:
            logging.error(f"Erro ao verificar duplicidades: {e}")
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT COUNT(DISTINCT codigoDaVaga) FROM {table_name}")
            unique_codes = cursor.fetchone()[0]
            
            logging.info(f"Processamento de vagas concluído: {total_count} registros inseridos ({unique_codes} códigos únicos) em {current_page} páginas")
            
            return total_count
        except Exception as e:
            logging.error(f"Erro ao gerar relatório final: {e}")
            return total_processed
    else:
        error_message = f"Erro na requisição inicial de vagas: {data}"
        logging.error(error_message)
        return 0



def process_vagas_compleo():
    start_time = time.time()
    start_datetime = datetime.now()
    total_processed = 0
    logging.info(f"Início da execução: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        json_dir = os.path.join(os.path.dirname(script_dir), "Jsons", "listaVagas")
        
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
            
            total_processed = process_vagas(
                session=session,
                cursor=cursor,
                json_dir=json_dir,
                processed_ids=processed_ids
            )
            
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                total_count = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT COUNT(DISTINCT codigoDaVaga) FROM {table_name}")
                unique_codes = cursor.fetchone()[0]
                
                cursor.execute(f"""
                    SELECT codigoDaVaga, COUNT(*) as count 
                    FROM {table_name} 
                    GROUP BY codigoDaVaga 
                    HAVING COUNT(*) > 1
                """)
                duplicates = cursor.fetchall()
                
                duplicate_info = ""
                if duplicates:
                    duplicate_info = f"\nATENÇÃO: Encontrados {len(duplicates)} códigos de vagas duplicados"
                
                integrity_message = (
                    f"Verificação de integridade:\n"
                    f"- Total de registros: {total_count}\n"
                    f"- Códigos de vagas únicos: {unique_codes}\n"
                    f"- Diferença: {total_count - unique_codes} registros"
                    f"{duplicate_info}"
                )
                
                logging.info(integrity_message)
                
                if duplicates:
                    logging.warning(f"Atenção: {len(duplicates)} códigos de vagas duplicados encontrados")
            except Exception as e:
                logging.error(f"Erro ao realizar verificação de integridade: {e}")
            
            conn.close()
            
        clean_json_files(json_dir)

    except Exception as e:
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        execution_time = time.time() - start_time
        minutes, seconds = divmod(execution_time, 60)
        error_message = (f"Erro geral no processamento: {e} "
                         f"Arquivo: listaVagas.py, Hora: {error_time}, "
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
            if filename.startswith("vagas_compleo_page") and filename.endswith(".json"):
                file_path = os.path.join(json_dir, filename)
                os.remove(file_path)
                count_removed += 1
        
        logging.info(f"Limpeza concluída: {count_removed} arquivos JSON removidos")
        
        if count_removed > 0:
            logging.info(f"{count_removed} arquivos JSON temporários foram removidos após processamento completo")
            
    except Exception as e:
        logging.error(f"Erro ao limpar arquivos JSON: {e}")

if __name__ == "__main__":
    print("Iniciando processamento de vagas da Compleo...")
    total_inseridos = process_vagas_compleo()
    print(f"Processamento concluído! {total_inseridos} vagas inseridas.")

