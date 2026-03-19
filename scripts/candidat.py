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

table_name = 'api_Candidatos'
url = "https://api.compleo.app/public/applicantlist/100056"

body_template = {
    "fields": [
        "createdAt",
        "name",
        "cf_salaryPayroll",
        "gender",
        "location",
        "source",
        "sourceInTheJob",
        "disabledPerson",
        "typeOfDisability",
        "position",
        "category",
        "monthlySalaryClaim",
        "currentSalary",
        "availableForTrips",
        "availableForMoving",
        "cf_previouslyWoredAtCliente",
        "scholarity",
        "applicantCode",
        "stage",
        "cpf",
        "opinionOnTheApplicant",
        "opinionOnTheApplicantJob",
        "jobs",
        "total"
    ],
    "pagination": {
        "currentPage": 1,
        "pageSize": 50
    },
    "sort": {
        "field": "lastUpdatedAt",
        "order": "asc"
    }
}

def generate_periods():
    from datetime import datetime, timedelta
    from calendar import monthrange
    
    start_date_env = os.getenv('START_DATE')
    end_date_env = os.getenv('END_DATE')
    period_mode = os.getenv('PERIOD_MODE', 'monthly')
    
    periods = []
    
    if start_date_env and end_date_env:
        start_date = datetime.strptime(start_date_env, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_env, "%Y-%m-%d")
        logging.info(f"Gerando períodos de {start_date.strftime('%d/%m/%Y')} até {end_date.strftime('%d/%m/%Y')} - Modo: {period_mode}")
        
        if period_mode == 'yearly':
            current_year = start_date.year
            end_year = end_date.year
            
            while current_year <= end_year:
                year_start = datetime(current_year, 1, 1)
                year_end = datetime(current_year, 12, 31)
                
                if current_year == start_date.year:
                    year_start = start_date
                if current_year == end_year:
                    year_end = end_date
                
                periods.append({
                    "name": str(current_year),
                    "lastUpdatedAtFrom": year_start.strftime("%Y-%m-%dT00:00:00Z"),
                    "lastUpdatedAtTo": year_end.strftime("%Y-%m-%dT23:59:59Z")
                })
                current_year += 1
                
        elif period_mode == 'monthly':
            current_date = start_date.replace(day=1)
            
            while current_date <= end_date:
                last_day = monthrange(current_date.year, current_date.month)[1]
                month_end = datetime(current_date.year, current_date.month, last_day)
                
                month_start = current_date
                if current_date.year == start_date.year and current_date.month == start_date.month:
                    month_start = start_date
                if month_end > end_date:
                    month_end = end_date
                
                periods.append({
                    "name": current_date.strftime("%Y-%m"),
                    "lastUpdatedAtFrom": month_start.strftime("%Y-%m-%dT00:00:00Z"),
                    "lastUpdatedAtTo": month_end.strftime("%Y-%m-%dT23:59:59Z")
                })
                
                if current_date.month == 12:
                    current_date = datetime(current_date.year + 1, 1, 1)
                else:
                    current_date = datetime(current_date.year, current_date.month + 1, 1)
        
        elif period_mode == 'weekly':
            current_date = start_date
            
            while current_date <= end_date:
                week_end = min(current_date + timedelta(days=6), end_date)
                
                periods.append({
                    "name": f"{current_date.strftime('%Y-%m-%d')}to{week_end.strftime('%d')}",
                    "lastUpdatedAtFrom": current_date.strftime("%Y-%m-%dT00:00:00Z"),
                    "lastUpdatedAtTo": week_end.strftime("%Y-%m-%dT23:59:59Z")
                })
                
                current_date = week_end + timedelta(days=1)
    else:
        logging.warning("START_DATE e END_DATE não configurados no .env. Usando ano padrão 2026.")
        current_date = datetime(2026, 1, 1)
        end_date = datetime(2026, 12, 31)
        
        while current_date <= end_date:
            last_day = monthrange(current_date.year, current_date.month)[1]
            month_end = datetime(current_date.year, current_date.month, last_day)
            
            periods.append({
                "name": current_date.strftime("%Y-%m"),
                "lastUpdatedAtFrom": current_date.strftime("%Y-%m-%dT00:00:00Z"),
                "lastUpdatedAtTo": month_end.strftime("%Y-%m-%dT23:59:59Z")
            })
            
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)
    
    logging.info(f"Gerados {len(periods)} períodos: {[p['name'] for p in periods]}")
    return periods

def check_and_subdivide_period_if_needed(period, session, url, body_template, headers):
    max_safe_records = 9500
    
    test_body = body_template.copy()
    test_body["lastUpdatedAtFrom"] = period["lastUpdatedAtFrom"]
    test_body["lastUpdatedAtTo"] = period["lastUpdatedAtTo"]
    test_body["pagination"]["currentPage"] = 1
    test_body["pagination"]["pageSize"] = 50
    
    try:
        response = session.post(url, json=test_body, headers=headers, verify=False)
        if response.status_code == 200:
            data = response.json()
            total_records = data.get('totalFiltered', 0) or data.get('total', 0)
            
            if total_records <= max_safe_records:
                return [period]
            
            period_name = period["name"]
            logging.warning(f"Período {period_name} tem {total_records} registros (>10.000). Subdividindo...")
            
            if len(period_name.split('-')) == 2:
                return subdivide_month_into_weeks(period)
            else:
                logging.error(f"Período {period_name} já é pequeno mas ainda tem muitos registros: {total_records}")
                return [period]
        else:
            logging.warning(f"Erro ao verificar período {period['name']}: HTTP {response.status_code}")
            return [period]
    except Exception as e:
        logging.warning(f"Erro ao verificar período {period['name']}: {e}")
        return [period]

def subdivide_month_into_weeks(period):
    from datetime import datetime, timedelta
    
    period_name = period["name"]
    year, month = period_name.split('-')
    year, month = int(year), int(month)
    
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    periods = []
    current_date = start_date
    week_num = 1
    
    while current_date <= end_date:
        week_end = min(current_date + timedelta(days=6), end_date)
        
        week_period = {
            "name": f"{period_name}-W{week_num}",
            "lastUpdatedAtFrom": current_date.strftime("%Y-%m-%dT00:00:00Z"),
            "lastUpdatedAtTo": week_end.strftime("%Y-%m-%dT23:59:59Z")
        }
        
        periods.append(week_period)
        logging.info(f"Criado subperíodo: {week_period['name']} ({current_date.strftime('%d/%m')} a {week_end.strftime('%d/%m')})")
        
        current_date = week_end + timedelta(days=1)
        week_num += 1
    
    return periods

periods = generate_periods()

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {COMPLEO_API_TOKEN}"
}

def clean_value(value):
    if value is None:
        return None
    if not value:
        return None
    clean_text = re.sub(r'<[^>]+>', '', str(value)).strip()
    return clean_text if clean_text else None

def clean_time_value(value):
    if not value:
        return ''
    clean_text = clean_value(value)
    time_match = re.search(r'\d{2}:\d{2}', clean_text)
    return time_match.group(0) if time_match else ''

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
                    nome NVARCHAR(255),
                    dataDeCadastro NVARCHAR(255),
                    salarioFolha NVARCHAR(50),
                    genero NVARCHAR(100),
                    pais NVARCHAR(100),
                    paisCodigo NVARCHAR(10),
                    UF NVARCHAR(2),
                    estado NVARCHAR(100),
                    cidade NVARCHAR(100),
                    codigoPostal NVARCHAR(50),
                    logradouro NVARCHAR(255),
                    complemento NVARCHAR(255),
                    bairro NVARCHAR(255),
                    origem NVARCHAR(255),
                    pessoaComDeficiencia NVARCHAR(50),
                    tipoDeDeficiencia NVARCHAR(255),
                    cargo NVARCHAR(500),
                    categoria NVARCHAR(255),
                    pretensaoSalarial NVARCHAR(50),
                    disponivelParaViagem NVARCHAR(100),
                    disponivelParaMudanca NVARCHAR(255),
                    escolaridade NVARCHAR(500),
                    codigoDoCandidato NVARCHAR(100),
                    estagioAtualNoProcessoSeletivo NVARCHAR(255),
                    jobCode NVARCHAR(100),
                    cf_previouslyWoredAtCliente NVARCHAR(50),
                    cpf NVARCHAR(20),
                    total INT,
                    data_captura DATETIME DEFAULT GETDATE(),
                    periodo NVARCHAR(10),
                    [Parecer sobre o candidato] NVARCHAR(MAX),
                    [Parecer sobre o candidato (Vaga)] NVARCHAR(MAX),
                    [Vagas associadas ao candidato] NVARCHAR(MAX)
                )
                """
                cursor.execute(create_table_query)
                cursor.commit()
                logging.info(f"Tabela {table_name} criada com sucesso")
                
                try:
                    index_query = f"CREATE INDEX idx_codigoDoCandidato ON {table_name} (codigoDoCandidato)"
                    cursor.execute(index_query)
                    cursor.commit()
                    logging.info(f"Índice criado para a coluna codigoDoCandidato")
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
        
        candidates = []
        period = data.get('period', '')
        
        if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
            candidates = data['data']
            total_records = data.get('totalFiltered', 0) or data.get('total', 0)
            current_page = data.get('pagination', {}).get('currentPage', 1)

        elif isinstance(data, list):
            candidates = data
            logging.info(f"Lista direta de {len(candidates)} candidatos recebida - Período: {period}")
        else:
            logging.error(f"Formato de dados inesperado: {type(data)}")
            return False
            
        count_inserted = 0
        count_skipped = 0

        for candidate in candidates:
            def safe_value(val):
                if val is None:
                    return None
                if isinstance(val, dict):
                    return json.dumps(val, ensure_ascii=False)[:200]
                return str(val)[:200]
            
            applicant_code = safe_value(candidate.get('applicantCode'))
            
            if applicant_code in processed_ids:
                count_skipped += 1

                continue
                
            try:
                check_query = f"SELECT COUNT(*) FROM {table_name} WHERE codigoDoCandidato = ?"
                cursor.execute(check_query, (applicant_code,))
                result = cursor.fetchone()
                exists = result[0] > 0 if result else False
                
                if exists:
                    count_skipped += 1

                    continue
            except Exception as e:
                logging.warning(f"Erro ao verificar duplicidade para candidato {applicant_code}: {e}")
            
            gender_value = None
            if 'gender' in candidate and candidate['gender']:
                if isinstance(candidate['gender'], dict):
                    gender_value = candidate['gender'].get('label-pt-BR')
                else:
                    gender_value = safe_value(candidate['gender'])
            
            country_name = None
            country_code = None
            uf_value = None
            state_name = None
            city = None
            postal_code = None
            logradouro = None
            complemento = None
            neighborhood = None

            if 'location' in candidate and candidate['location']:
                loc = candidate['location']
                if isinstance(loc, dict):
                    if 'country' in loc and loc['country']:
                        if isinstance(loc['country'], dict):
                            country_value = loc['country'].get('label')
                            if country_value:
                                country_name, country_code = extrair_pais_uf(country_value)
                        else:
                            country_value = safe_value(loc['country'])
                            if country_value:
                                country_name, country_code = extrair_pais_uf(country_value)
                    
                    if 'provinceOrState' in loc and loc['provinceOrState']:
                        if isinstance(loc['provinceOrState'], dict):
                            state_value = loc['provinceOrState'].get('label')
                            if state_value:
                                uf_value, state_name = extrair_estado_uf(state_value)
                        else:
                            state_value = safe_value(loc['provinceOrState'])
                            if state_value:
                                uf_value, state_name = extrair_estado_uf(state_value)
                    
                    if 'city' in loc and loc['city']:
                        if isinstance(loc['city'], dict):
                            city = loc['city'].get('label')
                        else:
                            city = safe_value(loc['city'])
                    
                    postal_code = safe_value(loc.get('postalCode'))
                    
                    logradouro = safe_value(loc.get('addressline1'))
                    
                    complemento = safe_value(loc.get('addressline2'))
                    
                    neighborhood = safe_value(loc.get('neighborhood'))
                else:
                    logging.warning(f"Campo location não é um dicionário: {type(loc)}")
            
            source = None
            if 'source' in candidate and candidate['source']:
                if isinstance(candidate['source'], dict):
                    source = candidate['source'].get('label-pt-BR')
                else:
                    source = safe_value(candidate['source'])
            
            disabled_person = None
            if 'disabledPerson' in candidate:
                disabled_person = str(candidate['disabledPerson']).lower()
            
            type_of_disability = safe_value(candidate.get('typeOfDisability'))
            
            position = safe_value(candidate.get('position'))
            
            category = None
            if 'category' in candidate and candidate['category']:
                if isinstance(candidate['category'], dict):
                    category = candidate['category'].get('label-pt-BR')
                else:
                    category = safe_value(candidate['category'])
            
            salary_claim = None
            if 'monthlySalaryClaim' in candidate and candidate['monthlySalaryClaim']:
                if isinstance(candidate['monthlySalaryClaim'], dict):
                    currency = candidate['monthlySalaryClaim'].get('currency', '')
                    value = candidate['monthlySalaryClaim'].get('value', '')
                    if currency or value:
                        salary_claim = f"{currency} {value}".strip()
                else:
                    salary_claim = safe_value(candidate['monthlySalaryClaim'])
            
            available_for_trips = None
            if 'availableForTrips' in candidate:
                available_for_trips = str(candidate['availableForTrips']).lower()
            
            moving_availability = None
            if 'availableForMoving' in candidate and candidate['availableForMoving']:
                if isinstance(candidate['availableForMoving'], dict):
                    moving_availability = candidate['availableForMoving'].get('label-pt-BR')
                else:
                    moving_availability = safe_value(candidate['availableForMoving'])
            
            education = None
            if 'scholarity' in candidate and candidate['scholarity']:
                if isinstance(candidate['scholarity'], dict):
                    education_raw = candidate['scholarity'].get('label-pt-BR')
                    education = remover_parenteses(education_raw) if education_raw else None
                else:
                    education_raw = safe_value(candidate['scholarity'])
                    education = remover_parenteses(education_raw) if education_raw else None
            
            salary_payroll_value = extrair_valor_numerico(candidate.get('cf_salaryPayroll'))
            
            salary_claim_value = None
            if 'monthlySalaryClaim' in candidate and candidate['monthlySalaryClaim']:
                salary_claim_value = extrair_valor_numerico(candidate['monthlySalaryClaim'])
            
            job_code = None
            if 'Jobs' in candidate and isinstance(candidate['Jobs'], list) and candidate['Jobs']:
                job = candidate['Jobs'][0]
                if isinstance(job, dict):
                    if 'JobId' in job:
                        job_code = job['JobId']
                    elif 'jobCode' in job:
                        job_code = job['jobCode']
                    else:
                        job_code = None
                else:
                    job_code = None
            previously_worked = None
            if 'cf_previouslyWoredAtCliente' in candidate:
                val = candidate['cf_previouslyWoredAtCliente']
                if isinstance(val, dict):
                    previously_worked = val.get('label-pt-BR') or val.get('label') or str(val)
                else:
                    previously_worked = str(val)
            if isinstance(data, dict):
                total_registros = data.get('totalFiltered', 0) or data.get('total', 0)
            else:
                total_registros = 0
            
            insert_query = f"""
            INSERT INTO {table_name} (
                nome,
                dataDeCadastro,
                salarioFolha,
                genero,
                pais,
                paisCodigo,
                UF,
                estado,
                cidade,
                codigoPostal,
                logradouro,
                complemento,
                bairro,
                origem,
                pessoaComDeficiencia,
                tipoDeDeficiencia,
                cargo,
                categoria,
                pretensaoSalarial,
                disponivelParaViagem,
                disponivelParaMudanca,
                escolaridade,
                codigoDoCandidato,
                estagioAtualNoProcessoSeletivo,
                jobCode,
                cf_previouslyWoredAtCliente,
                cpf,
                total,
                periodo,
                [Parecer sobre o candidato],
                [Parecer sobre o candidato (Vaga)],
                [Vagas associadas ao candidato]
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            cpf_value = None
            if 'cpf' in candidate:
                cpf_value = safe_value(candidate.get('cpf'))
                if cpf_value:
                    cpf_value = re.sub(r'[^\d]', '', cpf_value)
            
            name_value = safe_value(candidate.get('name'))
            
            opinion_applicant_raw = candidate.get('opinionOnTheApplicant')
            opinion_applicant_job_raw = candidate.get('opinionOnTheApplicantJob')
            jobs_raw = candidate.get('jobs')
            
            logging.debug(f"Candidato {applicant_code}: opinionOnTheApplicant = {opinion_applicant_raw}")
            logging.debug(f"Candidato {applicant_code}: opinionOnTheApplicantJob = {opinion_applicant_job_raw}")
            logging.debug(f"Candidato {applicant_code}: jobs = {jobs_raw}")
            
            opinion_applicant = clean_value(opinion_applicant_raw)
            opinion_applicant_job = clean_value(opinion_applicant_job_raw)
            jobs_associated = None
            if 'jobs' in candidate and candidate['jobs']:
                if isinstance(candidate['jobs'], list):
                    jobs_associated = json.dumps(candidate['jobs'], ensure_ascii=False)
                else:
                    jobs_associated = safe_value(candidate['jobs'])
            
            logging.debug(f"Candidato {applicant_code}: Valores processados - opinion_applicant: '{opinion_applicant}', opinion_applicant_job: '{opinion_applicant_job}', jobs_associated: '{jobs_associated}'")
            
            try:
                cursor.execute(insert_query, (
                    name_value,
                    safe_value(candidate.get('createdAt')),
                    salary_payroll_value,
                    gender_value,
                    country_name,
                    country_code,
                    uf_value,
                    state_name,
                    city,
                    postal_code,
                    logradouro,
                    complemento,
                    neighborhood,
                    source,
                    disabled_person,
                    type_of_disability,
                    position,
                    category,
                    salary_claim_value,
                    available_for_trips,
                    moving_availability,
                    education,
                    applicant_code,
                    safe_value(candidate.get('stage')),
                    job_code,
                    previously_worked,
                    cpf_value,
                    total_registros,
                    period,
                    opinion_applicant,
                    opinion_applicant_job,
                    jobs_associated
                ))
                
                processed_ids.add(applicant_code)
                
                count_inserted += 1
                
                logging.debug(f"Candidato {applicant_code}: Inserido com sucesso no banco de dados")
                
                if count_inserted % 10 == 0:
                    cursor.commit()
            except Exception as e:
                logging.error(f"Erro ao inserir candidato {applicant_code}: {e}")
                try:
                    logging.error(f"Candidato problemático: {json.dumps(candidate, ensure_ascii=False)[:500]}")
                except:
                    logging.error("Não foi possível serializar o candidato problemático")
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
        logging.error(f"Erro ao salvar dados: {error_type} - {error_message}")
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

def process_period(period, session, cursor, json_dir, is_first_period, processed_ids):
    period_name = period["name"]
    period_dir = os.path.join(json_dir, period_name)
    os.makedirs(period_dir, exist_ok=True)
    logging.info(f"Iniciando processamento do período {period_name}")
    
    period_body = body_template.copy()
    period_body["lastUpdatedAtFrom"] = period["lastUpdatedAtFrom"]
    period_body["lastUpdatedAtTo"] = period["lastUpdatedAtTo"]
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
        
        logging.info(f"Período {period_name}: Total de {total_records} registros, {total_pages} páginas")
        
        if data.get('data') and len(data['data']) > 0:
            first_candidate = data['data'][0]
            has_opinion = 'opinionOnTheApplicant' in first_candidate
            has_opinion_job = 'opinionOnTheApplicantJob' in first_candidate
            has_jobs = 'jobs' in first_candidate
            logging.info(f"Campos especiais na API - opinionOnTheApplicant: {has_opinion}, opinionOnTheApplicantJob: {has_opinion_job}, jobs: {has_jobs}")
        
        if total_pages > max_pages:
            logging.warning(f"⚠️  ATENÇÃO: Período {period_name} tem {total_pages} páginas, mas a API limita em {max_pages} páginas (10.000 registros)")
            logging.warning(f"Processaremos apenas as primeiras {max_pages} páginas. Considere dividir este período em intervalos menores.")
            total_pages = max_pages
        
        output_path = os.path.join(period_dir, f"candidatos_compleo_page{current_page}.json")
        with open(output_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        
        if not save_to_database(data, table_name, cursor, is_first_page=is_first_period, processed_ids=processed_ids):
            return 0
        
        candidates_page1 = data.get('data', [])
        candidates_count = len(candidates_page1)
        for candidate in candidates_page1:
            if candidate.get('applicantCode'):
                total_processed += 1
        
        logging.info(f"Página {current_page}/{total_pages}: {candidates_count} candidatos processados")
        
        has_more_pages = candidates_count > 0 and current_page < total_pages
        
        while has_more_pages and current_page < max_pages and current_page < total_pages:
            current_page += 1
            period_body["pagination"]["currentPage"] = current_page
            
            if current_page >= max_pages:
                logging.warning(f"Atingindo limite de páginas ({max_pages}) para o período {period_name}. Parando aqui.")
                break
            
            success, data = make_api_request_with_retry(session, url, period_body)
            
            if success:
                if isinstance(data, dict):
                    data['period'] = period_name
                
                output_path = os.path.join(period_dir, f"candidatos_compleo_page{current_page}.json")
                with open(output_path, "w", encoding="utf-8") as json_file:
                    json.dump(data, json_file, ensure_ascii=False, indent=4)
                
                if save_to_database(data, table_name, cursor, is_first_page=False, processed_ids=processed_ids):
                    candidates = data.get('data', [])
                    candidates_count = len(candidates)
                    
                    page_processed = 0
                    for candidate in candidates:
                        if candidate.get('applicantCode'):
                            page_processed += 1
                    
                    total_processed += page_processed
                    logging.info(f"Página {current_page}/{total_pages}: {page_processed} candidatos processados (total: {total_processed})")
                    
                    has_more_pages = candidates_count > 0 and current_page < total_pages and current_page < max_pages
                else:
                    logging.error(f"Erro ao salvar dados da página {current_page} do período {period_name}")
                    break
            else:
                logging.error(f"Erro na requisição da página {current_page} do período {period_name}: {data}")
                break
            
            time.sleep(0.025)
        
        if current_page >= max_pages and current_page < total_pages:
            logging.warning(f"Processamento do período {period_name} interrompido no limite de {max_pages} páginas")
            logging.warning(f"Foram processadas {total_processed} registros de um total estimado de {total_records}")
        
        try:
            cursor.execute(f"""
                SELECT codigoDoCandidato, COUNT(*) as count 
                FROM {table_name} 
                WHERE periodo = '{period_name}' 
                GROUP BY codigoDoCandidato 
                HAVING COUNT(*) > 1
            """)
            duplicates = cursor.fetchall() or []
            duplicates_count = len(duplicates)
            
            if duplicates_count > 0:
                logging.warning(f"Verificação final: {duplicates_count} códigos de candidatos duplicados encontrados")
                logging.info(f"Removendo duplicidades para {duplicates_count} códigos de candidatos")
                for dup_code, count in duplicates:
                    try:
                        cursor.execute(f"""
                            WITH cte AS (
                                SELECT id, ROW_NUMBER() OVER (PARTITION BY codigoDoCandidato ORDER BY id DESC) as rn
                                FROM {table_name}
                                WHERE codigoDoCandidato = ?
                            )
                            DELETE FROM cte WHERE rn > 1
                        """, (dup_code,))
                        cursor.commit()
                    except Exception as e:
                        logging.error(f"Erro ao remover duplicidade para código {dup_code}: {e}")
        except Exception as e:
            logging.error(f"Erro ao verificar duplicidades: {e}")
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE periodo = '{period_name}'")
            result = cursor.fetchone()
            period_total = result[0] if result else 0
            
            cursor.execute(f"SELECT COUNT(DISTINCT codigoDoCandidato) FROM {table_name} WHERE periodo = '{period_name}'")
            result = cursor.fetchone()
            unique_codes = result[0] if result else 0
            
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE periodo = '{period_name}' AND [Parecer sobre o candidato] IS NOT NULL AND [Parecer sobre o candidato] != ''")
            result = cursor.fetchone()
            opinion_count = result[0] if result else 0
            
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE periodo = '{period_name}' AND [Parecer sobre o candidato (Vaga)] IS NOT NULL AND [Parecer sobre o candidato (Vaga)] != ''")
            result = cursor.fetchone()
            opinion_job_count = result[0] if result else 0
            
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE periodo = '{period_name}' AND [Vagas associadas ao candidato] IS NOT NULL AND [Vagas associadas ao candidato] != ''")
            result = cursor.fetchone()
            jobs_count = result[0] if result else 0
            
            logging.info(f"Processamento do período {period_name} concluído: {period_total} registros inseridos ({unique_codes} códigos únicos) em {current_page} páginas")
            logging.info(f"Campos especiais preenchidos - Parecer candidato: {opinion_count}, Parecer vaga: {opinion_job_count}, Vagas associadas: {jobs_count}")
            
            cursor.execute(f"SELECT TOP 3 codigoDoCandidato, [Parecer sobre o candidato], [Parecer sobre o candidato (Vaga)], [Vagas associadas ao candidato] FROM {table_name} WHERE periodo = '{period_name}'")
            samples = cursor.fetchall() or []
            logging.debug("Exemplos de dados inseridos:")
            for sample in samples:
                logging.debug(f"Código: {sample[0]}, Parecer: {sample[1]}, Parecer Vaga: {sample[2]}, Vagas: {sample[3]}")
            
            return period_total
        except Exception as e:
            logging.error(f"Erro ao gerar relatório final do período {period_name}: {e}")
            return total_processed
    else:
        error_message = f"Erro na requisição inicial do período {period_name}: {data}"
        logging.error(error_message)
        return 0

def process_candidatos_compleo():
    total_all_periods = 0
    start_time = time.time()
    start_datetime = datetime.now()
    logging.info(f"Início da execução: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_dir = os.path.join(os.path.dirname(script_dir), "Jsons", "listaCandidatos")
    
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
            
            try:
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
                
                total_all_periods = 0
                
                logging.info(f"Processando {len(periods)} períodos iniciais para evitar limite de 10.000 registros da API")
                
                expanded_periods = []
                for period in periods:
                    subdivided = check_and_subdivide_period_if_needed(period, session, url, body_template, headers)
                    expanded_periods.extend(subdivided)
                
                logging.info(f"Após verificação automática: {len(expanded_periods)} períodos serão processados")
                
                for i, period in enumerate(expanded_periods):
                    logging.info(f"📊 Progresso: Processando período {i+1}/{len(expanded_periods)} - {period['name']}")
                    
                    period_total = process_period(
                        period=period,
                        session=session,
                        cursor=cursor,
                        json_dir=json_dir,
                        is_first_period=(i == 0),
                        processed_ids=processed_ids
                    )
                    total_all_periods += period_total
                    
                    progress_pct = ((i + 1) / len(expanded_periods)) * 100
                    logging.info(f"✅ Período {period['name']} concluído: {period_total} registros | Progresso total: {progress_pct:.1f}%")
                    
                    if i < len(expanded_periods) - 1:
                        logging.info(f"Aguardando 0.25 segundos antes de iniciar próximo período...")
                        time.sleep(0.25)
                
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    result = cursor.fetchone()
                    total_count = result[0] if result else 0
                    
                    cursor.execute(f"SELECT COUNT(DISTINCT codigoDoCandidato) FROM {table_name}")
                    result = cursor.fetchone()
                    unique_codes = result[0] if result else 0
                    
                    cursor.execute(f"""
                        SELECT codigoDoCandidato, COUNT(*) as count 
                        FROM {table_name} 
                        GROUP BY codigoDoCandidato 
                        HAVING COUNT(*) > 1
                    """)
                    duplicates = cursor.fetchall() or []
                    
                    duplicate_info = ""
                    if duplicates:
                        duplicate_info = f"\nATENÇÃO: Encontrados {len(duplicates)} códigos de candidatos duplicados"
                    
                    integrity_message = (
                        f"Verificação de integridade:\n"
                        f"- Total de registros: {total_count}\n"
                        f"- Códigos de candidatos únicos: {unique_codes}\n"
                        f"- Diferença: {total_count - unique_codes} registros"
                        f"{duplicate_info}"
                    )
                    
                    logging.info(integrity_message)
                    
                    if duplicates:
                        logging.warning(f"Atenção: {len(duplicates)} códigos de candidatos duplicados encontrados")
                except Exception as e:
                    logging.error(f"Erro ao realizar verificação de integridade: {e}")
                
                try:
                    cursor.execute(f"SELECT periodo, COUNT(*) as total FROM {table_name} GROUP BY periodo")
                    period_counts = cursor.fetchall() or []
                    
                    summary = "Resumo do processamento:\n"
                    for period_name, count in period_counts:
                        summary += f"- {period_name}: {count} registros\n"
                    summary += f"Total geral: {total_all_periods} registros"
                    
                    logging.info(summary)
                    
                except Exception as e:
                    logging.error(f"Erro ao gerar resumo final: {e}")
                
                conn.close()
            except pyodbc.Error as e:
                logging.error(f"Erro ao conectar ao banco de dados: {e}")
                
                logging.info("Tentando processar apenas os dados da API sem salvar no banco...")
                
                for i, period in enumerate(periods):
                    try:
                        period_name = period["name"]
                        period_dir = os.path.join(json_dir, period_name)
                        os.makedirs(period_dir, exist_ok=True)
                        logging.info(f"Iniciando processamento do período {period_name}")
                        
                        period_body = body_template.copy()
                        period_body["lastUpdatedAtFrom"] = period["lastUpdatedAtFrom"]
                        period_body["lastUpdatedAtTo"] = period["lastUpdatedAtTo"]
                        period_body["pagination"]["currentPage"] = 1
                        period_body["pagination"]["pageSize"] = 10
                        
                        success, data = make_api_request_with_retry(session, url, period_body)
                        
                        if success:
                            output_path = os.path.join(period_dir, f"candidatos_compleo_page1.json")
                            with open(output_path, "w", encoding="utf-8") as json_file:
                                json.dump(data, json_file, ensure_ascii=False, indent=4)
                            
                            candidates_count = len(data.get('data', []))
                            total_all_periods += candidates_count
                            logging.info(f"Salvos {candidates_count} candidatos do período {period_name} em {output_path}")
                    except Exception as e:
                        logging.error(f"Erro ao processar período {period.get('name')}: {e}")

    except Exception as e:
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        execution_time = time.time() - start_time
        minutes, seconds = divmod(execution_time, 60)
        error_message = (f"Erro geral no processamento: {e} "
                         f"Arquivo: listaCandidatos.py, Hora: {error_time}, "
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

def extrair_valor_numerico(valor):
    if not valor:
        return None
    
    if isinstance(valor, dict) and 'value' in valor:
        valor = valor.get('value', '')
    
    numerico = re.sub(r'[^\d.,]', '', str(valor))
    
    if not numerico:
        return None
        
    try:
        numerico_ponto = numerico.replace(',', '.')
        float(numerico_ponto)
        return numerico_ponto
    except ValueError:
        return None

def extrair_pais_uf(valor):
    if not valor:
        return None, None
    
    match = re.match(r'(.*?)\s*\(([A-Z]{2,})\)', valor)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return valor, None

def extrair_estado_uf(valor):
    if not valor:
        return None, None
    
    match = re.match(r'(.*?)\s*\(([A-Z]{2})\)', valor)
    if match:
        return match.group(2).strip(), match.group(1).strip()
    return None, valor

def remover_parenteses(valor):
    if not valor:
        return None
    
    return re.sub(r'\s*\([^)]*\)', '', str(valor)).strip()

if __name__ == "__main__":
    print("Iniciando processamento de candidatos da Compleo...")
    total_inseridos = process_candidatos_compleo()
    print(f"Processamento concluído! {total_inseridos} registros inseridos.")

