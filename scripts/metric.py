import requests
import json
import logging
import time
import os
import re
import pyodbc
import warnings
from datetime import datetime, timedelta
from dotenv import load_dotenv

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

load_dotenv()

server = os.getenv('server')
database = os.getenv('database')
username = os.getenv('username')
password = os.getenv('password')
COMPLEO_API_TOKEN = os.getenv('COMPLEO_API_TOKEN')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler()])

table_name = 'api_Metricas'
url = "https://api.compleo.app/public/jobmetrics/100056"

body_template = {
    "fields": [
        "cf_clienteBranch",
        "cf_clienteCostCenter",
        "cf_clienteJobTitle",
        "cf_clienteTimeShift",
        "location",
        "numberOfPositions",
        "openingDate",
        "openingReason",
        "salaryRange",
        "hiringEndDate",
        "warningDaysBeforeEndDate",
        "dangerDaysBeforeEndDate",
        "status",
        "createdAt",
        "lastUpdatedAt",
        "createdByUser",
        "lastUpdatedByUser",
        "title",
        "customer",
        "visibility",
        "category",
        "highlightOnCareerSite",
        "justForPCD",
        "employmentType",
        "branch",
        "experienceLevel",
        "mainRecruiter",
        "workingModel",
        "recruiters",
        "requester",
        "otherRequesters",
        "JobStatusHistory",
        "currentStage",
        "jobCode",
        "jobNumber",
        "notes",
        "total",
        "totalFiltered"
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

def generate_periods():
    start_date_env = os.getenv('START_DATE')
    end_date_env = os.getenv('END_DATE')
    period_mode = os.getenv('PERIOD_MODE', 'yearly')
    
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
            current_date = start_date
            
            while current_date <= end_date:
                if current_date.month == 12:
                    month_end = datetime(current_date.year, 12, 31)
                else:
                    next_month = datetime(current_date.year, current_date.month + 1, 1)
                    month_end = next_month - timedelta(days=1)
                
                if month_end > end_date:
                    month_end = end_date
                
                periods.append({
                    "name": current_date.strftime("%Y-%m"),
                    "lastUpdatedAtFrom": current_date.strftime("%Y-%m-%dT00:00:00Z"),
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
        logging.warning("START_DATE e END_DATE não configurados no .env. Usando períodos padrão.")
        periods = [
            {
                "name": "2026",
                "lastUpdatedAtFrom": "2026-01-01T00:00:00Z",
                "lastUpdatedAtTo": "2026-12-31T23:59:59Z"
            }
        ]
    
    logging.info(f"Gerados {len(periods)} períodos: {[p['name'] for p in periods]}")
    return periods

periods = generate_periods()

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
                    numeroDaVaga NVARCHAR(100),
                    titulo NVARCHAR(200),
                    status NVARCHAR(100),
                    dataCriacao NVARCHAR(100),
                    dataAtualizacao NVARCHAR(100),
                    criadoPor NVARCHAR(100),
                    atualizadoPor NVARCHAR(100),
                    dataAbertura NVARCHAR(100),
                    empresa NVARCHAR(200),
                    cnpj NVARCHAR(20),
                    centroCusto NVARCHAR(100),
                    codCentroCusto NVARCHAR(20),
                    cargo NVARCHAR(100),
                    codCargo NVARCHAR(20),
                    turno NVARCHAR(200),
                    codTurno NVARCHAR(20),
                    categoria NVARCHAR(100),
                    tipoContratacao NVARCHAR(100),
                    pcd BIT,
                    nivelExperiencia NVARCHAR(100),
                    tipoJustificativa NVARCHAR(100),
                    salarioMinimo FLOAT,
                    salarioMaximo FLOAT,
                    moeda NVARCHAR(10),
                    posicoes INT,
                    dataLimiteContratacao NVARCHAR(100),
                    diasAlerta INT,
                    diasUrgencia INT,
                    tipoCandidatura NVARCHAR(100),
                    descTipoCandidatura NVARCHAR(100),
                    destacadoSiteCarreiras BIT,
                    departamento NVARCHAR(100),
                    filial NVARCHAR(100),
                    recrutadorPrincipal NVARCHAR(100),
                    recrutadores NVARCHAR(MAX),
                    solicitante NVARCHAR(100),
                    outrosSolicitantes NVARCHAR(MAX),
                    modeloTrabalho NVARCHAR(100),
                    cep NVARCHAR(20),
                    endereco NVARCHAR(200),
                    complemento NVARCHAR(200),
                    pais NVARCHAR(100),
                    numero NVARCHAR(20),
                    bairro NVARCHAR(100),
                    cidade NVARCHAR(100),
                    uf NVARCHAR(2),
                    estado NVARCHAR(100),
                    etapaMaisAvancada NVARCHAR(100),
                    totalCandidatosEtapaMaisAvancada INT,
                    historicoStatus NVARCHAR(MAX),
                    data_captura DATETIME DEFAULT GETDATE(),
                    periodo NVARCHAR(10),
                    -- Novos campos timeToStatus e jobCode
                    requestedDate NVARCHAR(100),
                    days_requested_to_approvedRequest INT,
                    days_requested_to_disapprovedRequest INT,
                    days_requested_to_open INT,
                    days_approvedRequest_to_open INT,
                    days_open_to_canceled INT,
                    days_open_to_suspended INT,
                    days_open_to_finished INT,
                    days_requested_to_finished INT,
                    days_awaitingCustomer_to_finished INT,
                    days_open_to_awaitingCustomer INT,
                    days_requested_to_awaitingCustomer INT,
                    jobCode NVARCHAR(100),
                    Observações NVARCHAR(MAX),
                    total INT,
                    totalFiltered INT
                )
                """
                cursor.execute(create_table_query)
                cursor.commit()
                logging.info(f"Tabela {table_name} criada com sucesso")
                
                try:
                    index_query = f"CREATE INDEX idx_numeroDaVaga ON {table_name} (numeroDaVaga)"
                    cursor.execute(index_query)
                    cursor.commit()
                    logging.info(f"Índice criado para a coluna numeroDaVaga")
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
        period = data.get('period', '')
        
        if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
            jobs = data['data']
            total_records = data.get('totalFiltered', 0) or data.get('total', 0)
            current_page = data.get('pagination', {}).get('currentPage', 1)

        elif isinstance(data, list):
            jobs = data
            logging.info(f"Lista direta de {len(jobs)} métricas recebida - Período: {period}")
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
            
            job_id = job.get('id', '')
            last_updated_at = safe_value(job.get('lastUpdatedAt', ''))
            created_at = safe_value(job.get('createdAt', ''))
            title = safe_value(job.get('title', ''))
            if title:
                title = title[:20]
            
            unique_identifier = f"{job_code}_{last_updated_at}_{created_at}_{title}"
            
            if unique_identifier in processed_ids:
                count_skipped += 1

                continue
            
            insert_query = f"""
            INSERT INTO {table_name} (
                numeroDaVaga,
                titulo,
                status,
                dataCriacao,
                dataAtualizacao,
                criadoPor,
                atualizadoPor,
                dataAbertura,
                empresa,
                cnpj,
                centroCusto,
                codCentroCusto,
                cargo,
                codCargo,
                turno,
                codTurno,
                categoria,
                tipoContratacao,
                pcd,
                nivelExperiencia,
                tipoJustificativa,
                salarioMinimo,
                salarioMaximo,
                moeda,
                posicoes,
                dataLimiteContratacao,
                diasAlerta,
                diasUrgencia,
                tipoCandidatura,
                descTipoCandidatura,
                destacadoSiteCarreiras,
                departamento,
                filial,
                recrutadorPrincipal,
                recrutadores,
                solicitante,
                outrosSolicitantes,
                modeloTrabalho,
                cep,
                endereco,
                complemento,
                pais,
                numero,
                bairro,
                cidade,
                uf,
                estado,
                etapaMaisAvancada,
                totalCandidatosEtapaMaisAvancada,
                historicoStatus,
                periodo,
                requestedDate,
                days_requested_to_approvedRequest,
                days_requested_to_disapprovedRequest,
                days_requested_to_open,
                days_approvedRequest_to_open,
                days_open_to_canceled,
                days_open_to_suspended,
                days_open_to_finished,
                days_requested_to_finished,
                days_awaitingCustomer_to_finished,
                days_open_to_awaitingCustomer,
                days_requested_to_awaitingCustomer,
                jobCode,
                Observações,
                total,
                totalFiltered
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            try:
                job_number = safe_value(job.get('jobNumber'))
                title = safe_value(job.get('title'))
                status = safe_value(job.get('status'))
                created_at = safe_value(job.get('createdAt'))
                last_updated_at = safe_value(job.get('lastUpdatedAt'))
                created_by_user = safe_value(job.get('createdByUser'))
                last_updated_by_user = safe_value(job.get('lastUpdatedByUser'))
                opening_date = safe_value(job.get('openingDate'))
                
                empresa = None
                cnpj = None
                if 'cf_clienteBranch' in job and job['cf_clienteBranch']:
                    if isinstance(job['cf_clienteBranch'], dict):
                        empresa_cnpj = job['cf_clienteBranch'].get('label', '')
                        if empresa_cnpj:
                            match = re.match(r'(.*?)\s*-\s*\(([^)]+)\)', empresa_cnpj)
                            if match:
                                empresa = match.group(1).strip()
                                cnpj = match.group(2).strip()
                
                centro_custo = None
                cod_centro_custo = None
                if 'cf_clienteCostCenter' in job and job['cf_clienteCostCenter']:
                    if isinstance(job['cf_clienteCostCenter'], dict):
                        cc_value = job['cf_clienteCostCenter'].get('value', '')
                        cc_label = job['cf_clienteCostCenter'].get('label', '')
                        
                        cod_centro_custo = cc_value
                        
                        match = re.match(r'(.*?)\s*-\s*\(([^)]+)\)', cc_label)
                        if match:
                            centro_custo = match.group(1).strip()
                
                cargo = None
                cod_cargo = None
                if 'cf_clienteJobTitle' in job and job['cf_clienteJobTitle']:
                    if isinstance(job['cf_clienteJobTitle'], dict):
                        cargo_value = job['cf_clienteJobTitle'].get('value', '')
                        cargo_label = job['cf_clienteJobTitle'].get('label', '')
                        
                        cod_cargo = cargo_value
                        
                        match = re.match(r'(.*?)\s*-\s*\(([^)]+)\)', cargo_label)
                        if match:
                            cargo = match.group(1).strip()
                
                turno = None
                cod_turno = None
                if 'cf_clienteTimeShift' in job and job['cf_clienteTimeShift']:
                    if isinstance(job['cf_clienteTimeShift'], dict):
                        turno_value = job['cf_clienteTimeShift'].get('value', '')
                        turno_label = job['cf_clienteTimeShift'].get('label-pt-BR', '') or job['cf_clienteTimeShift'].get('label', '')
                        
                        cod_turno = turno_value
                        turno = turno_label
                
                categoria = None
                if 'category' in job and job['category']:
                    if isinstance(job['category'], dict):
                        categoria = job['category'].get('label-pt-BR', '') or job['category'].get('label', '')
                
                tipo_contratacao = None
                if 'employmentType' in job and job['employmentType']:
                    if isinstance(job['employmentType'], dict):
                        tipo_contratacao = job['employmentType'].get('label-pt-BR', '') or job['employmentType'].get('label', '')
                
                pcd = 1 if job.get('justForPCD') else 0
                
                nivel_experiencia = None
                if 'experienceLevel' in job and job['experienceLevel']:
                    if isinstance(job['experienceLevel'], dict):
                        nivel_experiencia = job['experienceLevel'].get('label-pt-BR', '') or job['experienceLevel'].get('label', '')
                
                tipo_justificativa = None
                if 'openingReason' in job and job['openingReason']:
                    if isinstance(job['openingReason'], dict):
                        tipo_justificativa = job['openingReason'].get('label-pt-BR', '') or job['openingReason'].get('label', '')
                
                salario_minimo = None
                salario_maximo = None
                moeda = None
                if 'salaryRange' in job and job['salaryRange']:
                    if isinstance(job['salaryRange'], dict):
                        salario_minimo = job['salaryRange'].get('minValue')
                        salario_maximo = job['salaryRange'].get('maxValue')
                        moeda = job['salaryRange'].get('currency')
                
                posicoes = job.get('numberOfPositions')
                
                data_limite_contratacao = safe_value(job.get('hiringEndDate'))
                dias_alerta = job.get('warningDaysBeforeEndDate')
                dias_urgencia = job.get('dangerDaysBeforeEndDate')
                
                tipo_candidatura = None
                descTipoCandidatura = None
                if 'visibility' in job and job['visibility']:
                    if isinstance(job['visibility'], dict):
                        tipo_candidatura_completo = job['visibility'].get('label-pt-BR', '') or job['visibility'].get('label', '')
                        if tipo_candidatura_completo:
                            match = re.match(r'(.*?)\s*\((.*?)\)', tipo_candidatura_completo)
                            if match:
                                tipo_candidatura = match.group(1).strip()
                                descTipoCandidatura = match.group(2).strip()
                            else:
                                tipo_candidatura = tipo_candidatura_completo
                
                destacado = 1 if job.get('highlightOnCareerSite') else 0
                
                departamento = None
                if 'customer' in job and job['customer']:
                    if isinstance(job['customer'], dict):
                        departamento = job['customer'].get('label')
                
                filial = None
                if 'branch' in job and job['branch']:
                    if isinstance(job['branch'], dict):
                        filial = job['branch'].get('label')
                
                recrutador_principal = None
                if 'mainRecruiter' in job and job['mainRecruiter']:
                    if isinstance(job['mainRecruiter'], dict):
                        recrutador_principal = job['mainRecruiter'].get('label')
                
                recrutadores = None
                if 'recruiters' in job and job['recruiters']:
                    if isinstance(job['recruiters'], list):
                        recrutadores_list = []
                        for rec in job['recruiters']:
                            if isinstance(rec, dict) and 'label' in rec:
                                recrutadores_list.append(rec['label'])
                        if recrutadores_list:
                            if len(recrutadores_list) == 1:
                                recrutadores = recrutadores_list[0]
                            else:
                                recrutadores = ", ".join(recrutadores_list)
                
                solicitante = None
                if 'requester' in job and job['requester']:
                    if isinstance(job['requester'], dict):
                        solicitante = job['requester'].get('label')
                
                outros_solicitantes = None
                if 'otherRequesters' in job and job['otherRequesters']:
                    if isinstance(job['otherRequesters'], list):
                        solicitantes_list = []
                        for sol in job['otherRequesters']:
                            if isinstance(sol, dict) and 'label' in sol:
                                solicitantes_list.append(sol['label'])
                        if solicitantes_list:
                            if len(solicitantes_list) == 1:
                                outros_solicitantes = solicitantes_list[0]
                            else:
                                outros_solicitantes = ", ".join(solicitantes_list)
                
                modelo_trabalho = None
                if 'workingModel' in job and job['workingModel']:
                    if isinstance(job['workingModel'], dict):
                        modelo_trabalho = job['workingModel'].get('label-pt-BR', '') or job['workingModel'].get('label', '')
                
                cep = None
                endereco = None
                complemento = None
                pais = None
                numero = None
                bairro = None
                cidade = None
                uf = None
                estado = None
                
                if 'location' in job and job['location']:
                    loc = job['location']
                    if isinstance(loc, dict):
                        cep = loc.get('postalCode')
                        endereco = loc.get('addressline1')
                        complemento = loc.get('addressline2')
                        numero = loc.get('number')
                        bairro = loc.get('neighborhood')
                        
                        if 'country' in loc and loc['country'] and isinstance(loc['country'], dict):
                            pais = loc['country'].get('label')
                        
                        if 'city' in loc and loc['city'] and isinstance(loc['city'], dict):
                            cidade = loc['city'].get('label')
                            uf = loc['city'].get('uf')
                        
                        if 'provinceOrState' in loc and loc['provinceOrState'] and isinstance(loc['provinceOrState'], dict):
                            estado_completo = loc['provinceOrState'].get('label')
                            if estado_completo:
                                match = re.match(r'(.*?)\s*\(([A-Z]{2})\)', estado_completo)
                                if match:
                                    estado = match.group(1).strip()
                                    if not uf:
                                        uf = match.group(2).strip()
                
                etapa_mais_avancada = None
                total_candidatos_etapa = None
                if 'currentStage' in job and job['currentStage']:
                    if isinstance(job['currentStage'], dict):
                        etapa_mais_avancada = job['currentStage'].get('lastStageWithApplicant')
                        total_candidatos_etapa = job['currentStage'].get('lastStageWithApplicantTotal')
                
                historico_status = safe_value(job.get('JobStatusHistory'))

                time_to_status = job.get('timeToStatus', {}) or {}
                requestedDate = time_to_status.get('requestedDate')
                days_requested_to_approvedRequest = time_to_status.get('days_requested_to_approvedRequest')
                days_requested_to_disapprovedRequest = time_to_status.get('days_requested_to_disapprovedRequest')
                days_requested_to_open = time_to_status.get('days_requested_to_open')
                days_approvedRequest_to_open = time_to_status.get('days_approvedRequest_to_open')
                days_open_to_canceled = time_to_status.get('days_open_to_canceled')
                days_open_to_suspended = time_to_status.get('days_open_to_suspended')
                days_open_to_finished = time_to_status.get('days_open_to_finished')
                days_requested_to_finished = time_to_status.get('days_requested_to_finished')
                days_awaitingCustomer_to_finished = time_to_status.get('days_awaitingCustomer_to_finished')
                days_open_to_awaitingCustomer = time_to_status.get('days_open_to_awaitingCustomer')
                days_requested_to_awaitingCustomer = time_to_status.get('days_requested_to_awaitingCustomer')
                jobCode = job.get('jobCode')
                notes = clean_value(job.get('notes'))

                if isinstance(data, dict):
                    total_value = data.get('total', 0)
                    total_filtered_value = data.get('totalFiltered', 0)
                else:
                    total_value = 0
                    total_filtered_value = 0

                cursor.execute(insert_query, (
                    job_number,
                    title,
                    status,
                    created_at,
                    last_updated_at,
                    created_by_user,
                    last_updated_by_user,
                    opening_date,
                    empresa,
                    cnpj,
                    centro_custo,
                    cod_centro_custo,
                    cargo,
                    cod_cargo,
                    turno,
                    cod_turno,
                    categoria,
                    tipo_contratacao,
                    pcd,
                    nivel_experiencia,
                    tipo_justificativa,
                    salario_minimo,
                    salario_maximo,
                    moeda,
                    posicoes,
                    data_limite_contratacao,
                    dias_alerta,
                    dias_urgencia,
                    tipo_candidatura,
                    descTipoCandidatura,
                    destacado,
                    departamento,
                    filial,
                    recrutador_principal,
                    recrutadores,
                    solicitante,
                    outros_solicitantes,
                    modelo_trabalho,
                    cep,
                    endereco,
                    complemento,
                    pais,
                    numero,
                    bairro,
                    cidade,
                    uf,
                    estado,
                    etapa_mais_avancada,
                    total_candidatos_etapa,
                    historico_status,
                    period,
                    requestedDate,
                    days_requested_to_approvedRequest,
                    days_requested_to_disapprovedRequest,
                    days_requested_to_open,
                    days_approvedRequest_to_open,
                    days_open_to_canceled,
                    days_open_to_suspended,
                    days_open_to_finished,
                    days_requested_to_finished,
                    days_awaitingCustomer_to_finished,
                    days_open_to_awaitingCustomer,
                    days_requested_to_awaitingCustomer,
                    jobCode,
                    notes,
                    total_value,
                    total_filtered_value
                ))
                
                processed_ids.add(unique_identifier)
                
                count_inserted += 1
                
                if count_inserted % 10 == 0:
                    cursor.commit()
            except Exception as e:
                logging.error(f"Erro ao inserir métrica {job_code}: {e}")
                try:
                    logging.error(f"Métrica problemática: {json.dumps(job, ensure_ascii=False)[:500]}")
                except:
                    logging.error("Não foi possível serializar a métrica problemática")
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
        logging.error(f"Erro ao salvar dados de métricas: {error_type} - {error_message}")
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
    max_pages = 1000
    
    if success:
        if isinstance(data, dict):
            data['period'] = period_name
        
        total_records = data.get('totalFiltered', 0) or data.get('total', 0)
        page_size = data.get('pageSize', 50) or 50
        total_pages = (total_records + page_size - 1) // page_size
        
        logging.info(f"Período {period_name}: Total de {total_records} registros, {total_pages} páginas")
        
        output_path = os.path.join(period_dir, f"metricas_compleo_page{current_page}.json")
        with open(output_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        
        if not save_to_database(data, table_name, cursor, is_first_page=is_first_period, processed_ids=processed_ids):
            return 0
        
        metricas_page1 = data.get('data', [])
        metricas_count = len(metricas_page1)
        for metrica in metricas_page1:
            if metrica.get('jobCode'):
                total_processed += 1
        
        logging.info(f"Página {current_page}/{total_pages}: {metricas_count} métricas processadas")
        
        has_more_pages = metricas_count > 0 and current_page < total_pages
        
        while has_more_pages and current_page < max_pages:
            current_page += 1
            period_body["pagination"]["currentPage"] = current_page
            
            success, data = make_api_request_with_retry(session, url, period_body)
            
            if success:
                if isinstance(data, dict):
                    data['period'] = period_name
                
                output_path = os.path.join(period_dir, f"metricas_compleo_page{current_page}.json")
                with open(output_path, "w", encoding="utf-8") as json_file:
                    json.dump(data, json_file, ensure_ascii=False, indent=4)
                
                if save_to_database(data, table_name, cursor, is_first_page=False, processed_ids=processed_ids):
                    metricas = data.get('data', [])
                    metricas_count = len(metricas)
                    
                    page_processed = 0
                    for metrica in metricas:
                        if metrica.get('jobCode'):
                            page_processed += 1
                    
                    total_processed += page_processed
                    logging.info(f"Página {current_page}/{total_pages}: {page_processed} métricas processadas (total: {total_processed})")
                    
                    has_more_pages = metricas_count > 0 and current_page < total_pages
                else:
                    logging.error(f"Erro ao salvar dados da página {current_page} do período {period_name}")
                    break
            else:
                logging.error(f"Erro na requisição da página {current_page} do período {period_name}: {data}")
                break
            
            time.sleep(0.5)
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE periodo = '{period_name}'")
            period_total = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT COUNT(DISTINCT numeroDaVaga) FROM {table_name} WHERE periodo = '{period_name}'")
            unique_codes = cursor.fetchone()[0]
            
            logging.info(f"Processamento do período {period_name} concluído: {period_total} registros inseridos ({unique_codes} códigos únicos) em {current_page} páginas")
            
            return period_total
        except Exception as e:
            logging.error(f"Erro ao gerar relatório para o período {period_name}: {e}")
            return total_processed
    else:
        error_message = f"Erro na requisição inicial do período {period_name}: {data}"
        logging.error(error_message)
        return 0

def process_metricas_compleo():
    start_time = time.time()
    start_datetime = datetime.now()
    logging.info(f"Início da execução: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_dir = os.path.join(os.path.dirname(script_dir), "Jsons", "listaMetricas")
    total_all_periods = 0

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
            
            total_all_periods = 0
            for i, period in enumerate(periods):
                period_total = process_period(
                    period=period,
                    session=session,
                    cursor=cursor,
                    json_dir=json_dir,
                    is_first_period=(i == 0),
                    processed_ids=processed_ids
                )
                total_all_periods += period_total
                
                if i < len(periods) - 1:
                    logging.info(f"Aguardando 5 segundos antes de iniciar próximo período...")
                    time.sleep(5)
            
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                result = cursor.fetchone()
                total_count = result[0] if result else 0
                
                cursor.execute(f"SELECT COUNT(DISTINCT numeroDaVaga) FROM {table_name}")
                result = cursor.fetchone()
                unique_codes = result[0] if result else 0
                
                cursor.execute(f"""
                    SELECT numeroDaVaga, COUNT(*) as count 
                    FROM {table_name} 
                    GROUP BY numeroDaVaga 
                    HAVING COUNT(*) > 1
                """)
                same_code_records = cursor.fetchall() or []
                same_code_count = len(same_code_records)
                
                integrity_message = (
                    f"Estatísticas finais:\n"
                    f"- Total de registros: {total_count}\n"
                    f"- Códigos de vagas únicos: {unique_codes}\n"
                    f"- Métricas com mesmo código: {same_code_count}\n"
                    f"- Registros por código (média): {round(total_count/unique_codes, 2) if unique_codes > 0 else 0}"
                )
                
                logging.info(integrity_message)
            except Exception as e:
                logging.error(f"Erro ao realizar verificação de estatísticas: {e}")
            
            try:
                cursor.execute(f"SELECT periodo, COUNT(*) as total FROM {table_name} GROUP BY periodo")
                period_counts = cursor.fetchall()
                
                summary = "Resumo do processamento:\n"
                for period_name, count in period_counts:
                    summary += f"- {period_name}: {count} registros\n"
                summary += f"Total geral: {total_all_periods} registros"
                
                logging.info(summary)
                
            except Exception as e:
                logging.error(f"Erro ao gerar resumo final: {e}")
            
            conn.close()

    except Exception as e:
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        execution_time = time.time() - start_time
        minutes, seconds = divmod(execution_time, 60)
        error_message = (f"Erro geral no processamento: {e} "
                         f"Arquivo: listaMetricas.py, Hora: {error_time}, "
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
        
        if count_removed > 0:
            logging.info(f"{count_removed} arquivos JSON foram removidos após processamento completo")
            
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
    print("Iniciando processamento de métricas da Compleo...")
    total_inseridos = process_metricas_compleo()
    print(f"Processamento concluído! {total_inseridos} métricas inseridas.")