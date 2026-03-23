# Pipeline de integração API → SQL Server (OXEN)

## Contexto

Scripts Python de **ingestão** para a **OXEN** (empresa fictícia): extraem dados do ATS **Compleo** via API REST pública, gravam cópias em JSON para rastreabilidade e persistem em **Microsoft SQL Server** para uso analítico e operacional em RH/recrutamento.

---

## Funcionamento técnico dos scripts

Cada arquivo em `scripts/` é um **ETL independente** com o mesmo padrão de execução:

1. **Configuração**: `python-dotenv` carrega variáveis de ambiente (`.env`). Conexão SQL usa `DB_SERVER`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` via **pyodbc** com **ODBC Driver 18 for SQL Server** e `TrustServerCertificate=yes` na string de conexão. A API Compleo usa `COMPLEO_API_TOKEN` (Bearer), `COMPLEO_API_BASE_URL` e `COMPLEO_COMPANY_ID` para montar a URL do recurso (`…/branchlist/{id}`, `…/joblist/{id}`, etc.).

2. **Requisição HTTP**: `requests.Session()` mantém a sessão. O script define um `body_template` (lista de `fields`, `sort`, `pagination`) e, em laço, envia **POST** JSON para o endpoint do recurso com cabeçalhos `Content-Type`, `Accept` e `Authorization: Bearer …`.

3. **Paginação e limites**: a resposta traz metadados de paginação; o código avança `currentPage` até esgotar registros. Onde a API impõe teto de volume (por exemplo, ~10.000 itens), o script **quebra o trabalho em períodos** (`START_DATE`, `END_DATE`, `PERIOD_MODE` — anual, mensal ou diário conforme o caso) para gerar várias janelas `lastUpdatedAtFrom` / `lastUpdatedAtTo` e repetir o ciclo por intervalo.

4. **Persistência em disco**: durante o processamento, objetos ou páginas relevantes são serializados em **JSON** sob `Jsons/<subpasta>/` (nome varia por script, ex.: listas de filiais, métricas), permitindo auditoria e reprocessamento.

5. **Persistência no banco**: para cada lote ou registro tratado, o código insere (ou reconcilia) linhas na tabela alvo. Na primeira execução, muitos scripts executam **DDL condicional** (`IF NOT EXISTS … CREATE TABLE`) com esquema alinhado aos campos da API. Alguns fluxos podem recriar ou ajustar tabelas quando a estrutura evolui (comportamento documentado no próprio script).

6. **Observabilidade**: logging em nível `INFO` (e eventualmente `DEBUG`) no console com timestamps, progresso de períodos/páginas e totais inseridos.

### Orquestração com Apache Airflow

Na OXEN, esses scripts **não ficam expostos como serviço**: são **disparados pelo Apache Airflow** em DAGs, **várias vezes ao dia**, conforme a criticidade de cada domínio (dimensões de cadastro, vagas, candidatos, movimentação, métricas). O Airflow apenas agenda e monitora a execução do interpretador Python com o ambiente e segredos configurados no worker; a lógica de negócio permanece nestes scripts.

### Mapeamento script → endpoint → tabela

| Script | Recurso Compleo (trecho da URL) | Tabela SQL (exemplo) | Observação |
|--------|-----------------------------------|----------------------|------------|
| `branchList.py` | `.../branchlist/{id}` | `api_listaFiliais` | Filiais |
| `costumerList.py` | `.../customerlist/{id}` | `api_listaClientes` | Clientes |
| `userList.py` | `.../userlist/{id}` | `api_listaUsuarios` | Usuários |
| `contactList.py` | `.../contactlist/{id}` | `api_listaContatos` | Contatos |
| `vaga.py` | `.../joblist/{id}` | `api_Vagas` | Vagas |
| `jobAdd.py` | `.../joblist/{id}` | `api_criacaoVaga` | Ênfase em criação de vagas (campos e períodos específicos) |
| `candidat.py` | `.../applicantlist/{id}` | `api_Candidatos` | Candidatos |
| `moviment.py` | `.../applicanthistory/{id}` | `api_Moviment` | Histórico de estágios/movimentação |
| `metric.py` | `.../jobmetrics/{id}` | `api_Metricas` | Métricas de vagas |

### Ordem lógica entre pipelines

Não há acoplamento obrigatório no código, mas para **modelagem dimensional** costuma-se rodar primeiro cadastros (filiais, clientes, usuários, contatos), depois vagas (`vaga` / `jobAdd`), em seguida candidatos e movimentação, e por fim métricas — alinhado às dependências analíticas típicas, não a foreign keys geridas por estes scripts.

Scripts com janela temporal via ambiente incluem, entre outros, `metric.py`, `candidat.py`, `jobAdd.py` e `moviment.py`; o modo (`PERIOD_MODE`) e o significado exato da janela variam conforme implementação em cada arquivo.

---

## Tecnologias utilizadas

- **Python 3**
- **Requests** (HTTP)
- **python-dotenv**
- **pyodbc** + **Microsoft ODBC Driver 18 for SQL Server**
- **Microsoft SQL Server**
- **API Compleo** (REST)
- **Apache Airflow** (agendamento e execução periódica em ambiente OXEN)

---

## Segurança (portfólio público)

- Credenciais e tokens devem existir apenas em ambiente segreto (por exemplo variáveis injetadas pelo Airflow ou `.env` local), não no repositório.
- `.env.example` documenta nomes de variáveis com placeholders.
- `COMPLEO_COMPANY_ID` externaliza o identificador da organização na API.
