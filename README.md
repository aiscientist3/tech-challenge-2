# Pipeline Híbrido — Análise da Alfabetização no Brasil

Tech Challenge Fase 2 — Pipeline de dados híbrida (Batch + Streaming) com Arquitetura Medalhão, integrando fontes educacionais da [Base dos Dados](https://basedosdados.org) para análise do programa Compromisso Nacional Criança Alfabetizada.

---

## Visão Geral da Arquitetura

```mermaid
flowchart TD
    subgraph fontes [Fontes de Dados]
        BQ["Google BigQuery\n(Base dos Dados)"]
        MSK["Amazon MSK\n(Kafka)"]
    end

    subgraph ingestao [Ingestão]
        Batch["Databricks Job\nPython .py — Batch"]
        Stream["Spark Structured Streaming\nDatabricks — Streaming"]
    end

    subgraph medalhao [Arquitetura Medalhão — S3 + Delta Lake]
        Bronze["Bronze\ndados brutos"]
        Silver["Silver\ndados tratados"]
        Gold["Gold\nindicadores analíticos"]
    end

    subgraph ops [Operações]
        Monitor["CloudWatch +\nDatabricks Alerts"]
        Quality["Great Expectations /\ndbt tests"]
    end

    BQ --> Batch --> Bronze
    MSK --> Stream --> Bronze
    Bronze --> Silver --> Gold
    Batch --> Monitor
    Stream --> Monitor
    Silver --> Quality
    Gold --> Quality
```

---

## Stack Tecnológica

| Camada | Tecnologia |
|---|---|
| Fonte de dados | Google BigQuery (Base dos Dados) |
| Ingestão Batch | Databricks Jobs (Python `.py`) |
| Ingestão Streaming | Amazon MSK (Kafka) + Spark Structured Streaming |
| Storage | AWS S3 + Delta Lake |
| Orquestração | Databricks Workflows |
| Credenciais GCP | Databricks Secret Scope |
| Credenciais AWS | Databricks Secret Scope (Serverless) + IAM Instance Profile (cluster clássico) |
| Qualidade de dados | Great Expectations / dbt tests *(pendente)* |
| Monitoramento | CloudWatch + Databricks email alerts |
| IaC | Terraform |

---

## Estrutura do Projeto

```
tech-challenge-2/
├── ingestion/
│   └── batch/                      ← Ingestão batch (implementado)
│       ├── config.py               ← Parâmetros centralizados
│       ├── connections/
│       │   ├── bigquery_client.py  ← Cliente GCP via Databricks Secrets
│       │   └── spark_s3.py         ← SparkSession com IAM Instance Profile
│       ├── sources/
│       │   ├── base_source.py      ← Classe abstrata + retry
│       │   ├── uf.py
│       │   ├── municipio.py
│       │   ├── meta_brasil.py
│       │   ├── meta_uf.py
│       │   ├── meta_municipio.py
│       │   └── alunos.py
│       ├── bronze_writer.py        ← Gravação Delta Lake no S3
│       └── main.py                 ← Entry point do Databricks Job
├── tests/
│   └── batch/
│       ├── conftest.py
│       └── test_sources.py
├── terraform/                      ← IaC AWS + Databricks Job
│   ├── README.md
│   ├── main.tf
│   ├── modules/
│   │   ├── s3_datalake/
│   │   ├── iam_databricks/
│   │   └── databricks_job/
│   └── terraform.tfvars.example
├── requirements.txt
└── README.md
```

---

## Camada Bronze — Ingestão Batch (Implementado)

### Fontes integradas

| Fonte | Tabela BigQuery | Destino S3 |
|---|---|---|
| UF | `basedosdados.br_bd_diretorios_brasil.uf` | `bronze/.../uf/` |
| Município | `basedosdados.br_bd_diretorios_brasil.municipio` | `bronze/.../municipio/` |
| Meta Brasil | `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil` | `bronze/.../meta_brasil/ano=XXXX/` |
| Meta UF | `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf` | `bronze/.../meta_uf/ano=XXXX/` |
| Meta Município | `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio` | `bronze/.../meta_municipio/ano=XXXX/` |
| Alunos | `basedosdados.br_inep_avaliacao_alfabetizacao.alunos` | `bronze/.../alunos/ano=XXXX/` |

### Metadados adicionados em cada registro

| Coluna | Descrição |
|---|---|
| `_ingestion_timestamp` | Data/hora UTC da ingestão (ISO 8601) |
| `_source_table` | Tabela de origem no BigQuery |
| `_batch_id` | UUID único do lote de ingestão |

### Layout S3 (Bronze)

```
s3://tech-challenge-2-datalake/
└── bronze/
    └── br_inep_alfabetizacao/
        ├── uf/
        ├── municipio/
        ├── meta_brasil/
        │   ├── ano=2023/
        │   └── ano=2024/
        ├── meta_uf/
        │   ├── ano=2023/
        │   └── ano=2024/
        ├── meta_municipio/
        │   ├── ano=2023/
        │   └── ano=2024/
        └── alunos/
            ├── ano=2023/
            └── ano=2024/
```

---

## Configuração de Credenciais

### 1. GCP — BigQuery (Databricks Secret Scope)

```bash
# Criar Secret Scope "gcp" via Databricks CLI
databricks secrets create-scope gcp
databricks secrets put --scope gcp --key project-id \
    --string-value "YOUR_GCP_PROJECT_ID"
databricks secrets put --scope gcp --key service-account-json \
    --string-value "$(cat service-account.json)"
```

Variáveis de ambiente relevantes:

| Variável | Padrão | Descrição |
|---|---|---|
| `GCP_PROJECT_ID_SECRET_KEY` | `project-id` | Chave do Secret Scope para o project ID |
| `DATABRICKS_SECRET_SCOPE` | `gcp` | Nome do Secret Scope |
| `DATABRICKS_SECRET_KEY` | `service-account-json` | Chave do JSON da service account |

**Fallback para desenvolvimento local:**
```bash
# Opção 1 — arquivo
export GOOGLE_APPLICATION_CREDENTIALS="/caminho/para/service-account.json"

# Opção 2 — JSON inline
export GCP_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
```

### 2. AWS — S3

#### Serverless (produção atual)

O batch Bronze em **Databricks Serverless** usa credenciais via **Secret Scope** (IAM User + access keys provisionados pelo Terraform):

```bash
databricks secrets create-scope aws
databricks secrets put --scope aws --key s3-bucket --string-value "YOUR_BUCKET_NAME"
databricks secrets put --scope aws --key access-key-id --string-value "AKIA..."
databricks secrets put --scope aws --key secret-access-key --string-value "..."
```

Após `terraform apply`, use os comandos documentados em [`terraform/README.md`](terraform/README.md).

| Secret Scope | Chave | Descrição |
|---|---|---|
| `aws` | `s3-bucket` | Nome do bucket S3 |
| `aws` | `access-key-id` | Access key do IAM User |
| `aws` | `secret-access-key` | Secret key do IAM User |

#### Cluster clássico (Instance Profile)

Para clusters clássicos ou streaming futuro, associe o **Instance Profile** criado pelo Terraform (`tech-challenge-2-<env>-databricks-s3-profile`) ao cluster Databricks. A política S3 mínima:

```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:DeleteObject"],
  "Resource": [
    "arn:aws:s3:::YOUR_BUCKET_NAME",
    "arn:aws:s3:::YOUR_BUCKET_NAME/*"
  ]
}
```

Variáveis de ambiente relevantes:

| Variável | Padrão | Descrição |
|---|---|---|
| `S3_BUCKET` | *(via Secret Scope)* | Bucket de destino |
| `BRONZE_PREFIX` | `bronze/br_inep_alfabetizacao` | Prefixo da camada Bronze |
| `AWS_DEFAULT_REGION` | `us-east-1` | Região AWS |
| `S3_ENDPOINT` | *(vazio)* | Endpoint alternativo (MinIO / LocalStack) |

---

## Como Executar

### Instalação local (desenvolvimento)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Testes unitários

```bash
pytest tests/batch/ -v
```

### Databricks Job (produção)

**Task type:** Python script
**Caminho:** `ingestion/batch/main.py`

```bash
# Ingerir todas as fontes, anos 2023 e 2024
python -m ingestion.batch.main --sources all --years 2023,2024

# Fontes específicas
python -m ingestion.batch.main --sources uf,meta_brasil --years 2024

# Modo desenvolvimento — limitar linhas por fonte
python -m ingestion.batch.main --sources meta_municipio --years 2024 --row-limit 10000

# Modo append (preserva dados existentes)
python -m ingestion.batch.main --sources alunos --years 2024 --append
```

### Databricks Widgets (alternativa via UI)

| Widget | Exemplo | Descrição |
|---|---|---|
| `sources` | `all` | Fontes separadas por vírgula ou `all` |
| `years` | `2023,2024` | Anos a ingerir |
| `batch_id` | *(vazio)* | UUID gerado automaticamente se vazio |
| `row_limit` | `10000` | Limite de linhas (vazio = sem limite) |
| `overwrite` | `true` | `false` para modo append |

---

## Boas Práticas Aplicadas (Batch Bronze)

- Scripts Python versionáveis (sem notebooks em produção)
- Credenciais via Databricks Secret Scope e IAM (zero secrets hardcoded)
- Classe base com contrato, retry exponencial e logging padronizado
- Configuração centralizada desacoplada de código
- Metadados de auditoria (`_ingestion_timestamp`, `_source_table`, `_batch_id`)
- Particionamento Delta Lake por `ano` para leituras seletivas nas camadas Silver/Gold
- Validação de schema mínimo antes da gravação
- Testes unitários com mocks — sem dependência de serviços de nuvem
- Parametrização via CLI e Databricks Widgets

---

## FinOps

- Filtrar por `ano` no BigQuery antes de transferir dados
- Usar `--row-limit` durante desenvolvimento para reduzir custo por query
- Particionar Delta por `ano` no S3 para varreduras seletivas
- Lifecycle policy no S3 para dados Bronze antigos (via Terraform — transição para IA após 90 dias)
- Reservar slots BigQuery para workloads recorrentes *(a avaliar)*

---

## TODO — Próximas Entregas

### Silver — Transformações e Joins
- [ ] Padronizar schemas entre fontes (tipos, nomes de colunas)
- [ ] Deduplicar registros por chave natural
- [ ] Join `meta_municipio` + `municipio` (enriquecer com dados territoriais)
- [ ] Join `meta_uf` + `uf`
- [ ] Gravar camada Silver em `s3://.../silver/`

### Gold — Indicadores Analíticos
- [ ] Calcular Indicador Criança Alfabetizada por UF e município
- [ ] Comparar resultados vs. metas (gap analysis)
- [ ] Séries temporais de evolução (2023–2030)
- [ ] Tabelas prontas para BI/dashboard

### Streaming — Kafka + Spark Structured Streaming
- [ ] Provisionar Amazon MSK
- [ ] Producer Python simulando chegada de microdados de alunos
- [ ] Consumer Spark Structured Streaming no Databricks
- [ ] Persistência de eventos na camada Bronze/Silver

### Qualidade de Dados
- [ ] Great Expectations ou dbt tests na Silver
- [ ] Validação de completude, unicidade e domínios
- [ ] Alertas automáticos em falhas de qualidade

### Monitoramento
- [x] Métricas de execução por job (registros, duração, falhas) — CloudWatch custom metrics + dashboard
- [x] CloudWatch Alarms — falha, duração, zero registros, S3 5xx
- [x] Databricks job alerts (e-mail)

Ver [`terraform/README.md`](terraform/README.md#monitoring) para configurar `alert_email` e confirmar a subscrição SNS.

### IaC — Terraform
- [x] Bucket S3 + lifecycle policies
- [x] IAM roles e Instance Profile
- [x] Definições de Databricks Workflows
- [ ] Amazon MSK cluster

Ver [`terraform/README.md`](terraform/README.md) para aplicar a infraestrutura.

---

## Referências

- [Base dos Dados — Avaliação da Alfabetização](https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72)
- [Compromisso Nacional Criança Alfabetizada — Inep](https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-e-avaliacao/avaliacao-da-alfabetizacao)
- [Documentação BigQuery — Base dos Dados](https://basedosdados.org/docs/access_data_bq)
- [Delta Lake Documentation](https://docs.delta.io)
- [Databricks Secrets Guide](https://docs.databricks.com/en/security/secrets/index.html)
