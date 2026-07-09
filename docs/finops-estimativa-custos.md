# Estimativa de Custo Teórica — FinOps

Estimativa simulada de custos do pipeline híbrido (Batch + Streaming) de
**Análise da Alfabetização no Brasil** (Tech Challenge Fase 2).

**Nuvem de referência:** AWS (S3, CloudWatch, SNS) + Databricks Serverless sobre AWS.  
**Fonte de dados:** Google BigQuery (Base dos Dados) — cobrada à parte na GCP.  
**Região de referência:** `us-east-1`.  
**Data de referência dos preços:** ordem de grandeza (jul/2026) — validar no Cost Explorer / calculadora oficial.

Práticas FinOps da arquitetura: ver seção [FinOps no README](../README.md#finops--otimização-de-custos-da-arquitetura).

---

## Premissas de volumetria

Volumetrias típicas para dados da Base dos Dados (Avaliação da Alfabetização /
diretórios Brasil), já em **Parquet + Snappy** (Delta Lake):

| Ativo | Volume estimado (comprimido) |
|---|---|
| Diretórios UF/Município | ~5–20 MB |
| Metas Brasil/UF/Município (2 anos) | ~50–200 MB |
| Microdados `alunos` (2 anos, amostra operacional do challenge) | ~8–15 GB Bronze |
| Silver (tratado + enriquecido) | ~6–12 GB |
| Gold (indicadores UF/município) | ~50–300 MB |
| Quarentena (`quarantine/...`, append-only) | ~50 MB – 1 GB* |
| Checkpoints / logs / versões Delta | ~1–2 GB |

\*Depende da taxa de rejeição das regras de qualidade. Com pass rate alto (&gt;95%),
fica na casa de dezenas/centenas de MB. Com dados muito sujos, pode crescer sem
lifecycle dedicado.

---

## Impacto FinOps das entregas de qualidade

Alterações recentes (validators Silver/Gold, quarentena Delta, DLQ Bronze lean,
métricas CloudWatch) mudam o perfil de custo assim:

| Mudança | Efeito no custo | Magnitude típica (challenge) |
|---|---|---|
| Validação de qualidade no job Silver/Gold/streaming | +DBU (CPU pandas + joins referenciais) | +5–15% na duração do job |
| Escrita em `quarantine/` (append Delta) | +S3 storage + PUT requests | **~US$ 0,01–0,05 / mês** se pass rate alto |
| DLQ Bronze streaming **log only** (sem S3) | Evita storage de payload inválido | Economia vs DLQ persistido |
| `publish_quality_metrics` (CloudWatch) | +`PutMetricData` | **&lt; US$ 0,50 / mês** |
| Só linhas válidas na Silver/Gold | Menos reprocessamento / BI sujo | Economia indireta de DBU |
| Gold às 08:00 (após Silver 07:00) | Evita Gold sobre Silver incompleto | Evita runs “à toa” |

**Conclusão:** o impacto monetário direto é **baixo** no cenário acadêmico; o
ganho FinOps principal é **evitar desperdício downstream** e ter sinais
(`quality_pass_rate`) para agir antes que a quarentena engorde o lake.

---

## 1. Custos de armazenamento (Bronze, Silver, Gold, Quarentena)

| Camada | Volume | Classe | Preço ref. | Custo/mês |
|---|---|---|---|---|
| **Bronze** (hot, &lt;90 dias) | 12 GB | S3 Standard (~US$ 0,023/GB) | 12 × 0,023 | **~US$ 0,28** |
| **Bronze** (cold, após lifecycle) | 12 GB | S3 Standard-IA (~US$ 0,0125/GB) | 12 × 0,0125 | **~US$ 0,15** |
| **Silver** | 10 GB | S3 Standard | 10 × 0,023 | **~US$ 0,23** |
| **Gold** | 0,3 GB | S3 Standard | 0,3 × 0,023 | **~US$ 0,01** |
| **Quarentena** (`quarantine/`) | 0,1–1 GB | S3 Standard (sem lifecycle ainda) | 0,1–1 × 0,023 | **~US$ 0,00–0,02** |
| Requests + versionamento (margem) | — | GET/PUT/LIST | — | **~US$ 0,50–1,50** |
| **Subtotal storage (cenário steady)** | | | | **~US$ 1–3 / mês** |

### Notas

- Com lifecycle `bronze/` → STANDARD_IA após 90 dias (Terraform), o raw fica ~45% mais barato após o período hot.
- Em escala acadêmica, o storage é **quase irrelevante** frente ao compute (Databricks DBUs).
- Silver/Gold permanecem em STANDARD por serem hot (consultas e reprocessamentos frequentes).
- **Quarentena** é append-only e **ainda não tem regra de lifecycle** no Terraform — se `quality_quarantine_rows` subir, priorize IA após 30–90 dias ou expiração após revisão.
- **DLQ Bronze** não entra na tabela de storage: eventos malformados não são gravados no S3 (modo lean).

---

## 2. Custos de processamento Batch (execuções pontuais)

**Premissa:** 1 pipeline completo/dia em dias úteis (~22 runs/mês), Databricks Serverless.  
Durações abaixo **já incluem** overhead típico de qualidade + quarentena.

| Job | Duração média | DBU ref.* | Custo/run | Runs/mês | Custo/mês |
|---|---|---|---|---|---|
| Bronze (metas + diretórios) | 8–15 min | ~0,5–1,5 DBU | ~US$ 0,40–1,20 | 22 | **~US$ 9–26** |
| Silver (transforms + joins + quality) | 12–25 min | ~1,1–2,3 DBU | ~US$ 0,80–1,80 | 22 | **~US$ 18–40** |
| Gold (indicadores + quality) | 4–10 min | ~0,4–1,0 DBU | ~US$ 0,25–0,75 | 22 | **~US$ 6–17** |
| BigQuery (bytes scanned, 2 anos filtrados) | — | ~2–8 GB billed | ~US$ 0,01–0,05/run | 22 | **~US$ 0,20–1** |
| **Subtotal batch** | | | | | **~US$ 33–84 / mês** |

\*DBU Serverless Jobs: use o preço do seu workspace (ordem ~US$ 0,70–0,90/DBU; varia por região/contrato).

### Cenário acadêmico enxuto

| Cenário | Frequência | Anos | Custo batch/mês |
|---|---|---|---|
| Desenvolvimento / defesa | 1–2 runs/semana | só 2024 | **~US$ 6–18** |
| Operação diária | 22 runs/mês | 2023–2024 | **~US$ 33–84** |

---

## 3. Custos de processamento Streaming (micro-batches)

**Premissa alinhada ao código:** não é stream contínuo 24×7; é job com
`Trigger.AvailableNow` sob demanda ou agendado. Cada micro-batch válido pode
acionar quality Silver + append de quarentena (se houver rejeições).

| Cenário | Frequência | Duração/run | Custo/run | Custo/mês |
|---|---|---|---|---|
| **A — Dev/demo** (manual ou 1–2×/dia) | 30 runs | 3–7 min | ~US$ 0,15–0,45 | **~US$ 5–14** |
| **B — Quase tempo real** (a cada 5 min, 12h/dia) | ~4.000 runs* | 1–3 min (backlog pequeno) | ~US$ 0,05–0,15 | **~US$ 200–600** |
| **C — Contínuo clássico (anti-padrão)** | cluster 24×7 | idle + compute | — | **US$ 800–2.000+** |
| Kafka broker (EC2 t3.small, se self-managed) | 730 h | — | ~US$ 0,0208/h | **~US$ 15** (+ EBS/ops) |
| MSK Serverless (se migrar) | cluster ligado o mês todo | — | ~US$ 0,75/cluster-h + GB | **~US$ 550+** (piso) |
| DLQ Bronze persistido no S3 *(não adotado)* | — | — | — | evitado pelo modo lean |

\*Muitos runs curtos: o custo sobe por *overhead* de start de job. Para o challenge, o **cenário A** (ou schedule a cada 30–60 min) é o sweet spot FinOps.

---

## 4. Resumo consolidado

### Cenário recomendado do challenge

| Componente | Estimativa mensal (US$) |
|---|---|
| Armazenamento S3 (Bronze IA + Silver + Gold + quarentena) | 1 – 3 |
| Batch Serverless (Bronze + Silver + Gold, com quality) | 33 – 84 |
| Streaming micro-batch (cenário A — acadêmico) | 5 – 14 |
| Kafka EC2 (opcional) | ~15 |
| CloudWatch + SNS (incl. métricas de qualidade) | 1 – 4 |
| BigQuery (fonte, filtrado por ano) | &lt; 1 |
| **Total estimado** | **~US$ 55 – 120 / mês** |

### Comparativo de cenários

| Cenário | Total/mês | Quando usar |
|---|---|---|
| **Mínimo acadêmico** (poucos runs, 1 ano) | **~US$ 18 – 40** | Desenvolvimento e defesa |
| **Operação diária batch + stream leve** | **~US$ 55 – 120** | Demo contínua controlada |
| **Stream a cada 5 min + batch diário** | **~US$ 260 – 720** | Só se latência exigir |
| **Cluster clássico 24×7** | **US$ 1.000+** | Evitar — pior TCO |

---

## 5. Alavancas que mais impactam a fatura

1. **Frequência do job de streaming** (maior alavanca de custo).
2. **Anos ingeridos** (`--years`) e volume de `alunos`.
3. **Serverless vs cluster sempre ligado**.
4. **Taxa de quarentena** — pass rate baixo aumenta appends em `quarantine/` e duração dos jobs.
5. **Lifecycle Bronze → IA** (e, no futuro, lifecycle da quarentena).
6. **Pushdown + Gold agregada** (reduz DBU das consultas recorrentes).

---

## 6. Como validar na prática

| Ação | Ferramenta |
|---|---|
| Custo AWS por serviço | AWS Cost Explorer (filtro por tags `Project` / `Environment`) |
| DBU Databricks | Databricks Account Console → Usage |
| Bytes billed BigQuery | GCP Billing + `INFORMATION_SCHEMA.JOBS` |
| Crescimento do lake | S3 Storage Lens / Inventory por prefixo `bronze/`, `silver/`, `gold/`, `quarantine/` |
| Regressão de duração | CloudWatch metric `DurationSeconds` + alarme |
| Saúde de qualidade | CloudWatch `quality_quarantine_rows` e `quality_pass_rate` (dimensões `Layer`, `Entity`) |