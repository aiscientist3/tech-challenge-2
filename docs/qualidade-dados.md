# Qualidade de Dados — Implementação Fase 2 (PDF)

Documentação das alterações realizadas para atender o plano **Regras de Qualidade de Dados (PDF Fase 2)**. Descreve o que mudou, por quê, como funciona e como operar/testar.

---

## Contexto e motivação

O pipeline medallion (Bronze → Silver → Gold) já ingeria e transformava dados INEP, mas **não havia validação de negócio** entre camadas. O catálogo em `docs/catalog/entities/*.yaml` documentava regras de qualidade com `status_implementacao: documentado`, sem código que as executasse.

### Problema anterior


| Camada        | Situação antes                                               |
| ------------- | ------------------------------------------------------------ |
| **Bronze**    | Dados raw; apenas validação técnica mínima no batch          |
| **Silver**    | Padronização, dedup e joins                                  |
| **Gold**      | Indicadores publicados mesmo com inconsistências cross-table |
| **Streaming** | `alunos` ia direto para Silver após `standardize_common`     |




### Objetivo da entrega

Implementar validadores **pandas customizados**, com **quarentena** de linhas inválidas em Delta no S3, mantendo a Bronze como camada raw.

---



## Decisões de arquitetura


| Decisão                | Escolha                               | Motivo                                                      |
| ---------------------- | ------------------------------------- | ----------------------------------------------------------- |
| Engine de validação    | Validadores Python + pandas           | Alinhado ao stack existente; sem nova infra                 |
| Fonte das regras       | `docs/catalog/entities/{entity}.yaml` | Single source of truth já documentado no catálogo           |
| Comportamento em falha | **Quarentena** (não drop silencioso)  | Rastreabilidade e possibilidade de correção/reprocessamento |
| Bronze                 | Sem regras de negócio                 | Medallion: raw na Bronze, qualidade na Silver               |
| Formato quarentena     | Delta append (`schema_mode=merge`)    | Consistente com Bronze/Silver/Gold                          |
| Observabilidade        | CloudWatch `quality_*`                | Reutiliza `ingestion/batch/metrics.py`                      |


---



## Visão do fluxo

```mermaid
Quarentena Delflowchart TD
    Bronze[Bronze raw] --> Std[standardize_common]
    Std --> Dedup[deduplicate]
    Dedup --> Enrich[apply_enrichment]
    Enrich --> QV[validate_entity]
    QV --> Valid[Linhas válidas]
    QV --> Quar[Quarentena Delta]
    Valid --> SilverTable[Silver Delta]
    SilverTable --> GoldJob[Gold job]
    GoldJob --> Build[build_indicador_*]
    Build --> CrossCheck[validate_indicador_*]
    CrossCheck --> GoldValid[Gold publicado]
    CrossCheck --> GoldQuar[quarantine/gold/...]
```





### Responsabilidade por camada


| Camada     | Papel na qualidade                                                     |
| ---------- | ---------------------------------------------------------------------- |
| **Bronze** | Raw; MERGE técnico no streaming; sem FK/domínio                        |
| **Silver** | **Principal** — completude, domínio, FK referencial                    |
| **Gold**   | Consistência entre tabelas — faixas, referência territorial, meta INEP |


---



## Arquivos criados


| Arquivo                                 | Descrição                                                    |
| --------------------------------------- | ------------------------------------------------------------ |
| `ingestion/silver/quality.py`           | Loader YAML + validadores + `QualityResult`                  |
| `ingestion/silver/quarantine_writer.py` | Writer Delta append para quarentena Silver/Gold              |
| `ingestion/gold/quality.py`             | Checks cross-table nos indicadores Gold                      |
| `tests/silver/test_quality.py`          | Testes unitários Silver (completude, unicidade, domínio, FK) |
| `tests/gold/test_quality.py`            | Testes unitários Gold (faixa, referência, meta ausente)      |
| `docs/qualidade-dados.md`               | Este documento                                               |


---



## Arquivos alterados


| Arquivo                                       | Alteração                                                                    |
| --------------------------------------------- | ---------------------------------------------------------------------------- |
| `ingestion/silver/config.py`                  | `QUARANTINE_PREFIX`, `quarantine_table_path()`                               |
| `ingestion/silver/main.py`                    | Integração `validate_entity` + quarentena no pipeline batch                  |
| `ingestion/streaming/silver_stream_writer.py` | Gate de qualidade antes do MERGE Silver (streaming `alunos`)                 |
| `ingestion/streaming/bronze_stream_writer.py` | Carrega referência `municipio` para FK; repassa `bucket` e `references`      |
| `ingestion/streaming/kafka/main.py`           | Passa `bucket` para habilitar quarentena no streaming                        |
| `ingestion/gold/main.py`                      | Valida indicadores antes de publicar; quarentena Gold                        |
| `ingestion/batch/metrics.py`                  | `publish_quality_metrics()` — `quality_quarantine_rows`, `quality_pass_rate` |
| `requirements.txt`                            | Dependência `pyyaml>=6.0.1`                                                  |
| `docs/catalog/entities/alunos.yaml`           | Regra referencial `alunos_municipio_fk`; status `implementado`               |
| `docs/catalog/entities/meta_municipio.yaml`   | Regra `meta_municipio_fk`; status `implementado`                             |
| `docs/catalog/entities/meta_uf.yaml`          | Regra `meta_uf_sigla_fk`; status `implementado`                              |
| `docs/catalog/entities/uf.yaml`               | status `implementado`                                                        |
| `docs/catalog/entities/municipio.yaml`        | status `implementado`                                                        |
| `docs/catalog/entities/meta_brasil.yaml`      | status `implementado`                                                        |


---



## Módulo Silver — `ingestion/silver/quality.py`



### API principal

- `load_quality_rules(entity_name)` — lê `docs/catalog/entities/{entity}.yaml` → `qualidade.regras`
- `validate_entity(df, entity_name, references)` — retorna `QualityResult`:
  - `valid_df` — linhas que passam em **todas** as regras
  - `quarantine_df` — rejeitadas + metadados de auditoria
  - `summary` — contagem por `rule_id` (log e métricas)
  - `total_input`, `quarantine_count`



### Tipos de regra (YAML → implementação)


| `tipo` no YAML | O que valida      | Implementação                                        |
| -------------- | ----------------- | ---------------------------------------------------- |
| `completude`   | Valores ausentes  | `coluna` not null após `standardize_common`          |
| `dominio`      | Formato           | Regex (`id_municipio` = 7 dígitos)                   |
| `referencial`  | Chave estrangeira | Valor de `coluna` ∈ `references[ref_table][ref_col]` |


Unicidade é tratada por `deduplicate()` no pipeline, não no módulo de qualidade.

### Metadados na quarentena

Colunas adicionadas às linhas rejeitadas:


| Coluna              | Conteúdo                          |
| ------------------- | --------------------------------- |
| `_quality_rule_ids` | IDs das regras violadas (vírgula) |
| `_quality_messages` | Descrições das regras             |
| `_quarantined_at`   | Timestamp UTC ISO 8601            |




### Log por entidade

Exemplo de saída:

```
Quality alunos: 12 quarantined / 5000 total (alunos_ano_obrigatorio: 3, alunos_municipio_fk: 9)
```



### Regras referenciais

Definidas no catálogo YAML de cada entidade:


| Regra                 | Entidade       | Coluna         | Referência               |
| --------------------- | -------------- | -------------- | ------------------------ |
| `alunos_municipio_fk` | alunos         | `id_municipio` | `municipio.id_municipio` |
| `meta_municipio_fk`   | meta_municipio | `id_municipio` | `municipio.id_municipio` |
| `meta_uf_sigla_fk`    | meta_uf        | `sigla_uf`     | `uf.sigla`               |


**Nota:** Se a tabela de referência não estiver disponível em `references`, o check referencial é **pulado** (log only).

---



## Quarentena — `ingestion/silver/quarantine_writer.py`



### Paths S3

```
s3://{bucket}/quarantine/br_inep_alfabetizacao/silver/{entity}/ano=...
s3://{bucket}/quarantine/br_inep_alfabetizacao/gold/{dataset}/ano=...
```

Prefix configurável via env: `QUARANTINE_PREFIX` (default: `quarantine/br_inep_alfabetizacao`).

### Comportamento de escrita

- **Mode:** `append`
- **Schema:** `merge` (aceita novas colunas de auditoria)
- **Partição:** `ano` quando a coluna existe
- **Metadados:** `_{layer}_batch_id`, `_quarantine_layer`

---



## Integração Silver batch — `ingestion/silver/main.py`

Ordem do pipeline (inalterada na essência, com novo passo no final):

```
Bronze read → standardize_common → deduplicate → apply_enrichment → validate_entity → write Silver
                                                              └→ write quarantine (se houver)
```



### Ordem de processamento das entidades

Mantida a ordem em `ALL_ENTITY_NAMES`: `uf` e `municipio` primeiro (referências para FK e enriquecimento de `meta_*` e `alunos`).

### Métricas

Ao final do job, `publish_quality_metrics()` envia para CloudWatch:

- `quality_quarantine_rows` (por entidade, layer=silver)
- `quality_pass_rate` (válidos / total processado)

---



## Integração Streaming — `alunos`

Fluxo medallion preservado:

```
Kafka → Bronze MERGE → Silver MERGE (somente válidos)
```



### Alterações

1. Após `prepare_alunos_silver_batch()`, chama `validate_entity(..., "alunos", references)`
2. Apenas `valid_df` entra no MERGE Silver
3. `quarantine_df` vai para quarentena append
4. No início do job, `_load_streaming_silver_references()` lê `silver/municipio` para validar FK

Se **todas** as linhas do micro-batch forem quarentenadas, o MERGE Silver é pulado (com warning no log).

---



## Módulo Gold — `ingestion/gold/quality.py`

Validação **após** `build_indicador_municipio` / `build_indicador_uf`, **antes** de publicar no Gold.

### Checks implementados (MVP)


| ID da regra                   | Descrição                                                            |
| ----------------------------- | -------------------------------------------------------------------- |
| `gold_taxa_faixa`             | `taxa_crianca_alfabetizada` entre 0 e 100                            |
| `gold_referencia_territorial` | `id_municipio` ou `sigla_uf` existe na referência Silver `municipio` |
| `gold_meta_ausente`           | `taxa_alfabetizacao` (INEP oficial) ausente após join com meta       |


Indicadores inconsistentes vão para `quarantine/.../gold/{dataset}`; apenas `valid_df` é publicado no Gold.

---



## Catálogo YAML — atualizações

Regras referenciais adicionadas onde faltavam:

`alunos.yaml`**:**

```yaml
- id: alunos_municipio_fk
  tipo: referencial
  coluna: id_municipio
  referencia: municipio.id_municipio
```

`meta_municipio.yaml` e `meta_uf.yaml`**:** regras análogas para FK territorial.

`status_implementacao` atualizado para `implementado` nas entidades cobertas pelo MVP.

---



## Observabilidade — CloudWatch

Função: `ingestion/batch/metrics.py` → `publish_quality_metrics()`


| Métrica                   | Dimensões                  | Significado                           |
| ------------------------- | -------------------------- | ------------------------------------- |
| `quality_quarantine_rows` | Environment, Layer, Entity | Linhas enviadas à quarentena          |
| `quality_pass_rate`       | Environment, Layer, Entity | Proporção de linhas válidas (0.0–1.0) |


Desabilitar com `DISABLE_CLOUDWATCH_METRICS=true`.

Namespace: `CLOUDWATCH_METRIC_NAMESPACE` (default: `TechChallenge2/BatchIngestion`).

---



## Dependências

Adicionado ao `requirements.txt`:

```
pyyaml>=6.0.1
```

Necessário para `load_quality_rules()` ler o catálogo YAML em runtime.

---



## Testes



### Executar

```powershell
python -m pytest tests/silver/test_quality.py tests/gold/test_quality.py -v
python -m pytest tests/ -q
```



### Cobertura


| Suite                                | Cenários                                                             |
| ------------------------------------ | -------------------------------------------------------------------- |
| `tests/silver/test_quality.py`       | Completude, domínio, FK, regras do YAML                              |
| `tests/gold/test_quality.py`         | Faixa de taxa, referência territorial, meta ausente (log), UF válido |
| `tests/streaming/test_bronze_dlq.py` | Envelope/payload inválido, chaves naturais ausentes                  |


**81 testes** passando na suite completa.

---



## Validações ativas



### Silver


| Tipo            | O que valida                                                       |
| --------------- | ------------------------------------------------------------------ |
| **Completude**  | `ano`, chaves críticas (`id_aluno`, `id_municipio`, `sigla`, etc.) |
| **Domínio**     | `id_municipio` com 7 dígitos                                       |
| **Referencial** | FK territorial (`alunos → municipio`, `meta_* → referência`)       |


Unicidade: `deduplicate()` no pipeline. Se referência FK indisponível: **pula** o check (log).

### Gold


| Regra                         | O que faz                                 |
| ----------------------------- | ----------------------------------------- |
| `gold_taxa_faixa`             | `taxa_crianca_alfabetizada` entre 0 e 100 |
| `gold_referencia_territorial` | Território existe em `municipio`          |


**Log only:** `log_meta_coverage_warning()` para linhas sem taxa INEP oficial.

### Bronze streaming

Eventos Kafka malformados são **rejeitados no parse** (`expand_kafka_microbatch`) com log; não há escrita em quarentena Bronze.

---



## Streaming Silver

`prepare_alunos_silver_batch()` aplica `standardize_common` → `deduplicate` →
metadados Silver → **projeção de colunas** (`project_alunos_for_silver`): remove
linhagem Kafka/evento (`_kafka_*`, `_event_*`) e colunas Bronze-only (`caderno`,
`id_escola`, etc.), mantendo só o subset quality/Gold.

Catálogo Gold em `docs/catalog/gold/` — tipos suportados: `faixa`, `referencial`.

---



## Melhorias entregues (MVP)

1. **Catálogo como código** — regras documentadas no YAML passam a ser executadas; `status_implementacao: implementado` reflete cobertura real.
2. **Separação válido/quarentena** — dados ruins não contaminam Silver/Gold; ficam auditáveis no S3.
3. **Paridade batch/streaming** — `alunos` no streaming passa pelo mesmo gate de
   qualidade do batch Silver; a ingestão de `alunos` é **somente streaming**
   (sem registry batch).
4. **FK territorial** — `alunos.id_municipio` validado contra `municipio`; metas validadas contra diretórios territoriais.
5. **Gold mais confiável** — indicadores fora de faixa ou sem meta INEP não são publicados.
6. **Métricas de qualidade** — visibilidade operacional (quarentena e pass rate) no CloudWatch.
7. **Extensibilidade** — novas regras no YAML (`tipo`, `coluna`, `referencia`) + novos padrões em `_DOMAIN_PATTERNS` sem mudar o pipeline.

---



## O que não foi alterado (por design)

- **Bronze batch/streaming** — permanece raw; MERGE técnico no streaming; sem dedup de negócio
- **Great Expectations / dbt** — não adotados (decisão do plano)
- **Job separado “quality report”** — fora do escopo MVP (opcional no plano completo)
- **Arquivo do plano** — `qualidade_silver_gold_005a5ddc.plan.md` não foi editado

---



## Escopo MVP vs. futuro



### Entregue (MVP PDF)

- [x] Silver: completude, unicidade, domínio, FK para `alunos`, `municipio`, `meta_municipio`, `meta_uf`
- [x] Quarentena funcional (Silver + Gold)
- [x] Gold: checks de faixa, referência territorial, meta ausente e **gap meta vs taxa**
- [x] Métricas CloudWatch
- [x] Testes automatizados



### Possíveis evoluções

- Validação de **totais** (soma de alunos vs. denominadores do indicador)
- Job Databricks dedicado a relatório de qualidade
- Dashboard Terraform para alarmes em `quality_pass_rate` baixo
- Regras opcionais de domínio para `rede` alinhadas a `meta_*`

---



## Operação rápida



### Silver batch

```powershell
python -m ingestion.silver.main --entities all --years 2023,2024
```



### Gold

```powershell
python -m ingestion.gold.main --datasets all --years 2023,2024
```



### Inspecionar quarentena

Paths no S3 (substituir `{bucket}`):

```
s3://{bucket}/quarantine/br_inep_alfabetizacao/bronze/alunos_kafka_dlq/
s3://{bucket}/quarantine/br_inep_alfabetizacao/silver/alunos/
s3://{bucket}/quarantine/br_inep_alfabetizacao/gold/indicador_crianca_alfabetizada_municipio/
```

Colunas úteis para análise: `_quality_rule_ids`, `_quality_messages`, `_quarantined_at`.

---



## Referências

- Plano de implementação: `qualidade_silver_gold_005a5ddc.plan.md` (Cursor plans)
- Catálogo de entidades: `docs/catalog/entities/`
- README do catálogo: `docs/README.md`

