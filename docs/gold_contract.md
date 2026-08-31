# Contrato Gold — br_inep_alfabetizacao

Contrato entre a pipeline da Fase 2 (este repositório) e o consumo analítico / ML
da Fase 3.

Path: `s3://{bucket}/gold/br_inep_alfabetizacao/`

## Domínio de `rede` (obrigatório)

Valores canônicos (lowercase, sem acento):

| Código INEP (as-is) | Texto (to-be) |
|---------------------|---------------|
| `1` | `federal` |
| `2` | `estadual` |
| `3` | `municipal` |
| `4` | `privada` |
| `5` | `publica` |

Também aceitos se já vierem em texto: `municipal`, `estadual`, `federal`,
`privada`, `publica` (inclui normalização de `pública`).

Aplica-se a **todas** as tabelas Gold com coluna `rede`, e à Silver via
`standardize_common` / `normalize_rede`.

## `id_municipio`

Sempre **string com 7 dígitos** (zero-pad). Implementado em `standardize_common`.

## Tabelas Gold

| Tabela | Grain | Papel |
|--------|-------|-------|
| `contexto_territorio` | `ano` × `id_municipio` × `rede` | Contexto territorial + socio + metas |
| `alunos_features` | aluno (`id_aluno`) | Fato ML com join materializado ao contexto |
| `alunos_analytic` | aluno | Visão ML sem pipeline / leakage |
| `indicador_crianca_alfabetizada_municipio` | `ano` × `id_municipio` × `rede` | Indicador + gaps |
| `indicador_crianca_alfabetizada_uf` | `ano` × `sigla_uf` × `rede` | Indicador + gaps |

Particionamento Hive: `ano=YYYY/` (coluna `ano` presente no DataFrame na escrita).

## Join aluno → contexto

Chaves: `(ano, id_municipio, rede)` — todas padronizadas.

- `alunos_features` materializa o left join.
- Cobertura mínima exigida pelos testes de qualidade: **80%** (`_join_match`
  ou `nome_municipio` preenchido).
- `contexto_territorio` inclui chaves presentes em `meta_municipio` **e** em
  `alunos`, para cobrir redes sem meta oficial.

## Leakage / colunas excluídas de `alunos_analytic`

Removidas da visão ML:

- Metadados de pipeline: `_ingestion_*`, `_silver_*`, `_gold_*`, `_batch_id`,
  `_source_table`, `_join_match`, …
- `nivel_alfabetizacao`, `proficiencia` (mesmo ciclo avaliativo do target)
- `id_escola`, `serie` (não necessários ao contrato mínimo)

Target: `alfabetizado` ∈ `{0.0, 1.0}`.

## Testes de qualidade

Módulo: `ingestion/gold/quality.py` (executado no fim do job, salvo
`--skip-quality`).

Cobertura unitária: `tests/gold/test_quality.py`, `tests/gold/test_transforms.py`,
`tests/silver/test_transforms.py`.

## Reprocessamento

```bash
python -m ingestion.gold.main --datasets all --years 2023,2024
```

Depois do merge, reexecutar o job Gold no Databricks para republicar as tabelas
no datalake.
