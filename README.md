# Otimização de Processamento de Dados — PySpark & Spark SQL

## Objetivo do Projeto
Estruturar e otimizar um pipeline de processamento de dados em escala utilizando **PySpark** e **Spark SQL**, integrando bases colunares de vídeos e comentários (`Parquet`) e aplicando técnicas de particionamento, análise de plano de execução e poda de colunas para reduzir custo computacional e evitar redundância de dados.

## Ferramentas Utilizadas
* **Python (Google Colab)**
* **Apache Spark (PySpark & Spark SQL)**
* **Formato Parquet (Compressão Snappy)**

## Pipeline e Etapas de Otimização
O fluxo de processamento foi estruturado em etapas progressivas para comparar o desempenho e o plano físico de execução do Spark:

1. **Leitura e Exploração de Schemas:** Ingestão dos arquivos `videos-preparados.snappy.parquet` e `videos-comments-tratados.snappy.parquet` via `spark.read.parquet`, validando tipos de dados e estruturas vetoriais (`Features PCA`, `Features Normal`, `Features`).
2. **Consultas Relacionais com Spark SQL:** Criação de *Temporary Views* (`videos` e `comments`) e execução de `INNER JOIN` pela chave `Video ID` utilizando `spark.sql`.
3. **Gerenciamento de Partições (`repartition` e `coalesce`):** Redistribuição dos DataFrames pela chave de junção (`repartition("Video ID")`) seguida da redução controlada de partições com `coalesce(4)` para minimizar operações de *shuffle*.
4. **Análise de Plano de Execução (`.explain(True)`):** Inspeção dos planos lógico e físico (*Parsed*, *Analyzed*, *Optimized Logical Plan* e *Physical Plan*) para identificar estratégias como `BroadcastHashJoin`, `Exchange hashpartitioning` e `PushedFilters`.
5. **Refatoração Final e Persistência Otimizada:** Seleção explícita apenas das colunas necessárias (eliminando colunas duplicadas da tabela de comentários), filtragem preventiva de chaves nulas (`IS NOT NULL`) e escrita final em formato colunar **Parquet** (`join-videos-comments-otimizado`).

## Principais Ganhos Técnicos Observados
* **Column Pruning (Poda de Colunas):** No plano físico final (`FileScan parquet`), o Spark lê apenas 4 colunas da base de comentários (`Video ID`, `Comment`, `Sentiment`, `Likes Comment`) em vez de todas as 12 colunas originais, reduzindo drasticamente o tráfego de I/O e memória.
* **Predicate Pushdown:** Aplicação de filtros `IsNotNull` empurrados diretamente para a leitura dos arquivos Parquet antes da execução do `BroadcastHashJoin`.
* **Persistência Colunar:** Armazenamento da base consolidada em Parquet, preservando os tipos complexos (vetores) e garantindo alta compressão para leituras analíticas futuras.

## Arquivos do Projeto
* `otimizacao.ipynb` — Notebook completo com o pipeline PySpark, consultas Spark SQL, comparativo de planos de execução (`.explain(True)`) e persistência em Parquet.
