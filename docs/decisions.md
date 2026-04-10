# 👨🏽‍⚖️ Decisões tomadas

## Por que PostgreSQL?

O PostgreSQL é amplamente considerado um dos bancos de dados mais robustos e avançados para engenharia de dados, oferecendo um equilíbrio entre confiabilidade relacional tradicional e flexibilidade moderna.

**Principais vantagens:**
 - Alta Extensibilidade e Funcionalidade,
 - Conformidade ACID e Confiabilidade,
 - Alto Desempenho e Escalabilidade
 - Poderoso Suporte a SQL e Analíticos,
 - Integração com Ecossistema Python,
 - Código Aberto e Baixo Custo.

## Por que Parquet?

Ideal para Big Data. por ter uma fácil compactação, facilitando a manipulação em enormes quantidades, as principais vantagens :
 - alta taxa de compressão (reduzindo custos de armazenamento),
 - leitura eficiente apenas das colunas necessárias (melhorando a velocidade de consultas analíticas)
 - amplamente superior a CSV/JSON em grandes volumes.

## Por que Airflow?

Por ser uma ferramenta poderosa e robusta que permite criar workflows dinâmicos e adaptáveis usando Python, facilitando a manutenção e versionamento, e possui uma ótima interface gráfica que possibilita um monitoramento visual.

(Além de ser a primeira ferramente de orquestração que eu aprendi a usar como desenvolvedor ETL)

## Por que incremental?

Para termos uma pipeline eficiente evitando processar dados históricos repetidamente, sendo veloz e otimizada com menor carga no sitema de origem, economia de banda e capacidade de processamento.