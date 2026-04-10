## 🏗️ Arquitetura do Pipeline

![Arquitetura do Pipeline](./images/Fluxo_Pipeline.svg)

O pipeline inicia com a **Ingestão** de dados brutos provenientes de uma API e arquivos JSON;

Em seguida são armazenados na camada **RAW** mantendo a originalidade.

Após isso vem a etapa **Transform**, onde é realizado o tratamento de inconsistencias como por exemplo:
 - colunas com valores nulos,
 - dados incorretos ou inválidos,
 - padronização de strings,
 - formatação de datas,
 - bytes incorretos (problemas de encoding).

Com os dados devidamente tratados, eles são armazenados na camada **Trusted**, onde mantemos a confiabilidade, organização e uma legibilidade limpa e clara para consumo.

Logo na sequencia os dados são preparados na camada **Analytics**, onde são estruturados para análises, gerando métricas e insights de negócios;

E por fim o **Carregamento** em Banco relacional como o PostgreSQL, onde temos todos os dados limpos para agruparmos, realizarmos consultas eficientes e aplicarmos regras de negócios.

Tudo **orquestrado** e automatizado pelo **Apache Airflow**.