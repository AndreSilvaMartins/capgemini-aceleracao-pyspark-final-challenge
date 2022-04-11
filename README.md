# Capgemini - Aceleração PySpark 2022

Este projeto é parte do Programa de Aceleração [PySpark](https://spark.apache.org) da [Capgemini Brasil](https://www.capgemini.com/br-pt).
[<img src="https://www.capgemini.com/wp-content/themes/capgemini-komposite/assets/images/logo.svg" align="right" width="140">](https://www.capgemini.com/br-pt)

## Sobre

Este projeto é o desafio final que, de maneira interdisciplinar, visa colocar em prática o conhecimento das últimas semanas ao realizar tarefas que buscam garantir a qualidade dos dados e sua transformação eficiente para responder a perguntas de negócio, além de praticar outros conhecimentos como o de Linux e o Git.

## Dependências

Nesta versão dos desafios da Aceleração Pyspark, utilizaremos scripts PySpark criado diretamente em python e versionar o código conforme as tarefas avançam. No momento não há outra dependência alé do [Spark instalado localmente](https://spark.apache.org/downloads.html).

## Estrutura de diretórios

```
├ /home/spark/capgemini-aceleracao-pyspark
|
├── LICENSE
├── README.md
|
├── data                    <- Diretório contendo os dados brutos.
|   ├── census-income
│       ├── census-income.csv
│       ├── census-income.names
|   ├── communities-crime
│       ├── communities-crime.csv
│       ├── communities.names
|   ├── online-retail
│       ├── online-retail.csv
│       ├── online-retail.names
│
├── scripts                    <- Contém respostas de negócio baseadas em dados.
│   ├── online-retail.py
│   ├── communities-crime.py
│   ├── census-income.py
```

## Inicializa os servidores Master e Worker do Spark
/opt/spark/sbin/start-master.sh && /opt/spark/sbin/start-worker.sh spark://spark:7077


## Abra a interface web do Spark
firefox http://127.0.0.1:8080/ &

## Executa o script no Spark
spark-submit --master spark://spark:7077 capgemini-aceleracao-pyspark/scripts/<script>.py 2> /dev/null

