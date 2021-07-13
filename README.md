# pysqlfix
Database Fixes and Maintenance for MySQL with Python

# Instrucoes
Este script realiza manutenções no banco de dados para o projeto IntegraIoT.

As tabelas iiot_rest_in e iiot_mensagem_in_v2 sao as tabelas de entrada da API dos dispositivos, recebendo as mensagens em tempo real sendo disparadas pelas diversas fontes.

De forma a tornar o sistema mais performático, apenas 3 meses de informações são armazenadas nessas tabelas.

O sistema IntegraIoT também grava automaticamente os mesmos registros nas tabelas "_history".

Mensalmente, é realizada uma manutenção de forma a remove das tabelas iiot_rest_in e iiot_mensagem_in_v2 os registros desnecessários, referentes a Mês Atual - 3 em diante.

Desta maneira, as tabelas "_history" contém todos os dados, e as tabelas "in" irão conter apenas 3 meses de dados.

# lfbasantos@gmail.com
