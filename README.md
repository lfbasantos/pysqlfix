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
1) verificar dados de acesso ao ambiente na pasta _conf local (essa pasta contém dados de acesso ao banco e não é sincronizada no github)
2) Validar se o arquivo 'param/param.py' está configurado como DEBUG=0 (Debug = 1 não realiza os deletes no banco, e deve ser usado para validar os counts do script)
3) Após realizar as manutenções podem ser executadas as seguintes consultas para validar os resultados:

````
select 
	count(*), year(dt_ins), month(dt_ins)
from
	iiot_rest_in
group by
	year(dt_ins), month(dt_ins)
order by
	year(dt_ins), month(dt_ins)
````

````
select 
	count(*), year(dt_ins), month(dt_ins)
from
	iiot_rest_in_history
group by
	year(dt_ins), month(dt_ins)
order by
	year(dt_ins), month(dt_ins)
````

````
select 
	count(*), year(dt_msg), month(dt_msg)
from
	iiot_mensagem_in_v2
group by
	year(dt_msg), month(dt_msg)
order by
	year(dt_msg), month(dt_msg)
````

````
select 
	count(*), year(dt_msg), month(dt_msg)
from
	iiot_mensagem_in_v2_history
group by
	year(dt_msg), month(dt_msg)
order by
	year(dt_msg), month(dt_msg)
````