#
## Imports
import os
import mysql.connector
from datetime import datetime

#
## Import Params
from _conf.conf import DBUSER, DBPASS, DBBASE, DBHOST, PATHLOG

#
## Inicializacao de Variaveis
horaAtual = datetime.now().strftime('%H:%M')
timestampLog = datetime.now().strftime('%Y%m%d_%H%M%S')
logName = "pysqlfix_" + timestampLog + ".log"
fileLog = open(PATHLOG + logName, "w")

#
## Conexao com a base
conectaBase = mysql.connector.connect(
  host=DBHOST,
  user=DBUSER,
  passwd=DBPASS,
  database=DBBASE
)

#
## Funcoes
def gravaLog(tp, fileLog, timestampLog, msg):
  hr = datetime.now().strftime('%Y%m%d_%H%M%S')
  if tp==1:
    flag_tp = "[i]"
  elif tp==2:
    flag_tp = "[k]"
  elif tp==3:
    flag_tp = "[w]"
  elif tp==4:
    flag_tp = "[E]"
  else:
    flag_tp = "[X]"
  fileLog.write(timestampLog + "|" + hr + ": "+ flag_tp + ":" + msg + "\n")
  print(timestampLog + "|" + hr + "|"+ flag_tp + "|" + msg)

#
##
msg="Iniciando manutencao no banco de dados"
gravaLog(1, fileLog, timestampLog, msg)

#
## Ano mes atual
def getAnoMes(conectaBase):
  sqlCursor = conectaBase.cursor()
  sqlQuery = "select year(now()) ano, month(now()) mes"
  sqlCursor.execute(sqlQuery)
  sqlDados = sqlCursor.fetchall()
  for sqlRecord in sqlDados:
    ano = sqlRecord[0]
    mes = sqlRecord[1]
  sqlCursor.close()
  return ano,mes
ano, mes = getAnoMes(conectaBase)
#
##
msg="Ano, Mes: " + str(ano) + "-" + str(mes)
gravaLog(1,fileLog, timestampLog, msg)

#
## Count Rest IN
def countRest(conectaBase, ano, mes):
  sqlCursor = conectaBase.cursor()
  sqlQuery = "select count(*) as qtd from integraiot.iiot_rest_in where year(dt_ins) = " + str(ano) + " and month(dt_ins) = " + str(mes)
  sqlCursor.execute(sqlQuery)
  sqlDados = sqlCursor.fetchall()
  for sqlRecord in sqlDados:
    qtd = sqlRecord[0]
  sqlCursor.close()
  return qtd

#
## Count Mensagem IN
def countMensagem(conectaBase, ano, mes):
  sqlCursor = conectaBase.cursor()
  sqlQuery = "select count(*) as qtd from integraiot.iiot_mensagem_in_v2  where year(dt_msg) = " + str(ano) + " and month(dt_msg) = " + str(mes)
  sqlCursor.execute(sqlQuery)
  sqlDados = sqlCursor.fetchall()
  for sqlRecord in sqlDados:
    qtd = sqlRecord[0]
  sqlCursor.close()
  return qtd

#
## Count History Rest IN
def countHistRest(conectaBase, ano, mes):
  sqlCursor = conectaBase.cursor()
  sqlQuery = "select count(*) as qtd from integraiot.iiot_rest_in_history where year(dt_ins) = " + str(ano) + " and month(dt_ins) = " + str(mes)
  sqlCursor.execute(sqlQuery)
  sqlDados = sqlCursor.fetchall()
  for sqlRecord in sqlDados:
    qtd = sqlRecord[0]
  sqlCursor.close()
  return qtd

#
## Count History Mensagem IN
def countHistMensagem(conectaBase, ano, mes):
  sqlCursor = conectaBase.cursor()
  sqlQuery = "select count(*) as qtd from integraiot.iiot_mensagem_in_v2_history  where year(dt_msg) = " + str(ano) + " and month(dt_msg) = " + str(mes)
  sqlCursor.execute(sqlQuery)
  sqlDados = sqlCursor.fetchall()
  for sqlRecord in sqlDados:
    qtd = sqlRecord[0]
  sqlCursor.close()
  return qtd

#
## Delete Rest In
def deleteRestIn(conectaBase, ano, mes):
  msg="Iniciando DELETE..."
  gravaLog(1,fileLog, timestampLog, msg)
  sqlCursor = conectaBase.cursor()
  sqlQuery = "delete from integraiot.iiot_rest_in  where year(dt_ins) = " + str(ano) + " and month(dt_ins) = " + str(mes)
  sqlCursor.execute(sqlQuery)
  conectaBase.commit()
  sqlCursor.close()
  msg="Registros deletados com sucesso."
  gravaLog(1,fileLog, timestampLog, msg)

#
## Delete Mensagem In
def deleteMensagemIn(conectaBase, ano, mes):
  sqlCursor = conectaBase.cursor()
  sqlQuery = "delete from integraiot.iiot_mensagem_in_v2  where year(dt_msg) = " + str(ano) + " and month(dt_msg) = " + str(mes)
  sqlCursor.execute(sqlQuery)
  conectaBase.commit()
  sqlCursor.close()
  msg="Registros deletados com sucesso."
  gravaLog(1,fileLog, timestampLog, msg)

#
## Manutencao de Disco
def optimizeTable(tbl):
  msg="Iniciando Optimize..." + tbl
  gravaLog(1, fileLog, timestampLog, msg)
  cmd = "c:/xampp/mysql/bin/mysqlcheck -u root -p\"1q2wAZSX3e4r\" integraiot " + tbl
  os.system(cmd)
  msg="Optimize finalizado."
  gravaLog(1, fileLog, timestampLog, msg)



#
# Manutencao (Com Delete)
def manutDB(ano, mes, fileLog, timestampLog):
  msg="Validando Ano Mes: " +str(ano)+"-"+str(mes)
  gravaLog(1,fileLog, timestampLog, msg)
  gravaLog(1,fileLog, timestampLog, "Contando Rest...")
  countRestIn = countRest(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_rest_in: " + str(countRestIn)
  gravaLog(1,fileLog, timestampLog, msg)
  #
  gravaLog(1,fileLog, timestampLog, "Contando Mensagem...")
  countMensagemIn = countMensagem(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_mensagem_in_v2: " + str(countMensagemIn)
  gravaLog(1,fileLog, timestampLog, msg)
  #
  gravaLog(1,fileLog, timestampLog, "Contando Rest History...")
  countHistRestIn = countHistRest(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_rest_in_history: " + str(countHistRestIn)
  gravaLog(1,fileLog, timestampLog, msg)
  #
  gravaLog(1,fileLog, timestampLog, "Contando Mensagem History...")
  countHistMensagemIn = countHistMensagem(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_mensagem_in_v2_history: " + str(countHistMensagemIn)
  gravaLog(1,fileLog, timestampLog, msg)

  #
  ## A partir de Mes-3 realiza manutencao na tabela principal
  ## desde que o histórico esteja correto.
  msg="Validando Mensagem e Mensagem History para: " +str(ano)+"-"+str(mes)
  gravaLog(1,fileLog, timestampLog, msg)
  if countMensagemIn > 0:
    if countMensagemIn == countHistMensagemIn:
      msg="Quantidade de registros em mensagem e mensagem_history e igual para: " +str(ano)+"-"+str(mes)
      gravaLog(1,fileLog, timestampLog, msg)
      msg="Realizando manutencao em mensagem para: " +str(ano)+"-"+str(mes)
      gravaLog(1,fileLog, timestampLog, msg)
      deleteMensagemIn(conectaBase, ano, mes)
    else:
      msg="ERRO verificar mensagem history <> mensagem para: " +str(ano)+"-"+str(mes)
      gravaLog(4,fileLog, timestampLog, msg)
  else:
    msg="Tabela Mensagem IN OK para:" +str(ano)+"-"+str(mes)
    gravaLog(2,fileLog, timestampLog, msg)

  msg="Validando Rest e Rest History para: " +str(ano)+"-"+str(mes)
  gravaLog(1,fileLog, timestampLog, msg)
  if countRestIn > 0:
    if countRestIn == countHistRestIn:
      msg="Quantidade de registros em rest e rest_history e igual para: " +str(ano)+"-"+str(mes)
      gravaLog(1,fileLog, timestampLog, msg)
      msg="Realizando manutencao em rest para: " +str(ano)+"-"+str(mes)
      gravaLog(1,fileLog, timestampLog, msg)
      deleteRestIn(conectaBase, ano, mes)
    else:
      msg="ERRO verificarrest history <> rest para: " +str(ano)+"-"+str(mes)
      gravaLog(4,fileLog, timestampLog, msg)
  else:
    msg="Tabela Rest IN OK para:" +str(ano)+"-"+str(mes)
    gravaLog(2,fileLog, timestampLog, msg)

#
# Check (Sem Delete)
def checkDB(ano, mes, fileLog, timestampLog):
  msg="Validando Ano Mes: " +str(ano)+"-"+str(mes)
  gravaLog(1,fileLog, timestampLog, msg)
  gravaLog(1,fileLog, timestampLog, "Contando Rest...")
  countRestIn = countRest(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_rest_in: " + str(countRestIn)
  gravaLog(1,fileLog, timestampLog, msg)
  #
  gravaLog(1,fileLog, timestampLog, "Contando Mensagem...")
  countMensagemIn = countMensagem(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_mensagem_in_v2: " + str(countMensagemIn)
  gravaLog(1,fileLog, timestampLog, msg)
  #
  gravaLog(1,fileLog, timestampLog, "Contando Rest History...")
  countHistRestIn = countHistRest(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_rest_in_history: " + str(countHistRestIn)
  gravaLog(1,fileLog, timestampLog, msg)
  #
  gravaLog(1,fileLog, timestampLog, "Contando Mensagem History...")
  countHistMensagemIn = countHistMensagem(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_mensagem_in_v2_history: " + str(countHistMensagemIn)
  gravaLog(1,fileLog, timestampLog, msg)

  msg="Validando Mensagem e Mensagem History para: " +str(ano)+"-"+str(mes)
  gravaLog(1,fileLog, timestampLog, msg)
  if countMensagemIn == countHistMensagemIn:
    msg="Quantidade de registros em mensagem e mensagem_history e igual para: " +str(ano)+"-"+str(mes)
    gravaLog(2,fileLog, timestampLog, msg)
  else:
    msg="ERRO verificar mensagem history <> mensagem para: " +str(ano)+"-"+str(mes)
    gravaLog(4, fileLog, timestampLog, msg)

  msg="Validando Rest e Rest History para: " +str(ano)+"-"+str(mes)
  gravaLog(1, fileLog, timestampLog, msg)
  if countRestIn == countHistRestIn:
    msg="Quantidade de registros em Rest e Rest_history e igual para: " +str(ano)+"-"+str(mes)
    gravaLog(2, fileLog, timestampLog, msg)
  else:
    msg="ERRO verificar rest history <> Rest para: " +str(ano)+"-"+str(mes)
    gravaLog(4, fileLog, timestampLog, msg)

#
## Check (Mes Atual -1 e Mes Atual -2)
mes=mes-1
checkDB(ano, mes, fileLog, timestampLog)
mes=mes-1
checkDB(ano, mes, fileLog, timestampLog)

#
## Manut (Mes Atual -3 e Mes Atual -4)
mes=mes-1
manutDB(ano, mes, fileLog, timestampLog)
mes=mes-1
manutDB(ano, mes, fileLog, timestampLog)

#
## Manutencao de Disco
optimizeTable("iiot_rest_in")
optimizeTable("iiot_mensagem_in_v2")
