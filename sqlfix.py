#
## Imports
import os
import mysql.connector
from datetime import datetime

#
## Import Params
from _conf.conf import DBUSER, DBPASS, DBBASE, DBHOST, PATHLOG, MYSQLCHECK
from param.param import DEBUG

#
## Inicializacao de Variaveis
horaAtual = datetime.now().strftime('%H:%M')
timestampLog = datetime.now().strftime('%Y%m%d_%H%M%S')
logName = "pysqlfix_" + timestampLog + ".log"

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
def gravaLog(tp, logName, timestampLog, msg):
  fileLog = open(PATHLOG + logName, "a")
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
  fileLog.close()

#
##
msg="Iniciando manutencao no banco de dados"
gravaLog(1, logName, timestampLog, msg)

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
gravaLog(1,logName, timestampLog, msg)

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
  gravaLog(1,logName, timestampLog, msg)
  sqlCursor = conectaBase.cursor()
  sqlQuery = "delete from integraiot.iiot_rest_in  where year(dt_ins) = " + str(ano) + " and month(dt_ins) = " + str(mes)
  sqlCursor.execute(sqlQuery)
  conectaBase.commit()
  sqlCursor.close()
  msg="Registros deletados com sucesso."
  gravaLog(1,logName, timestampLog, msg)

#
## Delete Mensagem In
def deleteMensagemIn(conectaBase, ano, mes):
  sqlCursor = conectaBase.cursor()
  sqlQuery = "delete from integraiot.iiot_mensagem_in_v2  where year(dt_msg) = " + str(ano) + " and month(dt_msg) = " + str(mes)
  sqlCursor.execute(sqlQuery)
  conectaBase.commit()
  sqlCursor.close()
  msg="Registros deletados com sucesso."
  gravaLog(1,logName, timestampLog, msg)

#
## Manutencao de Disco
def optimizeTable(tbl, DBUSER, DBPASS):
  msg="Iniciando Optimize..." + tbl
  gravaLog(1, logName, timestampLog, msg)
  cmd = MYSQLCHECK + "mysqlcheck -u " + DBUSER + " -p\"" + DBPASS + "\" integraiot " + tbl
  os.system(cmd)
  msg="Optimize finalizado."
  gravaLog(1, logName, timestampLog, msg)


#
# Manutencao (Com Delete)
def manutDB(ano, mes, logName, timestampLog, DEBUG):
  msg="Validando Ano Mes: " +str(ano)+"-"+str(mes)
  gravaLog(1,logName, timestampLog, msg)
  gravaLog(1,logName, timestampLog, "Contando Rest...")
  countRestIn = countRest(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_rest_in: " + str(countRestIn)
  gravaLog(1,logName, timestampLog, msg)
  #
  gravaLog(1,logName, timestampLog, "Contando Mensagem...")
  countMensagemIn = countMensagem(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_mensagem_in_v2: " + str(countMensagemIn)
  gravaLog(1,logName, timestampLog, msg)
  #
  gravaLog(1,logName, timestampLog, "Contando Rest History...")
  countHistRestIn = countHistRest(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_rest_in_history: " + str(countHistRestIn)
  gravaLog(1,logName, timestampLog, msg)
  #
  gravaLog(1,logName, timestampLog, "Contando Mensagem History...")
  countHistMensagemIn = countHistMensagem(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_mensagem_in_v2_history: " + str(countHistMensagemIn)
  gravaLog(1,logName, timestampLog, msg)

  #
  ## A partir de Mes-3 realiza manutencao na tabela principal
  ## desde que o historico esteja correto.
  msg="Validando Mensagem e Mensagem History para: " +str(ano)+"-"+str(mes)
  gravaLog(1,logName, timestampLog, msg)
  if countMensagemIn > 0:
    if countMensagemIn == countHistMensagemIn:
      msg="Quantidade de registros em mensagem e mensagem_history e igual para: " +str(ano)+"-"+str(mes)
      gravaLog(1,logName, timestampLog, msg)
      msg="Realizando manutencao em mensagem para: " +str(ano)+"-"+str(mes)
      gravaLog(1,logName, timestampLog, msg)
      if DEBUG==0:
        deleteMensagemIn(conectaBase, ano, mes)
    else:
      msg="ERRO verificar mensagem history <> mensagem para: " +str(ano)+"-"+str(mes)
      gravaLog(4,logName, timestampLog, msg)
  else:
    msg="Tabela Mensagem IN OK para:" +str(ano)+"-"+str(mes)
    gravaLog(2,logName, timestampLog, msg)

  msg="Validando Rest e Rest History para: " +str(ano)+"-"+str(mes)
  gravaLog(1,logName, timestampLog, msg)
  if countRestIn > 0:
    if countRestIn == countHistRestIn:
      msg="Quantidade de registros em rest e rest_history e igual para: " +str(ano)+"-"+str(mes)
      gravaLog(1,logName, timestampLog, msg)
      msg="Realizando manutencao em rest para: " +str(ano)+"-"+str(mes)
      gravaLog(1,logName, timestampLog, msg)
      if DEBUG==0:
        deleteRestIn(conectaBase, ano, mes)
    else:
      msg="ERRO verificar! rest history <> rest para: " +str(ano)+"-"+str(mes)
      gravaLog(4,logName, timestampLog, msg)
  else:
    msg="Tabela Rest IN OK para:" +str(ano)+"-"+str(mes)
    gravaLog(2,logName, timestampLog, msg)

#
# Check (Sem Delete)
def checkDB(ano, mes, logName, timestampLog):
  msg="Validando Ano Mes: " +str(ano)+"-"+str(mes)
  gravaLog(1,logName, timestampLog, msg)
  gravaLog(1,logName, timestampLog, "Contando Rest...")
  countRestIn = countRest(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_rest_in: " + str(countRestIn)
  gravaLog(1,logName, timestampLog, msg)
  #
  gravaLog(1,logName, timestampLog, "Contando Mensagem...")
  countMensagemIn = countMensagem(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_mensagem_in_v2: " + str(countMensagemIn)
  gravaLog(1,logName, timestampLog, msg)
  #
  gravaLog(1,logName, timestampLog, "Contando Rest History...")
  countHistRestIn = countHistRest(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_rest_in_history: " + str(countHistRestIn)
  gravaLog(1,logName, timestampLog, msg)
  #
  gravaLog(1,logName, timestampLog, "Contando Mensagem History...")
  countHistMensagemIn = countHistMensagem(conectaBase, ano, mes)
  msg="Quantidade de registros em iiot_mensagem_in_v2_history: " + str(countHistMensagemIn)
  gravaLog(1,logName, timestampLog, msg)

  msg="Validando Mensagem e Mensagem History para: " +str(ano)+"-"+str(mes)
  gravaLog(1,logName, timestampLog, msg)
  if countMensagemIn == countHistMensagemIn:
    msg="Quantidade de registros em mensagem e mensagem_history e igual para: " +str(ano)+"-"+str(mes)
    gravaLog(2,logName, timestampLog, msg)
  else:
    msg="ERRO verificar mensagem history <> mensagem para: " +str(ano)+"-"+str(mes)
    gravaLog(4, logName, timestampLog, msg)

  msg="Validando Rest e Rest History para: " +str(ano)+"-"+str(mes)
  gravaLog(1, logName, timestampLog, msg)
  if countRestIn == countHistRestIn:
    msg="Quantidade de registros em Rest e Rest_history e igual para: " +str(ano)+"-"+str(mes)
    gravaLog(2, logName, timestampLog, msg)
  else:
    msg="ERRO verificar rest history <> Rest para: " +str(ano)+"-"+str(mes)
    gravaLog(4, logName, timestampLog, msg)

#
## Check (Mes Atual -1 e Mes Atual -2)
mes=mes-1
checkDB(ano, mes, logName, timestampLog)
mes=mes-1
checkDB(ano, mes, logName, timestampLog)

#
## Manut (Mes Atual -3, -4 e -5)
mes=mes-1
manutDB(ano, mes, logName, timestampLog, DEBUG)
mes=mes-1
manutDB(ano, mes, logName, timestampLog, DEBUG)
mes=mes-1
manutDB(ano, mes, logName, timestampLog, DEBUG)

#
## Manutencao de Disco
optimizeTable("iiot_rest_in",DBUSER, DBPASS)
optimizeTable("iiot_mensagem_in_v2", DBUSER,DBPASS)
