#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import snowflake.connector
from snowflake.connector import DictCursor
import os
from sys import argv
import sys
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
import base64
import time
import re
from datetime import timedelta, datetime, date
from decimal import *
import json
import requests
import configparser
# Logging with file size limit
import logging
from logging.handlers import RotatingFileHandler

# creates a connection
def openConn(settings):
    ctx = snowflake.connector.connect(
        user= settings["sfUser"],
        private_key=settings["pkb"],
        account=settings["sfAccount"],
        region=settings["sfRegion"]
    )
    return ctx

###################################
# Note: String template is used for excute due to Snowflake not replace table name correctly for substitution within excute.
# It adds '' to the value result in SQL error. Only company is a user input, so work to santized that instead.
# https://github.com/snowflakedb/snowflake-connector-python/issues/10
###################################

#######
# maps table layout to require fields and type.
#######
def returnLayout(fields, config):
    cols = {}
    cols['columns'] = {}
    for field in fields:
        # map special fields ie row index and braze id
        if field['name'].upper() == config['BRAZE_INDEX'].upper():
            cols['index_id'] = field['name'].upper()
        elif field['name'] == config['BRAZE_ID_COLUMN'].upper():
            cols['braze_id'] = field['name'].upper()
        else:
            # remove any precision
            field_type, *field_precision = re.split('\(|\)',field['type'])
            cols['columns'][field['name'].upper()] = field_type
    return cols

#######
# converts snowflake rowmap to api call based on object type of record.
#######
def convertRow(rows, layout, settings, config, isevent = False):
    convertedRow = {}
    convertedRow['fields'] = []
    cols = layout['columns']
    if config['PROCESS_FULL_TABLE']:
        convertedRow['lastRow'] = 0
        convertedRow['startRow'] = 0

    for row in rows:
        curRow = {}
        curRow[config['BRAZE_ID_TYPE'].lower()] = row[config['BRAZE_ID_COLUMN'].upper()]
        if not config['PROCESS_FULL_TABLE']:
            convertedRow['lastRow'] = row[config['BRAZE_INDEX'].upper()]
            if 'startRow' not in convertedRow:
                convertedRow['startRow'] = row[config['BRAZE_INDEX'].upper()]

        for col, field in cols.items():
            rowValue = row[col.upper()]
            colName = col.lower()
            # For event, if columns end with custom event prefix, rename and set the value to
            # column.
            if isevent:
                if colName.startswith(settings['eventColPrefix']):
                    # Ignore fields ending with postfix
                    if colName.endswith(settings['eventColPostfix']):
                        continue
                    # else assign rowvalue to the postfix column
                    else:
                        colName = row[col.upper()]
                        rowValue = row[col.upper() + settings['eventColPostfix'].upper()]

            # if a number type, determine if it's integer or decimal/float.
            # decimal or float needs to be cast to it's numeric value.
            if (field == 'NUMBER') or (field == 'INT') or (field == 'REAL') or (field == 'FLOAT'):
                if isinstance(rowValue, Decimal):
                    curRow[colName] = float(rowValue)
                else:
                    curRow[colName] = rowValue
            # boolean does not need to be treated
            elif (field == 'BOOLEAN') or (field == 'BINARY'):
                curRow[colName] = rowValue
            # date needs to be converted to iso 8601 format which is default for isoformat()
            elif ('DATE' in field ) or ('TIME' in field):
                # check if a date fields exist. Else set as none
                if rowValue:
                    # if date, then format as date
                    curRow[colName] = rowValue.isoformat()
                else:
                    curRow[colName] = rowValue
            # if array, save it as an array attribute
            elif (field == 'ARRAY'):
                if rowValue is None:
                    curRow[colName] = None
                else:
                    curRow[colName] = json.loads(rowValue)
            # need to figure out how to test this fully, for now, we will treat it as a string
            # elif (field == 'VARIANT') or (field == 'OBJECT') or (field == 'ARRAY'):
            #     if rowValue is None:
            #         curRow[colName] = None
            #     else:
            #         curRow[colName] = json.loads(json.loads(rowValue))
            else:
                curRow[colName] = rowValue

        convertedRow['fields'].append(curRow)
    return convertedRow

#######
# logs the event in the event table per api call.
#######
def logAPIEvent(settings, conn, config, options):
    message = ''
    if 'message' in options['response']:
        message = options['response']['message']
    table_query = "insert into %s(COMPANY_NAME, TABLE_NAME, TABLE_TYPE, ROW_STARTNUM, ROW_ENDNUM, MESSAGE, RESPONSE, START_TIME, END_TIME, NUMBER_UPDATED) " \
        " values('%s', '%s', '%s', %s, %s, '%s', '%s', '%s', '%s', %s);" \
        % (settings["logTable"],config['COMPANY_NAME'],config['TABLE_NAME'],config['TABLE_TYPE'], options['startRow'], options['lastRow'], message, str(options['response']).replace("\'","\\\'"), options['startTime'], options['endTime'], options['rowsProcessed'])
    cs = conn.cursor()
    cs.execute(table_query)
    if settings["sfDebug"]: logging.info(table_query)

#######
# updates config table once api is done.
#######
def updateConfigTableMeta(settings, conn, config, options):
    message = ''
    if 'message' in options['response']:
        message = options['response']['message']
    table_query = "update %s set LAST_ROWNUM = %s, LASTRUN_DATE = '%s', LAST_RESPONSE = '%s', LAST_MESSAGE = '%s' " \
        " where COMPANY_NAME = '%s' and TABLE_NAME = '%s' and TABLE_TYPE = '%s';" \
        % (settings["configTable"], options['lastRow'], options['endTime'], message, str(options['response']).replace("\'","\\\'"), config['COMPANY_NAME'], config['TABLE_NAME'], config['TABLE_TYPE'])
    cs = conn.cursor()
    cs.execute(table_query)
    if settings["sfDebug"]: logging.info(table_query)
#######
# set Config Table to processing state.
#######
def setConfigTableProcessing(settings, conn, config):
    curTime = datetime.now().isoformat()
    table_query = "update %s set LASTRUN_DATE = '%s', LAST_RESPONSE = '%s'" \
        " where COMPANY_NAME = '%s' and TABLE_NAME = '%s' and TABLE_TYPE = '%s';" \
        % (settings["configTable"], curTime, 'processing', config['COMPANY_NAME'],config['TABLE_NAME'], config['TABLE_TYPE'])
    cs = conn.cursor()
    cs.execute(table_query)
    if settings["sfDebug"]: logging.info(table_query)

#######
# process attributes table type.
#######
def processAttributesTable(settings, conn, config):
    if settings["sfDebug"]:
        if config['PROCESS_FULL_TABLE']:
            logging.info("Processing Company(%s) - Full Table(%s) of type '%s'." % (config['COMPANY_NAME'],config['TABLE_NAME'],config['TABLE_TYPE']) )
        else:
            logging.info("Processing Company(%s) - Table(%s) of type '%s' starting after row: %s." % (config['COMPANY_NAME'],config['TABLE_NAME'],config['TABLE_TYPE'], config['LAST_ROWNUM']) )

    # try and describe table for processing. Error if not found, or incorrect permissions to table.
    try:
        table_query = "describe table %s;" % (config['TABLE_NAME'])
        cur = conn.cursor(DictCursor).execute(table_query)
        ret = cur.fetchall()
        layout = returnLayout(ret,config)
    except Exception as e:
        errorStr = 'Missing Attributes Table(' + config['TABLE_NAME'] + '):' + str(e)
        if settings["sfDebug"]: logging.error(errorStr)
        raise Exception(errorStr)
    # Ensure number of columns is under api limit.  May split the api calls up in the future to avoid limit.
    if len(layout['columns']) > config['BRAZE_MAXCOLUMNS']:
        if settings["sfDebug"]: logging.error("Too many columns in table. This needs to be under " + config['BRAZE_MAXCOLUMNS']  + ".")
        raise Exception("Too many columns in table. This needs to be under " + config['BRAZE_MAXCOLUMNS']  + ".")
    # ensure an user id and row id is present
    if (not config['PROCESS_FULL_TABLE'] and ('index_id' in layout) and ('braze_id' in layout) ) or ( config['PROCESS_FULL_TABLE'] and ('braze_id' in layout)):
        # query the table for all results after the last row number
        endPoint = config['BRAZE_ENDPOINT'].rstrip('/') + settings["trackEndPoint"]
        apiHeader = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + config['BRAZE_APIKEY']
            }
        try:
            # find newest records since last ran
            if config['PROCESS_FULL_TABLE']:
                table_query = "select * from %s" % (config['TABLE_NAME'])
            else:
                table_query = "select * from %s where %s > %s order by %s asc;" % (config['TABLE_NAME'], config['BRAZE_INDEX'], config['LAST_ROWNUM'], config['BRAZE_INDEX'])
            cur = conn.cursor(DictCursor).execute(table_query)
            ret = cur.fetchmany(config['BRAZE_MAXRECORDS'])
            endTime = datetime.now().isoformat()
            # only process if new records exist since last ran
            if len(ret) > 0:
                setConfigTableProcessing(settings, conn, config)
                processLines = 0
                while len(ret) > 0:
                    startTime = datetime.now().isoformat()
                    apiRow = convertRow(ret, layout, settings, config)
                    apiBody = {}
                    apiBody['attributes'] = apiRow['fields']
                    # make api call to braze
                    response = requests.post(endPoint, data=json.dumps(apiBody), headers=apiHeader)

                    endTime = datetime.now().isoformat()
                    if config['PROCESS_FULL_TABLE']:
                        startRow = processLines
                        lastRow = processLines + len(ret)
                    else:
                        startRow = apiRow['startRow']
                        lastRow = apiRow['lastRow']
                    if settings["sfDebug"]: logging.info("Processing %s:%s - Line (%s to %s): %s." % (config['COMPANY_NAME'], config['TABLE_NAME'], startRow, lastRow, response.json()) )

                    if settings["logResponse"]: logAPIEvent(settings, conn, config, {'startRow': startRow, 'lastRow': lastRow, 'startTime': startTime, 'endTime': endTime, 'rowsProcessed': len(ret), 'response': response.json()})
                    processLines += len(ret)
                    ret = cur.fetchmany(config['BRAZE_MAXRECORDS'])
                # update config table with latest row
                updateConfigTableMeta(settings, conn, config, {'lastRow': lastRow, 'endTime': endTime, 'response': response.json()})
            else:
                if settings["sfDebug"]: logging.info("No new item to process.")
        except Exception as e:
            errorStr = 'Error: ' + str(e)
            if settings["sfDebug"]: logging.error(errorStr)
            raise Exception(errorStr)
    else:
        if settings["sfDebug"]: logging.error('Missing index_id and/or braze_id.')
        raise Exception('Missing index_id and/or braze_id.')

#######
# process events table type.
#######
def processEventsTable(settings, conn, config):
    if settings["sfDebug"]:
        if config['PROCESS_FULL_TABLE']:
            logging.info("Processing Company(%s) - Full Table(%s) of type '%s'." % (config['COMPANY_NAME'],config['TABLE_NAME'],config['TABLE_TYPE']) )
        else:
            logging.info("Processing Company(%s) - Table(%s) of type '%s' starting after row: %s." % (config['COMPANY_NAME'],config['TABLE_NAME'],config['TABLE_TYPE'], config['LAST_ROWNUM']) )

    # try and describe table for processing. Error if not found, or incorrect permissions to table.
    try:
        table_query = "describe table %s;" % (config['TABLE_NAME'])
        cur = conn.cursor(DictCursor).execute(table_query)
        ret = cur.fetchall()
        layout = returnLayout(ret,config)
    except Exception as e:
        errorStr = 'Missing Events Table(' + config['TABLE_NAME'] + '):' + str(e)
        if settings["sfDebug"]: logging.error(errorStr)
        raise Exception(errorStr)
    # Ensure number of columns is under api limit.  May split the api calls up in the future to avoid limit.
    if len(layout['columns']) > config['BRAZE_MAXCOLUMNS']:
        if settings["sfDebug"]: logging.error("Too many columns in table. This needs to be under " + config['BRAZE_MAXCOLUMNS']  + ".")
        raise Exception("Too many columns in table. This needs to be under " + config['BRAZE_MAXCOLUMNS'] )

    # ensure an user id, row id, name and time are present, other fields will be optional
    if ((not config['PROCESS_FULL_TABLE'] and ('index_id' in layout) and ('braze_id' in layout) ) or ( config['PROCESS_FULL_TABLE'] and ('braze_id' in layout))) and ('NAME' in layout['columns']) and ('TIME' in layout['columns']):
        # query the table for all results after the last row number
        endPoint = config['BRAZE_ENDPOINT'].rstrip('/') + settings["trackEndPoint"]
        apiHeader = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + config['BRAZE_APIKEY']
            }
        try:
            # find newest records since last ran
            if config['PROCESS_FULL_TABLE']:
                table_query = "select * from %s" % (config['TABLE_NAME'])
            else:
                table_query = "select * from %s where %s > %s order by %s asc;" % (config['TABLE_NAME'], config['BRAZE_INDEX'], config['LAST_ROWNUM'], config['BRAZE_INDEX'])
            cur = conn.cursor(DictCursor).execute(table_query)
            ret = cur.fetchmany(config['BRAZE_MAXRECORDS'])
            endTime = datetime.now().isoformat()
            # only process if new records exist since last ran
            if len(ret) > 0:
                setConfigTableProcessing(settings, conn, config)
                processLines = 0
                while len(ret) > 0:
                    startTime = datetime.now().isoformat()
                    apiRow = convertRow(ret, layout, settings, config, True)
                    apiBody = {}
                    apiBody['events'] = apiRow['fields']

                    # make api call to braze
                    response = requests.post(endPoint, data=json.dumps(apiBody), headers=apiHeader)

                    endTime = datetime.now().isoformat()
                    if config['PROCESS_FULL_TABLE']:
                        startRow = processLines
                        lastRow = processLines + len(ret)
                    else:
                        startRow = apiRow['startRow']
                        lastRow = apiRow['lastRow']
                    if settings["sfDebug"]: logging.info("Processing %s:%s - Line (%s to %s): %s." % (config['COMPANY_NAME'], config['TABLE_NAME'], startRow, lastRow, response.json()) )

                    if settings["logResponse"]: logAPIEvent(settings, conn, config, {'startRow': startRow, 'lastRow': lastRow, 'startTime': startTime, 'endTime': endTime, 'rowsProcessed': len(ret), 'response': response.json()})
                    processLines += len(ret)
                    ret = cur.fetchmany(config['BRAZE_MAXRECORDS'])
                # update config table with latest row
                updateConfigTableMeta(settings, conn, config, {'lastRow': lastRow, 'endTime': endTime, 'response': response.json()})
            else:
                if settings["sfDebug"]: logging.info("No new item to process.")
        except Exception as e:
            errorStr = 'Error: ' + str(e)
            if settings["sfDebug"]: logging.error(errorStr)
            raise Exception(errorStr)
    else:
        if settings["sfDebug"]: logging.error('Missing index_id, braze_id, name or time.')
        raise Exception('Missing index_id, braze_id, name or time.')

#######
# process all active tables for the company.
#######
def processCompany(company, settings):
    conn = openConn(settings)
    cs = conn.cursor()
    try:
        # use config database name, connector and schema
        cs.execute("use database " + settings["databaseName"] + ";")
        cs.execute("use warehouse " + settings["warehouseName"] + ";")
        cs.execute("use schema " + settings["schemaName"] + ";")

        if settings["sfDebug"]: logging.info('Processing Company(%s) using %s:%s:%s' % (company, settings["warehouseName"], settings["databaseName"], settings["schemaName"]))

        company = company.upper()

        config_fields = ['COMPANY_NAME','TABLE_NAME','TABLE_TYPE','BRAZE_ID_TYPE','BRAZE_ID_COLUMN','BRAZE_INDEX','BRAZE_ENDPOINT','BRAZE_APIKEY','BRAZE_MAXRECORDS','BRAZE_MAXCOLUMNS','LAST_ROWNUM','CREATED_DATE','LASTRUN_DATE','PROCESS_FULL_TABLE']
        conf_query = "select %s from %s where ENABLED = true and UPPER(COMPANY_NAME) = '%s' and (LOWER(LAST_RESPONSE) != 'processing' OR LAST_RESPONSE IS NULL);" % (','.join(config_fields), settings["configTable"], company )

        cur = conn.cursor(DictCursor).execute(conf_query)
        ret = cur.fetchmany(1)
        if len(ret) > 0:
            # loop through each row and process them
            while len(ret) > 0:
                comp_config = ret[0]
                #print(comp_config)
                if comp_config['TABLE_TYPE'].lower() == 'attributes':
                    processAttributesTable(settings, conn, comp_config)
                elif comp_config['TABLE_TYPE'].lower() == 'events':
                    processEventsTable(settings, conn, comp_config)
                ret = cur.fetchmany(1)
        else:
            if settings["sfDebug"]: logging.warning('No active table entry found for %s using %s:%s:%s' % (company, settings["warehouseName"], settings["databaseName"], settings["schemaName"]))
    except Exception as e:
        errorStr = 'Error: ' + str(e)
        if settings["sfDebug"]: logging.error(errorStr)
        raise Exception(errorStr)

#######
# first parameter is company name.
#######
if __name__ == "__main__":
    # execute only if run as a script
    if len(argv) > 1:
        # sanitize parameter
        company = argv[1].replace(';', '\\;').replace('*', '\\*').replace('=', '\\=').replace('+', '\\+').replace('%', '\\%').replace("\n", '').replace("\r", '').replace("\b", '')
        settings = {}
        reqSettings = ["sfPwd", "sfUser", "sfAccount", "sfRegion", "databaseName", "warehouseName", "schemaName", "configTable", "logTable", "trackEndPoint"]
        rsaSettings = ["rsaPrivateKey", "rsaFile"]

        for setting in reqSettings:
            settings[setting] = None
        for rsa in rsaSettings:
            settings[rsa] = None

        # Default Optional setting
        settings["sfDebug"] = False
        settings["logResponse"] = False

        settings["logSize"] = 5
        settings["eventColPrefix"] = 'bce_'
        settings["eventColPostfix"] = '_value'

        if "logResponse" in os.environ:
            settings["logResponse"] = os.environ["logResponse"].lower() == 'true'
        # get settings from environment or config file.
        if "sfPwd" in os.environ:
            for setting in reqSettings:
                if setting in os.environ:
                    settings[setting] = os.environ[setting]
            for rsa in rsaSettings:
                if rsa in os.environ:
                    settings[rsa] = os.environ[rsa]

            if "sfDebug" in os.environ:
                settings["sfDebug"] = os.environ["sfDebug"].lower() == 'true'
            if settings["sfDebug"]:
                if "logName" in os.environ:
                    settings["logName"] = os.environ["logName"]
                if "logSize" in os.environ:
                    settings["logSize"] = int(os.environ["logSize"])
            if "eventColPrefix" in os.environ:
                settings["eventColPrefix"] = os.environ["eventColPrefix"].lower()
            if "eventColPostfix" in os.environ:
                settings["eventColPostfix"] = os.environ["eventColPostfix"].lower()
        else:
            # Configuration variables
            config = configparser.RawConfigParser()
            config.read('./config.cfg')

            # sets config variables, else error if missing.
            if config.has_section('snowflake'):
                for setting in reqSettings:
                    if config.has_option('snowflake', setting):
                        settings[setting] = config.get('snowflake', setting)
                for rsa in rsaSettings:
                    if config.has_option('snowflake', rsa):
                        settings[rsa] = config.get('snowflake', rsa)
                if config.has_option('snowflake', 'logResponse'):
                    settings["logResponse"] = config.getboolean('snowflake', 'logResponse')
                if config.has_option('snowflake', 'sfDebug'):
                    settings["sfDebug"] = config.getboolean('snowflake', 'sfDebug')
                if settings["sfDebug"]:
                    if config.has_option('snowflake', 'logName'):
                        settings["logName"] = config.get('snowflake', 'logName')
                    if config.has_option('snowflake','logSize'):
                        settings["logSize"] = config.getint('snowflake', 'logSize')
                if config.has_option('snowflake', 'eventColPrefix'):
                    settings["eventColPrefix"] = config.get('snowflake', 'eventColPrefix').lower()
                if config.has_option('snowflake', 'eventColPostfix'):
                    settings["eventColPostfix"] = config.get('snowflake', 'eventColPostfix').lower()
            else:
                raise Exception("Missing Config Section: snowflake")

        # check for missing settings
        for setting in reqSettings:
            if settings[setting] is None:
                if settings["sfDebug"]: logging.error('Missing config: %s' % (setting))
                raise Exception('Missing config: %s' % (setting))

        # Check for missing rsa settings
        if (settings["rsaPrivateKey"] is None) and (settings["rsaFile"] is None):
            if settings["sfDebug"]: logging.error('Missing config: rsaPrivateKey or rsaFile')
            raise Exception("Missing config: rsaPrivateKey or rsaFile")

        if settings["sfDebug"]:
            if ("logName" in settings) and (settings["logName"]):
                logging.basicConfig(
                    format='%(asctime)s - %(levelname)s:[%(name)s.%(funcName)s:%(lineno)d]: %(message)s',
                    filename=settings["logName"],
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
                # limit log size
                if settings["logSize"] > 0:
                    handler = RotatingFileHandler(settings["logName"],maxBytes=(1024 * 1024 * settings["logSize"]),backupCount=1)
                    handler.setLevel(logging.INFO)
                    logging.getLogger(__name__).addHandler(handler)
            else:
                logging.basicConfig(
                    format='%(asctime)s - %(levelname)s:[%(name)s.%(funcName)s:%(lineno)d]: %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

        # Auth through rsa
        # https://docs.snowflake.net/manuals/user-guide/snowsql-start.html#using-key-pair-authentication
        if settings["rsaPrivateKey"]:
            p_key= serialization.load_pem_private_key(
                settings["rsaPrivateKey"].encode('utf-8'),
                password=base64.b64decode(settings["sfPwd"]),
                backend=default_backend()
            )
        elif settings["rsaFile"]:
            with open(settings["rsaFile"], "rb") as key:
                p_key= serialization.load_pem_private_key(
                key.read(),
                password=base64.b64decode(settings["sfPwd"]),
                backend=default_backend()
            )
        else:
            if settings["sfDebug"]: logging.error('Missing config: rsaPrivateKey or rsaFile')
            raise Exception("Missing config: rsaPrivateKey or rsaFile")

        settings["pkb"] = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        processCompany(company, settings)
    else:
        raise Exception('Missing company name.')
