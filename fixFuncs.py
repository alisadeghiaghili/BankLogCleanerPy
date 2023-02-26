# -*- coding: utf-8 -*-
"""
Created on Sun Feb 12 15:54:31 2023

@author: sadeghi.a
"""

import os
import re
import pandas as pd
from datetime import datetime
import shutil
import sqlalchemy as sa
import codecs

def extractWantedFolders(path):
    return [folder for folder in os.listdir(path) if re.search('\d+', folder)]

def extractWantedFiles(path, folder):
    fullPath = path + '/' + folder
    files = os.listdir(fullPath)
    wantedFiles = ['block.txt', 'blockErrors.txt', 'transferBlock.txt', 'transferBlockErrors.txt', 'unblock.txt', 'unblockErrors.txt']
    result = []
    for file in files:
        if file in wantedFiles:
            result.append(file)
    
    return result

def detectFileType(file):
    pattern = '(block|unblock|blockErrors|unblockErrors|transferBlock|transferBlockErrors)\.txt'
    return re.findall(pattern, file)[0]

def readRawText(logsPath, folder, file):
    filePath = '/'.join([logsPath, folder, file])
    with codecs.open(filePath, 'r', 'UTF-8') as text:
        return text.readlines()

def removeProblematicChars(data):
        return [line.replace(" \"	","").replace("\"", "").replace("\t\r\n", "") for line in data]
    
def splitValues(data):
    return data.split("\t")

def createDataFrame(data, fileType):
    data = [splitValues(line) for line in data]
    if fileType == "block":
        columns = ["BankName", "AccountNumber", "ShebaNumber", "Amount", "BlockCode", "Date", "TransactionTime", "ReferenceCode", "Status"]
    elif fileType == "blockErrors":
        columns = ["BankName", "AccountNumber", "ShebaNumber", "Amount", "BlockCode", "Date", "TransactionTime", "ReferenceCode", "ErrorCode", "Status"]
    elif fileType == "transferBlock":
        columns = ["BankName", "SourceAccount", "DestinationAccount", "BlockAmount", "DocumentNumber", "BlockCode", "TransferAmount", "Date", "TransactionTime", "ReferenceCode", "Status", "Debit", "Credit"]
    elif fileType == "transferBlockErrors":
        columns = ["BankName", "SourceAccount", "DestinationAccount", "BlockAmount", "DocumentNumber", "BlockCode", "TransferAmount", "Date", "TransactionTime", "ReferenceCode", "ErrorCode", "Status"]
    elif fileType == "unblock":
        columns = ["BankName", "AccountNumber", "ShebaNumber", "Amount", "BlockCode", "Date", "TransactionTime", "ReferenceCode", "Status"]
    elif fileType == "unblockErrors":
        columns = ["BankName", "AccountNumber", "ShebaNumber", "Amount", "BlockCode", "Date", "TransactionTime", "ReferenceCode", "ErrorCode", "Status"]
    return pd.DataFrame(data, columns = columns)
       
def makeDataClean(data, fileType):
    for col in range(data.shape[1]):
        data.iloc[:, col] = data.iloc[:, col].str.replace('^.*?:','', regex = True).str.strip()
        
    if fileType == "block":
        data.AccountNumber = data.AccountNumber.str.replace('\\*', '', regex = True).str.strip()
        data.ShebaNumber = data.ShebaNumber.str.replace('\\*', '', regex = True).str.strip()
    
    return data

def makeBlockCodeClean(data):
    data.loc[:, 'BlockCode'] = data.loc[:, 'BlockCode'].str.replace('^.*?:','', regex = True).str.strip()
    return data

def enrichData(data, file, fileType):
    data['FileName'] = file
    data['Type'] = fileType
    
    return data
    
def createPickle(data, fileType):
    if fileType == "block":
        data.to_pickle('Pickles/block.pickle')
    elif fileType == "blockErrors":
        data.to_pickle('Pickles/blockErrors.pickle')
    elif fileType == "transferBlock":
        data.to_pickle('Pickles/transferBlock.pickle')
    elif fileType == "transferBlockErrors":
        data.to_pickle('Pickles/transferBlockErrors.pickle')
    elif fileType == "unblock":
        data.to_pickle('Pickles/unblock.pickle')
    elif fileType == "unblockErrors":
        data.to_pickle('Pickles/unblockErrors.pickle')
    

def moveLogs(path, files):
    folderName = datetime.strftime(datetime.now(), '%Y-%m-%d %H%M')
    os.makedirs('/'.join([path, folderName]))
    for file in files:
        shutil.move('/'.join([path, file]), '/'.join([path, folderName, file]))
        
def enrichUnresolved(unresolved):
    unresolved.drop(['FileName', 'Type', 'EndedUpIn'], axis = 1, inplace = True)
    unresolved.BankName = 'بانک:' + unresolved.BankName
    unresolved.AccountNumber = 'حساب:' + unresolved.AccountNumber
    unresolved.ShebaNumber = 'شبا:' + unresolved.ShebaNumber
    unresolved.Amount = 'مبلغ:' + unresolved.Amount
    unresolved.BlockCode = 'کد مسدودی:' + unresolved.BlockCode
    unresolved.Date = 'تاریخ:' + unresolved.Date
    unresolved.TransactionTime = 'زمان تراکنش:' + unresolved.TransactionTime
    unresolved.ReferenceCode = 'کد پیگیری:' + unresolved.ReferenceCode
    unresolved.Status = 'وضعیت:' + unresolved.Status
    
    return unresolved

def writeUnresolvedBlocks(unresolved, path):
    unresolved.to_csv('/'.join([path, 'block2.txt']), index = None, header=None, sep = '\t', mode='a')
    
def createEngine():
    config = 'mssql+pyodbc://BankLogManagement:SJTu4T1QcsgiWbCI@172.16.2.29/Bank?driver=SQL+Server+Native+Client+11.0'
    return sa.create_engine(config)

def setDBTypes(fileType):
    if fileType == "block":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "AccountNumber": sa.types.VARCHAR(length=50), 
                 "ShebaNumber": sa.types.VARCHAR(length=50),
                 "Amount": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=21), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(length=255), 
                 "FileName": sa.types.NVARCHAR(length=255), 
                 "Type": sa.types.VARCHAR(length=30), 
                 "EndedUpIn": sa.types.VARCHAR(length=13)}
    elif fileType == "blockErrors":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "AccountNumber": sa.types.VARCHAR(length=50), 
                 "ShebaNumber": sa.types.VARCHAR(length=50),
                 "Amount": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=255), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "ErrorCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(length=1000), 
                 "FileName": sa.types.VARCHAR(length=100), 
                 "Type": sa.types.VARCHAR(length=30)}
    elif fileType == "transferBlock":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "SourceAccount": sa.types.VARCHAR(length=50), 
                 "DestinationAccount": sa.types.VARCHAR(length=50),
                 "BlockAmount": sa.types.VARCHAR(length=50), 
                 "DocumentNumber": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "TransferAmount": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=21), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(length=255), 
                 "Debit": sa.types.VARCHAR(length=50), 
                 "Credit": sa.types.VARCHAR(length=50), 
                 "FileName": sa.types.VARCHAR(length=255), 
                 "Type": sa.types.VARCHAR(length=30)}
    elif fileType == "transferBlockErrors":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "SourceAccount": sa.types.VARCHAR(length=50), 
                 "DestinationAccount": sa.types.VARCHAR(length=50),
                 "BlockAmount": sa.types.VARCHAR(length=50), 
                 "DocumentNumber": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "TransferAmount": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=30), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "ErrorCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(), 
                 "FileName": sa.types.VARCHAR(length=255), 
                 "Type": sa.types.VARCHAR(length=30)}
    elif fileType == "unblock":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "AccountNumber": sa.types.VARCHAR(length=50), 
                 "ShebaNumber": sa.types.VARCHAR(length=50),
                 "Amount": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=21), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(length=255), 
                 "FileName": sa.types.VARCHAR(length=255), 
                 "Type": sa.types.VARCHAR(length=30)}
    elif fileType == "unblockErrors":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "AccountNumber": sa.types.VARCHAR(length=50), 
                 "ShebaNumber": sa.types.VARCHAR(length=50),
                 "Amount": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=21), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "ErrorCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(length=1000), 
                 "FileName": sa.types.VARCHAR(length=255), 
                 "Type": sa.types.VARCHAR(length=15)}
    return dtype

def fixColumnSize(data, fileType):
    if fileType == "block":
        data.BankName = data.BankName.str[:50]
        data.AccountNumber = data.AccountNumber.str[:50]
        data.ShebaNumber = data.ShebaNumber.str[:50]
        data.Amount = data.Amount.str[:50]
        data.BlockCode = data.BlockCode.str[:50]
        data.Date = data.Date.str[:50]
        data.TransactionTime = data.TransactionTime.str[:21]
        data.ReferenceCode = data.ReferenceCode.str[:50]
        data.Status = data.Status.str[:255]
        data.FileName = data.FileName.str[:255]
        data.Type = data.Type.str[:30]
        data.EndedUpIn = data.EndedUpIn.str[:13]
    elif fileType == "blockErrors":
        data.BankName = data.BankName.str[:50]
        data.AccountNumber = data.AccountNumber.str[:50]
        data.ShebaNumber = data.ShebaNumber.str[:50]
        data.Amount = data.Amount.str[:50]
        data.BlockCode = data.BlockCode.str[:50]
        data.Date = data.Date.str[:50]
        data.TransactionTime = data.TransactionTime.str[:255]
        data.ReferenceCode = data.ReferenceCode.str[:50]
        data.ErrorCode = data.ErrorCode.str[:50]
        data.Status = data.Status.str[:1000]
        data.FileName = data.FileName.str[:100]
        data.Type = data.Type.str[:30]
    elif fileType == "transferBlock":
        data.BankName = data.BankName.str[:50]
        data.SourceAccount = data.SourceAccount.str[:50]
        data.DestinationAccount = data.DestinationAccount.str[:50]
        data.BlockAmount = data.BlockAmount.str[:50]
        data.DocumentNumber = data.DocumentNumber.str[:50]
        data.BlockCode = data.BlockCode.str[:50]
        data.TransferAmount = data.TransferAmount.str[:50]
        data.Date = data.Date.str[:50]
        data.TransactionTime = data.TransactionTime.str[:21]
        data.ReferenceCode = data.ReferenceCode.str[:50]
        data.Status = data.Status.str[:255]
        data.Debit = data.Debit.str[:50]
        data.Credit = data.Credit.str[:50]
        data.FileName = data.FileName.str[:255]
        data.Type = data.Type.str[:30]
    elif fileType == "transferBlockErrors":
        data.BankName = data.BankName.str[:50]
        data.SourceAccount = data.SourceAccount.str[:50]
        data.DestinationAccount = data.DestinationAccount.str[:50]
        data.BlockAmount = data.BlockAmount.str[:50]
        data.DocumentNumber = data.DocumentNumber.str[:50]
        data.BlockCode = data.BlockCode.str[:50]
        data.TransferAmount = data.TransferAmount.str[:50]
        data.Date = data.Date.str[:50]
        data.TransactionTime = data.TransactionTime.str[:30]
        data.ReferenceCode = data.ReferenceCode.str[:50]
        data.ErrorCode = data.ErrorCode.str[:50]
        data.FileName = data.FileName.str[:255]
        data.Type = data.Type.str[:30]
    elif fileType == "unblock":
        data.BankName = data.BankName.str[:50]
        data.AccountNumber = data.AccountNumber.str[:50]
        data.ShebaNumber = data.ShebaNumber.str[:50]
        data.Amount = data.Amount.str[:50]
        data.BlockCode = data.BlockCode.str[:50]
        data.Date = data.Date.str[:50]
        data.TransactionTime = data.TransactionTime.str[:21]
        data.ReferenceCode = data.ReferenceCode.str[:50]
        data.Status = data.Status.str[:255]
        data.FileName = data.FileName.str[:255]
        data.Type = data.Type.str[:30]
    elif fileType == "unblockErrors":
        data.BankName = data.BankName.str[:50]
        data.AccountNumber = data.AccountNumber.str[:50]
        data.ShebaNumber = data.ShebaNumber.str[:50]
        data.Amount = data.Amount.str[:50]
        data.BlockCode = data.BlockCode.str[:50]
        data.Date = data.Date.str[:50]
        data.TransactionTime = data.TransactionTime.str[:21]
        data.ReferenceCode = data.ReferenceCode.str[:50]
        data.ErrorCode = data.ErrorCode.str[:50]
        data.Status = data.Status.str[:1000]
        data.FileName = data.FileName.str[:255]
        data.Type = data.Type.str[:15]
    return data
