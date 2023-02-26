# -*- coding: utf-8 -*-
"""
Created on Tue Jan 24 11:54:49 2023

@author: sadeghi.a
"""
import pandas as pd
import os
import warnings

warnings.filterwarnings("ignore")
workingDir = r'D:\Logs\_App\BankLogCleanerPy'
os.chdir(workingDir)

from funcs import *
logsPath = r'D:\Logs\BlockingLogs'

files = extractWantedFiles(logsPath)

if ("transferBlock.txt" in files) or ("unblock.txt" in files):
    for file in files:
        results = readRawText(logsPath, file)
        if len(results) == 0: 
            continue
        results = removeProblematicChars(results)
        fileType = detectFileType(file)
        data = createDataFrame(results, fileType)
        data = makeDataClean(data, fileType)
        data = enrichData(data, file, fileType)
        createPickle(data, fileType)
        
        if fileType == "block":
            block = data
        elif fileType == "blockErrors":
            blockError = data
        elif fileType == "transferBlock":
            transferBlock = data
        elif fileType == "transferBlockErrors":
            transferBlockErrors = data
        elif fileType == "unblock":
            unblock = data
        elif fileType == "unblockErrors":
            unblockErrors = data
			
    for file in files:
        fileType = detectFileType(file)
        if fileType == "block":
            block = makeBlockCodeClean(block)
        elif fileType == "blockErrors":
            blockError = makeBlockCodeClean(blockError)
        elif fileType == "transferBlock":
            transferBlock = makeBlockCodeClean(transferBlock)
        elif fileType == "transferBlockErrors":
            transferBlockErrors = makeBlockCodeClean(transferBlockErrors)
        elif fileType == "unblock":
            unblock = makeBlockCodeClean(unblock)
        elif fileType == "unblockErrors":
            unblockErrors = makeBlockCodeClean(unblockErrors)
        
    block['EndedUpIn'] = ''
    
    if 'transferBlock' in dir() and 'unblock' in dir():
        inTransferBlock = block[block['BlockCode'].isin(transferBlock['BlockCode'])]
        inTransferBlock.loc[:, 'EndedUpIn'] = 'TransferBlock'
        inUnblock = block[block['BlockCode'].isin(unblock['BlockCode'])]
        inUnblock.loc[:, 'EndedUpIn'] = 'UnBlock'
        resolved = pd.concat([inTransferBlock, inUnblock])
    elif 'transferBlock' in dir():
        inTransferBlock = block[block['BlockCode'].isin(transferBlock['BlockCode'])]
        inTransferBlock.loc[:, 'EndedUpIn'] = 'TransferBlock'
        resolved = inTransferBlock
    elif 'unblock' in dir():
        inUnblock = block[block['BlockCode'].isin(unblock['BlockCode'])]
        inUnblock.loc[:, 'EndedUpIn'] = 'UnBlock'
        resolved = inUnblock
    else:
        resolved = block
        
    merged = block.merge(resolved, on = 'BlockCode', how = 'left', indicator = True)
    notIncluded = merged.loc[merged['_merge'] == 'left_only', 'BlockCode']
    unresolved = block[block['BlockCode'].isin(notIncluded)]
    
    moveLogs(logsPath, files)
        
    unresolved = enrichUnresolved(unresolved)
    
    unresolved.to_csv('/'.join([logsPath, 'block.txt']), index = None, header=None, sep = '\t', mode='a')
    
    engine = createEngine()
    
    for file in files:
        fileType = detectFileType(file)
        dbtypes = setDBTypes(fileType)
        if fileType == "block":
            fixColumnSize(resolved, fileType).to_sql(name = 'Block', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)
        elif fileType == "blockErrors":
            fixColumnSize(blockError, fileType).to_sql(name = 'BlockErrors', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)
        elif fileType == "transferBlock":
            fixColumnSize(transferBlock, fileType).to_sql(name = 'TransferBlock', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)
        elif fileType == "transferBlockErrors":
            fixColumnSize(transferBlockErrors, fileType).to_sql(name = 'TransferBlockErrors', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)
        elif fileType == "unblock":
            fixColumnSize(unblock, fileType).to_sql(name = 'UnBlock', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)
        elif fileType == "unblockErrors":
            fixColumnSize(unblockErrors, fileType).to_sql(name = 'UnBlockErrors', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)

    engine.dispose()
