# -*- coding: utf-8 -*-
"""
Created on Sun Feb 26 16:44:09 2023

@author: sadeghi.a
"""
import pandas as pd
import os
import warnings

warnings.filterwarnings("ignore")
workingDir = r'D:\Logs\_App\BankLogCleanerPy'
os.chdir(workingDir)

from fixFuncs import *
blockList = []
blockErrorList = []
transferBlockList = []
transferBlockErrorsList = []
unblockList = []
unblockErrorsList = []

logsPath = r'D:\Logs\BlockingLogs'
folders = extractWantedFolders(logsPath)

for folder in folders:
    files = extractWantedFiles(logsPath, folder)
    
    for file in files:
        try:
            results = readRawText(logsPath, folder, file)
        except FileNotFoundError:
            continue
        
        if len(results) == 0: 
            continue
        results = removeProblematicChars(results)
        # [result for result in results if re.search('5979989', result)]
        fileType = detectFileType(file)
        data = createDataFrame(results, fileType)
        data = makeDataClean(data, fileType)
        data = enrichData(data, file, fileType)
        
        if fileType == "block":
            blockList.append(data)
        elif fileType == "blockErrors":
            blockErrorList.append(data)
        elif fileType == "transferBlock":
            transferBlockList.append(data)
        elif fileType == "transferBlockErrors":
            transferBlockErrorsList.append(data)
        elif fileType == "unblock":
            unblockList.append(data)
        elif fileType == "unblockErrors":
            unblockErrorsList.append(data)
            
del results
del data

if len(blockList) > 0:
    blocks = pd.concat(blockList)
    del blockList
if len(blockErrorList) > 0:
    blockError = pd.concat(blockErrorList)
    del blockErrorList
if len(transferBlockList) > 0:
    transferBlock = pd.concat(transferBlockList)
    del transferBlockList
if len(transferBlockErrorsList) > 0:
    transferBlockErrors = pd.concat(transferBlockErrorsList)
    del transferBlockErrorsList
if len(unblockList) > 0:
    unblock = pd.concat(unblockList)
    del unblockList
if len(unblockErrorsList) > 0:
    unblockErrors = pd.concat(unblockErrorsList)
    del unblockErrorsList

for file in files:
    fileType = detectFileType(file)
    if fileType == "block":
        blocks = makeBlockCodeClean(blocks)
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
    
# [code for code in blocks.BlockCode if re.search('\D+', code)]
            
blocks['EndedUpIn'] = ''

if 'transferBlock' in dir():
    inTransferBlock = blocks[blocks['BlockCode'].isin(transferBlock['BlockCode'])]
    inTransferBlock.loc[:, 'EndedUpIn'] = 'TransferBlock'
if 'unblock' in dir():
    inUnblock = blocks[blocks['BlockCode'].isin(unblock['BlockCode'])]
    inUnblock.loc[:, 'EndedUpIn'] = 'UnBlock'
resolved = pd.concat([inTransferBlock, inUnblock])
merged = blocks.merge(resolved, on = 'BlockCode', how = 'left', indicator = True)
notIncluded = merged.loc[merged['_merge'] == 'left_only', 'BlockCode']
unresolved = blocks[blocks['BlockCode'].isin(notIncluded)]
blocks = pd.concat([resolved, unresolved])
del inTransferBlock
del inUnblock
del resolved
del unresolved
del merged
del notIncluded
         
engine = createEngine()
for file in files:
    fileType = detectFileType(file)
    dbtypes = setDBTypes(fileType)
    if fileType == "block":
        blocksDB = pd.read_sql_table(table_name = 'Block', con = engine, schema= 'bankLog')
        blocksSet = set(blocks.BlockCode)
        blocksDBset = set(blocksDB.BlockCode)
        diffSet = blocksSet.difference(blocksDBset)
        blocks = blocks[blocks['BlockCode'].isin(diffSet)].drop_duplicates()
        fixColumnSize(blocks, fileType).to_sql(name = 'Block', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)
        del blocksDB
        del blocksSet
        del blocksDBset
        del blocks
    elif fileType == "blockErrors":
        blockErrorDB = pd.read_sql_table(table_name = 'BlockErrors', con = engine, schema = 'bankLog')
        blockErrorSet = set(blockError.BlockCode)
        blockErrorsDBset = set(blockErrorDB.BlockCode)
        diffSet = blockErrorSet.difference(blockErrorsDBset)
        blockError = blockError[blockError['BlockCode'].isin(diffSet)].drop_duplicates()
        fixColumnSize(blockError, fileType).to_sql(name = 'BlockErrors', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)
        del blockErrorDB
        del blockErrorSet
        del blockErrorsDBset
        del blockError
    elif fileType == "transferBlock":
        transferBlockDB = pd.read_sql_table(table_name = 'TransferBlock', con = engine, schema = 'bankLog')
        transferBlockSet = set(transferBlock.BlockCode)
        transferBlockDBset = set(transferBlockDB.BlockCode)
        diffSet = transferBlockSet.difference(transferBlockDBset)
        transferBlock = transferBlock[transferBlock['BlockCode'].isin(diffSet)].drop_duplicates()
        fixColumnSize(transferBlock, fileType).to_sql(name = 'TransferBlock', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)
        del transferBlockDB
        del transferBlockSet
        del transferBlockDBset
        del transferBlock
    elif fileType == "transferBlockErrors":
        transferBlockErrorsDB = pd.read_sql_table(table_name = 'TransferBlockErrors', con = engine, schema = 'bankLog')
        transferBlockErrorsSet = set(transferBlockErrors.BlockCode)
        transferBlockErrorsDBset = set(transferBlockErrorsDB.BlockCode)
        diffSet = transferBlockErrorsSet.difference(transferBlockErrorsDBset)
        transferBlockErrors = transferBlockErrors[transferBlockErrors['BlockCode'].isin(diffSet)].drop_duplicates()
        fixColumnSize(transferBlockErrors, fileType).to_sql(name = 'TransferBlockErrors', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)
        del transferBlockErrorsDB
        del transferBlockErrorsSet
        del transferBlockErrorsDBset
        del transferBlockErrors
    elif fileType == "unblock":
        unblockDB = pd.read_sql_table(table_name = 'UnBlock', con = engine, schema = 'bankLog')
        unblockSet = set(unblock.BlockCode)
        unblockDBset = set(unblockDB.BlockCode)
        diffSet = unblockSet.difference(unblockDBset)
        unblock = unblock[unblock['BlockCode'].isin(diffSet)].drop_duplicates()
        fixColumnSize(unblock, fileType).to_sql(name = 'UnBlock', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)
        del unblockDB
        del unblockSet
        del unblockDBset
        del unblock
    elif fileType == "unblockErrors":
        unblockErrorsDB = pd.read_sql_table(table_name = 'UnBlockErrors', con = engine, schema = 'bankLog')
        unblockErrorsSet = set(unblockErrors.BlockCode)
        unblockErrorsDBset = set(unblockErrorsDB.BlockCode)
        diffSet = unblockErrorsSet.difference(unblockErrorsDBset)
        unblockErrors = unblockErrors[unblockErrors['BlockCode'].isin(diffSet)].drop_duplicates()
        fixColumnSize(unblockErrors, fileType).to_sql(name = 'UnBlockErrors', con = engine, schema = 'bankLog', if_exists='append', index = False, dtype = dbtypes)
        del unblockErrorsDB
        del unblockErrorsSet
        del unblockErrorsDBset
        del unblockErrors
