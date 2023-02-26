# -*- coding: utf-8 -*-
"""
Created on Sun Feb 26 16:26:03 2023

@author: sadeghi.a
"""
import sqlalchemy as sa

def createEngine():
    config = 'mssql+pyodbc://BankLogManagement:SJTu4T1QcsgiWbCI@172.16.2.29/Bank?driver=SQL+Server+Native+Client+11.0'
    return sa.create_engine(config)

engine = createEngine()

updateInTransferBlocksQuery = """
update bankLog.[Block]
set bankLog.[Block].EndedUpIn = 'TransferBlock'
where bankLog.[Block].EndedUpIn Is Null And
bankLog.[Block].BlockCode in (select DocumentNumber from bankLog.[TransferBlock])
"""
engine.execute(updateInTransferBlocksQuery)


updateInUnBlocksQuery = """
update bankLog.[Block]
set bankLog.[Block].EndedUpIn = 'UnBlock'
where bankLog.[Block].EndedUpIn Is Null And
bankLog.[Block].BlockCode in (select BlockCode from bankLog.[UnBlock])
"""
engine.execute(updateInUnBlocksQuery)

engine.dispose()
