import pytest
import os
from efs_distributed.handler import FileHandlerProcess
from efs_distributed.measurements import MeasurementManager
from distributed import Client
import asyncio
import sys
records = [{
                   'url': 'https://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/foldChange/E079-H3K27me3.fc.signal.bigwig',
                   'file_type': 'bigwig', 'datatype': 'bp', 'name': 'E079-H3K27me3', 'id': 'E079-H3K27me3',
                   'genome': 'hg19', 'annotation': {'group': 'digestive', 'tissue': 'Esophagus', 'marker': 'H3K27me3'}},
               {
                   'url': 'https://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/foldChange/E106-H3K27me3.fc.signal.bigwig',
                   'file_type': 'bigwig', 'datatype': 'bp', 'name': 'E106-H3K27me3', 'id': 'E106-H3K27me3',
                   'genome': 'hg19',
                   'annotation': {'group': 'digestive', 'tissue': 'Sigmoid Colon', 'marker': 'H3K27me3'}}]
mMgr = MeasurementManager()
fms = mMgr.import_records(records, genome="hg19")

res = asyncio.run(fms[0].get_data("chr1", 1000000, 1002000, bins=100))
df = res[0]
def test_columns():
    columns = df.columns.tolist()
    assert columns==['start', 'end', 'E079-H3K27me3']

def test_range():
    start = 1000000
    end = 1002000
    for _, row in df.iterrows():
        assert (row['start'] <= end or row['end'] >= start)


def test_get_bytes():
    mMgr = MeasurementManager()
    fms = mMgr.import_records(records, genome="hg19")
    res , err = asyncio.run(fms[0].get_byteRanges("chr1", 1000000, 1002000, bins=100))
    df = res
    size = sys.getsizeof(df)
    assert size == 10727

def test_bin_rows():
    mMgr = MeasurementManager()
    fms = mMgr.import_records(records, genome="hg19")
    res = asyncio.run(fms[0].get_data("chr1", 1000000, 1002000, bins=100))
    df = res[0]
    rows = len(df)
    assert rows == 100

def test_mean():
    mMgr = MeasurementManager()
    fms = mMgr.import_records(records, genome="hg19")
    res = asyncio.run(fms[0].get_data("chr1", 1000000, 1002000, bins=100))
    df = res[0]
    mean = df['E079-H3K27me3'].mean()
    assert mean == 1.6159404346346855
