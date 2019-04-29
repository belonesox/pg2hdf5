#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2018, Stas Fomin <stas-fomin@yandex.ru>

import itertools
import psycopg2
import numpy as np
import h5py
import csv
import io
import gzip

import itertools

def sql2hdf5(sql, hdf5filename, con, type_hints={}):
    """
    Read SQL query, return a DataFrame.

    Parameters
    ----------
    sql : SQL string with all parameters substituted
    con : connectable (django connection, or psycopg connection) 
    """
    
    # We have to estimate number of rows for one-time allocation of numpy arrays
    
    count = None
    if not (hdf5filename.endswith('.csv.gz') or hdf5filename.endswith('.csv')):
        sql_count = "select count(*) from (%s) s" % sql
        cursor = con.cursor()
        cursor.execute(sql_count)
        count = cursor.fetchone()[0]
        cursor.close()
        if count == 0:
            return None

    # Funny way to reliable get psycopg connection. We need it to get server-side cursors.
    pgcon = con.cursor().connection
    
    # with transaction.atomic():
    if True:
        cursor = pgcon.cursor("serversidecursor", withhold=True)
        pgcon.commit()
        chunk_size = int(max(min(count/10, 100000), 10))
        cursor.itersize = chunk_size 
        cursor.execute(sql)
        row0 = cursor.fetchone()

        columns = []
        dtypes = []

        for i, col_desc in enumerate(cursor.description):
            col_name = col_desc[0]
            dtype = None
            if col_name in type_hints:
                dtype = np.dtype(type_hints[col_name])
            else:    
                if col_desc.type_code == 25:
                    dtype = np.dtype('S16')
                    pass
                if col_desc.type_code == 16:
                    dtype = np.dtype(bool)
                elif col_desc.type_code == 1043:
                    dtype = np.dtype('S16')
                    pass
                    # dtype = np.object
                elif col_desc.type_code == 1082:
                    dtype = np.dtype('i8') #'datetime64[D]'
                elif col_desc.type_code == 700:
                    if col_desc.internal_size == 2:
                        dtype = np.float16
                    elif col_desc.internal_size == 2:
                        dtype = np.int16
                    elif col_desc.internal_size == 4:
                        dtype = np.float32
                    elif col_desc.internal_size == 8:
                        dtype = np.float64
                    else:
                        assert "Float with undefined length"
                elif col_desc.type_code == psycopg2.NUMBER:
                    if col_desc.internal_size == 1:
                        dtype = np.int8
                    elif col_desc.internal_size == 2:
                        dtype = np.int16
                    elif col_desc.internal_size == 4:
                        dtype = np.int32
                    elif col_desc.internal_size == 8:
                        dtype = np.int64
                    else:
                        assert "Unknown number type"
            columns.append(col_name)
            dtypes.append( (col_name, dtype) )

        
        hdf5file = None
        h5 = None
    
        row_num = 0
        csvwriter = None

        if hdf5filename.endswith('.csv.gz'):
            # csvfile = open(hdf5filename, 'w', newline='')
            with gzip.open(hdf5filename, "w") as file:
                writer = csv.writer(io.TextIOWrapper(file, newline="", write_through=True))
                writer.writerow(columns)
                writer.writerow(row0)
                # writer = csv.writer(csvfile)
                writer.writerows(cursor)
        elif hdf5filename.endswith('.csv'):
            with open(hdf5filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(columns)
                writer.writerow(row0)
                writer.writerows(cursor)
        else:
            # Todo: automatically build Category types from string fields
            hdf5file = h5py.File(hdf5filename, 'w') # driver="core")
            # h5 = hdf5file.create_dataset('data', shape=(count,), dtype=dtypes, chunks = (count,), compression="gzip", compression_opts=0)
            h5 = hdf5file.create_dataset('data', shape=(count,), dtype=dtypes, chunks = (count,))

            h5[:] = cursor
            
            for row in cursor:
                # for row in rows:
                if row_num < count:
                    h5[row_num] = row
                    # for j in range(len(row)):
                    #     h5[row_num][j] = row[j]
                row_num += 1
                    
               #todo: resize arrays if  i < count
               # It possible, if result set changed between row count calculation,
               # of if we use random sampling in SQL select.
                pass
            hdf5file.close()

        cursor.close()
        del cursor
    pass


def hdf52pd(hdf5filename):
    import pandas as pd
    with h5py.File(hdf5filename) as hf:
        h5 = hf['data']
        df = pd.DataFrame(h5[:])
        return df



