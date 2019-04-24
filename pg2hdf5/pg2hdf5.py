#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2018, Stas Fomin <stas-fomin@yandex.ru>

import itertools
import psycopg2
import numpy as np

import itertools

def sql2hdf5(sql, hdf5filename, con):
    """
    Read SQL query, return a DataFrame.

    Parameters
    ----------
    sql : SQL string with all parameters substituted
    con : connectable (django connection, or psycopg connection) 
    """
    
    # We have to estimate number of rows for one-time allocation of numpy arrays
    sql_count = "select count(*) from (%s) s" % sql
    cursor = con.cursor()
    cursor.execute(sql_count)
    count = cursor.fetchone()[0]

    # Funny way to reliable get psycopg connection. We need it to get server-side cursors.
    pgcon = con.cursor().connection
    cursor.close()
    
    if count == 0:
        return None
    
    # with transaction.atomic():
    if True:
        cursor = pgcon.cursor("serversidecursor", withhold=True)
        pgcon.commit()
        chunk_size = int(max(min(count/10, 100000), 10))
        cursor.itersize = chunk_size 
        cursor.execute(sql)
        
        columns = []
        arrays = []
    
        row_num = 0
        
        # while True:
        #     rows = cursor.fetchmany(chunk_size)
        for row in cursor:
            # if not rows:
            #     break
    
            if not arrays:
                # Now we only support int/float types.
                # Todo: automatically build Category types from string fields
                for i, col_desc in enumerate(cursor.description):
                    columns.append(col_desc[0])
                    dtype = None
                    if col_desc.type_code == 25:
                        dtype = np.dtype('U')
                    if col_desc.type_code == 16:
                        dtype = np.dtype(bool)
                    elif col_desc.type_code == 1043:
                        dtype = np.object
                    elif col_desc.type_code == 1082:
                        dtype = 'datetime64[D]'
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
                    thearray = np.empty((count,), dtype=dtype)
                    arrays.append(thearray)
                columns = _ensure_index(columns)
                
            # for row in rows:
            if row_num < count:
                for j, thearray in enumerate(arrays):
                    if row[j]:
                        thearray[row_num] = row[j]
                    else:
                        pass
            row_num += 1
                
           #todo: resize arrays if  i < count
           # It possible, if result set changed between row count calculation,
           # of if we use random sampling in SQL select.
            pass

        cursor.close()
        del cursor

    mgr = _arrays_to_mgr(arrays, columns, None, columns)
    return pandas.core.api.DataFrame(mgr)