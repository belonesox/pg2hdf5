# pg2hdf5

## The Problem


For medium size ML/Datascience projects (≈100M rows), based on data from Postgres Databases, working with lot of separated datasets in CSV is very unefficient.

Will be more convinient not to download it as CSV files and doing joins on Pandas datasets,
but create materialized views, using regular RDMS joins, windows functions and so on.

But the problem, how to efficiently download it to Pandas dataframe, remains.

Because official way, with ``read_query`` from ``Pandas`` we get following problems:
* Exhausting of memory during select operations, because of use classical client side cursors (all these 100M rows will be downloaded on client)
   * Yes, it possible to work with ``chunksize`` with it, but users (data scientists) often forgot about it.
* Resulting datasets will be unoptimal, because data types of columns in querysets will be ignored. So ``int2`` or ``float32`` fields will be converted to ``int64/float64`` doubling memory consumption (also unwanted «int→float» conversion if exists NULL in the column).
  * A lot of copy/transformations opperations in process. 

All these results in memory exhaustion and dying Jupyter Kernel.

Here I tried:
* Onetime calculate resultset size, and allocate numpy arrays before downloading.
* Numpy arrays created accordingly Postgres types of columns in query.
* Download result set to HDF5 using server side cursor.


# Installation

```
sudo pip install git+https://github.com/belonesox/pg2hdf5.git 
```
or checkout the project and run usual
```
python setup.py install
```


# Using

```
import pg2hdf5

…

django.setup()
conn = django.db.connection

…

df = pg2hdf5.sql2hdf5("select * from my_data_set", "hfdfilename.hf5")
```

# ToDo
Automatically create categorial columns for string columns.

