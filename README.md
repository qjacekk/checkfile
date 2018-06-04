# CheckFile
=====
CheckFile (cf) is Java command line tool to read Avro, Parquet or CSV files and generates a short overview and some statistics of the content (aka coverage report) like this:

> cf parquet_snappy > coverage_report.txt
> cat coverage_report.txt

Tested in 205 ms
File: parquet_snappy
Info: PARQUET compression: SNAPPY blocks: 1 rowCount: 1000 compressedSize: 25678 totalByteSize: 38369
=================
 Coverage report
=================
Total number of rows:            1000
field                          : count    : coverage : type             : comment
---------------------------------------------------------------------------------
boolean                        : 1000     : 100.00   : boolean (BOOLEAN) :
bytes                          : 1000     : 100.00   : bytes (BINARY)   :
double                         : 1000     : 100.00   : double (DOUBLE)  :
float                          : 1000     : 100.00   : float (FLOAT)    :
integer                        : 1000     : 100.00   : int (INT32)      :
long                           : 1000     : 100.00   : long (INT64)     :
string                         : 1000     : 100.00   : String (UTF8)    :
v_null                         : 0        : 0.00     : int (INT32)      : ALL_NULL
v_zero                         : 1000     : 100.00   : int (INT32)      :
vs_null                        : 1000     : 100.00   : String (UTF8)    : NULL_LITERALS

5 most frequent values
======================
field                          : count    : frequency : value
------------------------------------------------------------------------
boolean
                               : 500      : 50.00    : false
                               : 500      : 50.00    : true
bytes
                               : 1        : 0.10     : 0x31
                               : 1        : 0.10     : 0x3130
                               : 1        : 0.10     : 0x313030
                               : 1        : 0.10     : 0x31303030
                               : 1        : 0.10     : 0x313031
double
                               : 1        : 0.10     : 0.001
                               : 1        : 0.10     : 0.002
                               : 1        : 0.10     : 0.003
                               : 1        : 0.10     : 0.004
                               : 1        : 0.10     : 0.005
float
                               : 1        : 0.10     : 0.001
                               : 1        : 0.10     : 0.002
                               : 1        : 0.10     : 0.003
                               : 1        : 0.10     : 0.004
                               : 1        : 0.10     : 0.005
integer
                               : 1        : 0.10     : 1
                               : 1        : 0.10     : 2
                               : 1        : 0.10     : 3
                               : 1        : 0.10     : 4
                               : 1        : 0.10     : 5
long
                               : 1        : 0.10     : 1
                               : 1        : 0.10     : 2
                               : 1        : 0.10     : 3
                               : 1        : 0.10     : 4
                               : 1        : 0.10     : 5
string
                               : 1        : 0.10     : 1
                               : 1        : 0.10     : 10
                               : 1        : 0.10     : 100
                               : 1        : 0.10     : 1000
                               : 1        : 0.10     : 101
v_null
                               : --- NOT AVAILABLE ---
v_zero
                               : 1000     : 100.00   : 0
vs_null
                               : 1000     : 100.00   : Null


This tool has been created to help testing file-based ETL apps in Hadoop distributed systems.
This process involves generating a set of files by your ETL app (either to local fs or HDFS) and verifying the files content to check if all required fields have been correctly populated.
Since Linux distros usually do not provide support for file formats like Avro and Parquet and Hadoop tools are not easily portable, you need to transfer your files to a machine where those tools are available. Then, to e.g. check if some specific field gets populated you need to convert the file to CSV/JSON and then run some script to check the content.
This tool was created to solve this problem. The design goals were as follows:
- read both Avro and Parquet
- no dependency on Hadoop libs or any other heavy apps
- give a brief overview/stats of the content of the file + meta info (fields, field types, compression)
- run fast (relatively)
- support both Linux and Windows
- easy to install
- provide simplified conversion to CSV/JSON
- allow for filtering on specific field value

CheckFile requires Java >=1.7 and consists of a single jar file (and optionally a shell script to run it) that contains all libs necessary to read Avro and Parquet formats so it weights about 27 MB but it's still better than most of the tools available in Hadoop.

# Usage
=====
usage: cf [OPTIONS] <file_name>
Generate coverage and data validity report.
 -c                 convert to CSV (instead of generating coverage report
 -d <arg>           CSV delimiter (if not specified ftest will try to
                    guess)
 -f <filed=value>   generate report only for rows matching the filter
 -h                 help
 -j                 convert to JSON (instead of generating coverage report
 -lf                print least frequent samples (default: most frequent
 -m <arg>           number of sample values to include in the report
                    (default 5)
 -n <arg>           number of rows in CSV or JSON output (all by default
 -ns                do not sort fields alphabetically in the report (use
                    the original file order)
 -q                 enable quoting strings (only if -c was specified, this
                    may slow things down)

Examples:
cf data_file.parquet > coverage_report.txt
cf -f customer_id=10 -m 10 data_file.avro > coverage_report.txt
cf -j data_file.parquet > data_file.json

# Building and installing
======
CheckFile uses Maven. Requires Java 1.7 or newer.

# Clone the repository
cd checkfile
mvn clean package

# Install
# copy the jar and script file to any dir on the system PATH, e.g.:
cp target/checkfile-1.0-SNAPSHOT-jar-with-dependencies.jar ~/bin
cp src/main/scripts/cf ~/bin
chmod a+x ~/bin/cf

# Notes
=======
Building for the first time, especially if maven has just been installed it may take a few minutes to download all the dependencies and build the jar.

Please note that Parquet's BINARY and Avro's bytes are serialized to CSV/JSON as hex dumps (so it might be difficult to deserialize them back to original formats).

