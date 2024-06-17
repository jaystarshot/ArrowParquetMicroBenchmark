# Arrow-Parquet C++ Vs Python Benchmark

## Overview
This repository contains programs for reading Parquet files using C++ Arrow vs. PyArrow. Both programs set allow arrow threadpool to 1 and I assume that Arrow internally uses concurrency while reading. Enabling ``arrow_reader_properties.set_use_threads(true)`` appears to confirm this in C++ as it is the default configuration in Python but is disabled in C++.


## Results
Tested with arrow version 16.1.0
The results don't show any significant python overhead

### Smaller file
0.5Mb Gzip compressed

| Method   | Average Time (seconds) |
|----------|------------------------|
| Py Arrow | 0.0136721134           |
| C++ Arrow| 0.00501744             |


### Larger file
300Mb Zstd compressed

| Method   | Average Time (seconds) |
|----------|------------------------|
| Py Arrow | 4.3928                 |
| C++ Arrow| 4.2719                 |

## Running the programs
Both the C++ and Python programs accept a Python file as input, along with the number of iterations to run. The default number of iterations is 100

## Setup

### Python
Install pyarrow

```sh
pip install pyarrow
```
and run the benchmark.py
```sh
python benchmark.py <<path_to_file>> <<iterations>>
```
### C++ Arrow

Install C++ arrow in your system following https://arrow.apache.org/install/

```sh
make
cd build/
```

```sh
./benchmark <<path_to_file>> <<iterations>>
```
