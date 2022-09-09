# Lab 1
> 1st lab for CS5344 22 FALL
## Brief Introduction
Input `meta_Musical_Instruments.json.gz` and `reviews_Musical_Instruments.json.gz`, find top 10 products with the greatest `#reviews`, output includes `ID`, `#reviews`, `average rating` and `price`. Implemented in PySpark. 
## Data
Open source Amazon product dataset of musical instruments (reviews & metadata). Download from [this website](http://jmcauley.ucsd.edu/data/amazon/links.html). 
## Environment
> All experiments are conducted under `MacOS 11.6.8`
- `Python 3.10.6`
- `Scala 2.13.8`
- `PySpark 3.3.0`
## Command
```shell
spark-submit main.py [PATH: metadata] [PATH: reviews] [PATH: outfile]
```
or
```shell
pip3 install --upgrade pyspark
python3 main.py [PATH: metadata] [PATH: reviews] [PATH: outfile]
```
the latter command should be fulfilled with prerequesites 
## Usage
- Download data and put them in the (sub)folder where `main.py` locates. 
  - Data can be either original `.json.gz` or unzipped `.json`
- Run command and set `outfile` the path to save output files
