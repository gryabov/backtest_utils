Stock history downloader
===

## Prerequisites

1.. Install python3
```shell script
$ brew install python
```

2.. Install ibapi

```
1) Download "API Latest" from http://interactivebrokers.github.io/
2) Unzip or install (if its a .msi file) the download.
3) Go to tws-api/source/pythonclient/
4) Build a wheel with: python3 setup.py bdist_wheel
5) Install the wheel with:

python3 -m pip install --user --upgrade dist/ibapi-9.76.1-py3-none-any.whl
```

3.. Install python packages
```shell script
$  pip3 install -r ../requirements.txt
```

4.. Install IB Gateway from https://www.interactivebrokers.co.uk/en/index.php?f=16896

5.. Before run open the IB Gateway and log in your IB account


## How to run

You can use console or gui version

### Console version 

Change main part of the `histdata.py` script. Adjust date and stock info (ticker, sectype, exchange, currency)
```python
    contract = app.buildContract("AMD", "STK", "SMART", "USD")
    fromDate = date(2020, 11, 1)
    toDate = date(2020, 11, 11)
```
and run with 
```shell script
$  pyton3 histdata.py
````

### GUI version
Just run

```shell script
$  pyton3 histdata_app.py
````
