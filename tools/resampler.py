import sys
from collections import OrderedDict

import pandas as pd


def usage():
    print("Usage:  python3 resampler.py from_file to_file resampling_value")
    sys.exit()

if __name__ == '__main__':
    if len(sys.argv) <= 3:
        usage()

    from_file_path = sys.argv[1]
    to_file_path = sys.argv[2]
    resampling_value = sys.argv[3]

    if from_file_path == from_file_path:
        print("Error: FROM and TO files are the same")
        usage()

    df = pd.read_csv(from_file_path, usecols=['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume'], na_values=['nan'])
    df['DateTime'] = pd.to_datetime(df['DateTime'], utc=True).dt.tz_convert('US/Eastern')
    df = df.set_index('DateTime')
    df = df.resample(resampling_value).agg(
        OrderedDict([
            ('Open', 'first'),
            ('High', 'max'),
            ('Low', 'min'),
            ('Close', 'last'),
            ('Volume', 'sum'),
        ])
    ).dropna()

    df.to_csv(to_file_path)
    print(f"Saved to {to_file_path}")