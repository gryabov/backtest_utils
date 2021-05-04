import sys
from collections import OrderedDict

import calplot
import pandas as pd


def loadData(filename: str):
    df = pd.read_csv(filename, usecols=['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume'], na_values=['nan'])
    df['DateTime'] = pd.to_datetime(df['DateTime'], utc=True).dt.tz_convert('US/Eastern')
    df = df.set_index('DateTime')
    return df


def resample(df):
    return df.resample('1min').agg(
        OrderedDict([
            ('Open', 'first'),
            ('High', 'max'),
            ('Low', 'min'),
            ('Close', 'last'),
            ('Volume', 'sum'),
        ])
    ).dropna()


if __name__ == '__main__':
    file_path = sys.argv[1]
    data_frame = loadData(file_path)
    # data_frame = resample(data_frame)
    data_frame['hasDay'] = 1
    fig, _ = calplot.calplot(data_frame['hasDay'], cmap='Blues', colorbar=False)
    print(f"Calendar hitmap has been saved to {file_path}_hitmap.png")
    fig.savefig(f"{file_path}_hitmap.png")
