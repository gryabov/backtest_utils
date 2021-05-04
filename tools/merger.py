import sys

import pandas as pd
import calplot
from matplotlib.figure import Figure
from pandas import DataFrame


def loadData(filename: str):
    df = pd.read_csv(filename, usecols=['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume'], na_values=['nan'])
    df['DateTime'] = pd.to_datetime(df['DateTime'], utc=True).dt.tz_convert('US/Eastern')
    return df.set_index('DateTime')


def createFilePrefix(data_frame):
    minDateTime = min(data_frame.index).date().strftime("%Y%m%d")
    maxDateTime = max(data_frame.index).date().strftime("%Y%m%d")
    return maxDateTime + "_" + minDateTime


def saveHitmap(file_prefix: str, figure: Figure):
    figure.savefig(f"{file_prefix}_hitmap.png")


def saveDebugStat(file_prefix: str, data_frame: DataFrame):
    data_frame.groupby(data_frame.index.date).count().to_csv(f'{file_prefix}_stat.csv')


def saveMergedData(file_prefix: str, data_frame: DataFrame):
    data_frame.to_csv(f'{file_prefix}_merged.csv')


if __name__ == '__main__':
    filenames = sys.argv[1:]
    dfs = [loadData(filename) for filename in filenames]
    df = pd.concat(dfs, join='outer')
    df['hasDay'] = 1
    df.drop_duplicates(inplace=True)
    df.sort_index(inplace=True)
    fig, _ = calplot.calplot(df['hasDay'], cmap='Blues', colorbar=False)

    prefix = createFilePrefix(df)
    saveMergedData(prefix, df)
    saveHitmap(prefix, fig)
    # saveDebugStat(prefix, df)
