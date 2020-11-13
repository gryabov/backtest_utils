import datetime
import sys

import finplot
import pandas as pd
import pyqtgraph as pg
from PyQt5 import QtWidgets, uic
from dateutil.tz import gettz


class MainWindow(QtWidgets.QMainWindow):

    def __init__(self, *args, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        uic.loadUi('chart.ui', self)
        self.initConnections()
        pg.setConfigOptions(foreground=finplot.foreground, background=finplot.background)
        fp = finplot.FinWindow(title="chart")
        self.ax = finplot.create_plot_widget(fp, init_zoom_periods=100)
        fp.ci.addItem(self.ax, row=0, col=0)
        fp.show_maximized = True
        self.plotWidget.setMaximumHeight(0)
        self.plotWidget.axs = [self.ax]  # finplot requires this property
        self.verticalLayout.addWidget(fp)
        self.dayDateEdit.setDate(datetime.date.today())
        self.priceLine = None
        self.stopPriceLine = None
        self.candleItems = None
        self.df = None
        self.filename = None
        self.isFileFirstOpen = True
        self.hoverLabel = finplot.add_legend('', ax=self.ax)
        finplot.set_time_inspector(self.update_legend_text, ax=self.ax, when='hover')
        finplot.display_timezone = gettz('America/New_York')

    def initConnections(self):
        self.actionOpen.triggered.connect(self.openFileActionCall)
        self.calculatePushButton.clicked.connect(self.updatePlot)

    def openFileActionCall(self):
        filename = QtWidgets.QFileDialog.getOpenFileName(self, 'Open File', filter="*.csv")
        self.filename = filename[0]
        self.isFileFirstOpen = False
        self.df = None

    def calculateQuotes(self, start_date: datetime, end_date: datetime):
        quotes = self.df.loc[(self.df['DateTime'] > start_date) & (self.df['DateTime'] < end_date)]
        quotes.reset_index(inplace=True)
        return quotes[['DateTime', 'Open', 'Close', 'High', 'Low']]

    def updateCandleItems(self, quotes):
        if not self.candleItems:
            self.candleItems = finplot.candlestick_ochl(quotes)
        else:
            self.candleItems.update_data(quotes)

    def updatePlot(self):
        if self.isFileFirstOpen:
            self.openFileActionCall()
            self.isFileFirstOpen = False

        if self.df is None:
            self.df = self.loadData(self.filename)
            minDateTime = min(self.df['DateTime'])
            self.dayDateEdit.setDate(minDateTime.date())

        end_date, start_date = self.calculateDateRange()

        if self.isDfHasDate(start_date):
            self.statusbar.showMessage('')

            quotes = self.calculateQuotes(start_date, end_date)
            self.updateCandleItems(quotes)

            fromTimestamp = quotes['DateTime'].min()
            toTimestamp = quotes['DateTime'].max()

            self.redrawEntryPointLine(fromTimestamp, toTimestamp)
            self.redrawStopLossLine(fromTimestamp, toTimestamp)

        else:
            self.statusbar.showMessage(f'No record for {start_date.day_name()}: {start_date}')

    def calculateDateRange(self):
        dateTimeFrom = self.dayDateEdit.dateTime().toPyDateTime()
        dateTimeTo = dateTimeFrom + datetime.timedelta(days=1)
        start_date = pd.to_datetime(dateTimeFrom, utc=True)
        end_date = pd.to_datetime(dateTimeTo, utc=True)
        return end_date, start_date

    def isDfHasDate(self, date: datetime) -> bool:
        return (self.df['DateTime'].dt.date == date).any()

    def redrawEntryPointLine(self, fromTimestamp, toTimestamp):
        priceText = self.priceLineEdit.text()
        self._redrawLine(self.priceLine, priceText, fromTimestamp, toTimestamp, '9900ff')

    def redrawStopLossLine(self, fromTimestamp, toTimestamp):
        priceText = self.stopPriceEdit.text()
        self._redrawLine(self.stopPriceLine, priceText, fromTimestamp, toTimestamp, 'ff0000')

    def _redrawLine(self, line, priceText: str, fromTimestamp, toTimestamp, color: str):
        if line:
            finplot.remove_line(line)

        if priceText:
            price = float(priceText)
            line = finplot.add_line((fromTimestamp, price), (toTimestamp, price), color=color, interactive=False)

    def loadData(self, filename: str):
        df = pd.read_csv(filename, usecols=['DateTime', 'Open', 'High', 'Low', 'Close'], na_values=['nan'])
        df['DateTime'] = pd.to_datetime(df['DateTime'], utc=True)
        df.reset_index(inplace=True)
        return df

    def update_legend_text(self, x, y):
        if self.df is not None:
            row = self.df.loc[self.df['DateTime'] == pd.to_datetime(pd.Timestamp(x * 1000000).floor('min'), utc=True)]
            if not row.empty:
                rawText = '<span style="font-size:13px">%s</span> &nbsp; O %s C %s H %s L %s'
                ticker = self.filename.split(sep="/")[-1].split(".")[0]
                self.hoverLabel.setText(rawText % (
                    ticker, row.Open.values[0], row.Close.values[0], row.High.values[0], row.Low.values[0]))


def main():
    app = QtWidgets.QApplication(sys.argv)
    main = MainWindow()
    finplot.show(qt_exec=False)
    main.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
