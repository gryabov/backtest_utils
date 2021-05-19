import datetime
import sys

import finplot
import pandas as pd
import pyqtgraph as pg
from PyQt5 import QtWidgets, uic
from dateutil.tz import gettz


class EntryStopLine:
    def __init__(self):
        pass

    def redraw(self, df, entryPrice, stopPrice, fromTimestamp, toTimestamp):
        y_max = df['High'].max()
        y_min = df['Low'].min()
        int_step = (y_max - y_min) / 100
        first_intersection = None

        self._redrawEntryPointLine(entryPrice, fromTimestamp, toTimestamp)
        if entryPrice:
            first_intersection = self._drawEntryPriceIntersection(df, entryPrice, first_intersection, int_step)

        self._redrawStopLossLine(stopPrice, fromTimestamp, toTimestamp)
        if stopPrice and first_intersection is not None:
            self._drawStopPriceIntersection(df, first_intersection, stopPrice, y_max, y_min)

    def _drawStopPriceIntersection(self, df, first_intersection, stopPrice, y_max, y_min):
        price = float(stopPrice)
        stop_df = df[df['DateTime'] > first_intersection['DateTime']]
        intersections = stop_df[self._ochlIntersectionMask(stop_df, price)]
        intersections.reset_index(inplace=True)
        size = len(intersections)
        if size > 0:
            candle = intersections.iloc[0]
            finplot.add_line((candle['DateTime'], y_min), (candle['DateTime'], y_max),
                             color='f7ff00', interactive=False, width=3)

    def _drawEntryPriceIntersection(self, df, entryPrice, first_intersection, int_step):
        price = float(entryPrice)
        intersections = df[self._ochlIntersectionMask(df, price)]
        intersections.reset_index(inplace=True)
        size = len(intersections)
        if size > 0:
            first_intersection = intersections.iloc[0]
            candle = intersections.iloc[0]
            finplot.add_line((candle['DateTime'], price + int_step), (candle['DateTime'], price - int_step),
                             color='9900ff', interactive=False, width=3)
        return first_intersection

    def _redrawEntryPointLine(self, price: str, fromTimestamp, toTimestamp):
        self._redrawLine(price, fromTimestamp, toTimestamp, '9900ff')

    def _redrawStopLossLine(self, price: str, fromTimestamp, toTimestamp):
        self._redrawLine(price, fromTimestamp, toTimestamp, 'ff0000')

    def _redrawLine(self, priceText: str, fromTimestamp, toTimestamp, color: str):
        if priceText:
            price = float(priceText)
            finplot.add_line((fromTimestamp, price), (toTimestamp, price), color=color, interactive=False)

    def _span(self, l, r, value):
        return (l >= value) & (r <= value)

    def _ochlIntersectionMask(self, df, value):
        o = df['Open']
        c = df['Close']
        h = df['High']
        l = df['Low']
        return self._span(h, o, value) | self._span(h, c, value) \
               | self._span(o, c, value) | self._span(c, o, value) \
               | self._span(o, l, value) | self._span(c, l, value)


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, *args, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        uic.loadUi('chart.ui', self)
        self.initConnections()
        pg.setConfigOptions(foreground=finplot.foreground, background=finplot.background)
        fp = finplot.FinWindow(title="chart")
        self.ax = finplot.create_plot_widget(fp, init_zoom_periods=500)
        fp.ci.addItem(self.ax, row=0, col=0)
        fp.show_maximized = True
        self.plotWidget.setMaximumHeight(0)
        self.plotWidget.axs = [self.ax]  # finplot requires this property
        self.verticalLayout.addWidget(fp)
        self.dayDateEdit.setDate(datetime.date.today())
        self.candleItems = None
        self.df = None
        self.filename = None
        self.isFileFirstOpen = True
        self.esLines= EntryStopLine()
        self.hoverLabel = finplot.add_legend('', ax=self.ax)
        finplot.set_time_inspector(self.updateLegend, ax=self.ax, when='hover')
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

    def updateCandlePane(self, quotes):
        self.ax.reset()
        finplot.candlestick_ochl(quotes)
        finplot.refresh()
        self.hoverLabel = finplot.add_legend('', ax=self.ax)
        finplot.set_time_inspector(self.updateLegend, ax=self.ax, when='hover')

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
            self.updateCandlePane(quotes)

            fromTimestamp = quotes['DateTime'].min()
            toTimestamp = quotes['DateTime'].max()
            entryPrice = self.priceLineEdit.text()
            stopPrice = self.stopPriceEdit.text()
            self.esLines.redraw(quotes, entryPrice, stopPrice, fromTimestamp, toTimestamp)

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

    def loadData(self, filename: str):
        df = pd.read_csv(filename, usecols=['DateTime', 'Open', 'High', 'Low', 'Close'], na_values=['nan'])
        df['DateTime'] = pd.to_datetime(df['DateTime'], utc=True)
        df.reset_index(inplace=True)
        return df

    def updateLegend(self, x, y):
        if self.df is not None:
            row = self.df.loc[self.df['DateTime'] == pd.to_datetime(pd.Timestamp(x).floor('min'), utc=True)]
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
