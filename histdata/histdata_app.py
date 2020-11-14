import datetime
import queue
import sys
from dataclasses import dataclass

from PyQt5 import QtWidgets, QtCore, uic
from dateutil.relativedelta import relativedelta

from histdata import BrokerClient

LOG_QUEUE = queue.Queue()
FINISHED = object()


def clearQueue():
    LOG_QUEUE.mutex.acquire()
    LOG_QUEUE.queue.clear()
    LOG_QUEUE.all_tasks_done.notify_all()
    LOG_QUEUE.unfinished_tasks = 0
    LOG_QUEUE.mutex.release()


class MainWindow(QtWidgets.QWidget):

    def __init__(self, *args, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        uic.loadUi('histdata.ui', self)
        self.setFixedSize(600, 525)
        self.initControlValues()
        self.setConnections()

    def initControlValues(self):
        today = datetime.date.today()
        self.startDateEdit.setDate(today - relativedelta(months=1))
        self.endDateEdit.setDate(today)
        self.tickerEdit.setText("AMD")
        self.exchangeEdit.setText("SMART")
        self.secTypeEdit.setText("STK")
        self.currencyEdit.setText("USD")

    def setConnections(self):
        self.downloadButton.clicked.connect(self.download)

    def clearLogs(self):
        self.logView.clear()

    def appendLog(self, message: str):
        self.logView.addItem(message)

    def download(self):
        self.downloadButton.setEnabled(False)
        self.clearLogs()
        ticker = self.tickerEdit.text()
        exchange = self.exchangeEdit.text()
        secType = self.secTypeEdit.text()
        currency = self.currencyEdit.text()
        fromDate = self.startDateEdit.date().toPyDate()
        endDate = self.endDateEdit.date().toPyDate()
        barSize = self.barSizeComboBox.currentText()

        clearQueue()
        logPoller = LogQueuePoller(self.appendLog)
        logPoller.start()
        worker = DownloadHistDataTask(
            ConnectionParams("127.0.0.1", 4001, 10),
            ContractParams(ticker, secType, exchange, currency),
            HistInfoParams(fromDate, endDate, barSize)
        )
        worker.sig_error.connect(self.appendLog)
        worker.sig_error.connect(lambda _: self.downloadButton.setEnabled(True))
        worker.sig_done.connect(lambda _: self.downloadButton.setEnabled(True))
        worker.start()
        QtWidgets.QApplication.processEvents()


class LogQueuePoller(QtCore.QThread):
    def __init__(self, router):
        super(LogQueuePoller, self).__init__()
        self._router = router

    def __del__(self):
        self.wait()

    def run(self):
        while True:
            message = LOG_QUEUE.get()
            if message == FINISHED:
                LOG_QUEUE.task_done()
                break
            self._router(message)
            LOG_QUEUE.task_done()


class DownloadHistDataTask(QtCore.QThread):
    sig_done = QtCore.pyqtSignal(list, str)
    sig_error = QtCore.pyqtSignal(str)

    def __init__(self, connectionParams, contractParams, histInfoParam):
        super(DownloadHistDataTask, self).__init__()
        self._cn = connectionParams
        self._ct = contractParams
        self._hip = histInfoParam

    def __del__(self):
        self.wait()

    def run(self):
        ib = BrokerClient(
            self._cn.ipAddress,
            self._cn.port,
            self._cn.clientId
        )
        ib.register(self.routeLogs)
        if not ib.lowLevelClient.isConnected():
            self.sig_error.emit("Cannot connect to client")
            ib.disconnect()
            return

        contract = ib.buildContract(
            self._ct.ticker,
            self._ct.secType,
            self._ct.exchange,
            self._ct.currency
        )
        self.routeLogs("Downloading is started..")
        data = ib.fetchHistoricalData(
            contract,
            self._hip.fromDate,
            self._hip.endDate,
            self._hip.barSize
        )
        self.routeLogs("Downloading is finished")
        fileName = self.buildFileName(
            self._ct.ticker,
            self._hip.fromDate,
            self._hip.endDate,
            self._hip.barSize
        )
        ib.saveAsCsv(data, fileName)
        ib.disconnect()
        self.sig_done.emit(data, fileName)

    def routeLogs(self, message):
        LOG_QUEUE.put(message)
        print(message)

    def buildFileName(self, ticker, endDate, fromDate, barSize):
        delimiter = "_"
        return delimiter.join([ticker.lower(),
                               fromDate.strftime("%Y%m%d"),
                               endDate.strftime("%Y%m%d"),
                               barSize.replace(" ", "_")])


@dataclass
class ConnectionParams:
    ipAddress: str
    port: int
    clientId: int


@dataclass
class ContractParams:
    ticker: str
    secType: str
    exchange: str
    currency: str


@dataclass
class HistInfoParams:
    fromDate: datetime.datetime
    endDate: datetime.datetime
    barSize: str


def main():
    app = QtWidgets.QApplication(sys.argv)
    main = MainWindow()
    main.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
