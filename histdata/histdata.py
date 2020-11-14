import queue
from dataclasses import dataclass, field
from datetime import datetime, date
from threading import Thread
from typing import List, Callable

import pandas
from dateutil.relativedelta import relativedelta
from ibapi.client import EClient
from ibapi.contract import Contract as IBcontract
from ibapi.wrapper import EWrapper

DEFAULT_HISTORIC_DATA_ID = 50
DEFAULT_GET_CONTRACT_ID = 43

FINISHED = object()
STARTED = object()
TIME_OUT = object()

MAX_WAIT_SECONDS = 30


@dataclass
class Observable:
    observers: List[Callable] = field(default_factory=list)

    def register(self, observer: Callable):
        self.observers.append(observer)

    def unregister(self, observer: Callable):
        self.observers.remove(observer)

    def notify(self, *args, **kwargs):
        for observer in self.observers:
            observer(*args, **kwargs)


class _FinishableQueue(object):
    def __init__(self, queue_to_finish):
        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished, or a FINISHED flag is received in the queue

        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue = []
        finished = False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
                    # keep going and try and get more data

            except queue.Empty:
                # If we hit a time out it's most probable we're not getting a finished element any time soon
                # give up and return what we have
                finished = True
                self.status = TIME_OUT

        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT


class _Wrapper(EWrapper):
    """
    The wrapper deals with the action coming back from the IB gateway or TWS instance
    We override methods in EWrapper that will get called when this action happens, like currentTime
    Extra methods are added as we need to store the results in this object
    """

    def __init__(self):
        super().__init__()
        self._contractDetails = {}
        self._historicDataDict = {}
        self.initError()

    # error handling code
    def initError(self):
        errorQueue = queue.Queue()
        self._errorQueue = errorQueue

    def getError(self, timeout=5):
        if self.isError():
            try:
                return self._errorQueue.get(timeout=timeout)
            except queue.Empty:
                return None

        return None

    def isError(self):
        an_error_if = not self._errorQueue.empty()
        return an_error_if

    def error(self, id, errorCode, errorString):
        errorMsg = "IB error id %d error code %d string %s" % (id, errorCode, errorString)
        self._errorQueue.put(errorMsg)

    # get contract details code
    def initContractDetails(self, reqId):
        contract_details_queue = self._contractDetails[reqId] = queue.Queue()
        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        if reqId not in self._contractDetails.keys():
            self.initContractDetails(reqId)
        self._contractDetails[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        if reqId not in self._contractDetails.keys():
            self.initContractDetails(reqId)
        self._contractDetails[reqId].put(FINISHED)

    def initHistoricPriceQueue(self, tickerId):
        historic_data_queue = self._historicDataDict[tickerId] = queue.Queue()
        return historic_data_queue

    def historicalData(self, tickerId, bar):
        barData = (bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume)
        historic_data_dict = self._historicDataDict

        if tickerId not in historic_data_dict.keys():
            self.initHistoricPriceQueue(tickerId)
        historic_data_dict[tickerId].put(barData)

    def historicalDataEnd(self, tickerId, start: str, end: str):
        if tickerId not in self._historicDataDict.keys():
            self.initHistoricPriceQueue(tickerId)
        self._historicDataDict[tickerId].put(FINISHED)


class _Client(EClient, Observable):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        Observable.__init__(self)

    def resolveContract(self, ibContract, reqId=DEFAULT_GET_CONTRACT_ID):

        """
        From a partially formed contract, returns a fully fledged version

        :returns fully resolved IB contract
        """

        # Make a place to store the data we're going to return
        contract_details_queue = _FinishableQueue(self.wrapper.initContractDetails(reqId))

        self.notify("Getting full contract details from the server... ")

        self.reqContractDetails(reqId, ibContract)

        ## Run until we get a valid contract(s) or get bored waiting
        MAX_WAIT_SECONDS = 10
        new_contract_details = contract_details_queue.get(timeout=MAX_WAIT_SECONDS)

        while self.wrapper.isError():
            self.notify(self.wrapper.getError())

        if contract_details_queue.timed_out():
            self.notify("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        if len(new_contract_details) == 0:
            self.notify("Failed to get additional contract details: returning unresolved contract")
            return ibContract

        if len(new_contract_details) > 1:
            self.notify("got multiple contracts using first one")

        new_contract_details = new_contract_details[0]
        resolved_ibcontract = new_contract_details.contract
        return resolved_ibcontract

    def fetchHistoricalData(self, ibContract, endDataTime=datetime.today().strftime("%Y%m%d %H:%M:%S %Z"),
                            durationStr="1 Y", barSizeSetting="1 day", tickerId=DEFAULT_HISTORIC_DATA_ID):

        # Make a place to store the data we're going to return
        historic_data_queue = _FinishableQueue(self.wrapper.initHistoricPriceQueue(tickerId))
        self.reqHistoricalData(
            tickerId,  # reqId,
            ibContract,  # contract,
            endDataTime,  # endDateTime,
            durationStr,  # durationStr,
            barSizeSetting,  # barSizeSetting,
            "TRADES",  # whatToShow,
            1,  # useRTH,
            2,  # formatDate
            False,  # KeepUpToDate <<==== added for api 9.73.2
            []  # chartoptions not used
        )

        # Wait until we get a completed data, an error, or get bored waiting
        self.notify("Getting historical data from the server... could take %d seconds to complete " % MAX_WAIT_SECONDS)

        historic_data = historic_data_queue.get(timeout=MAX_WAIT_SECONDS)

        cancelTask = False
        while self.wrapper.isError():
            self.notify(self.wrapper.getError())
            cancelTask = True

        if historic_data_queue.timed_out():
            self.notify("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")
            cancelTask = True

        if cancelTask:
            self.cancelHistoricalData(tickerId)

        return historic_data


class BrokerClient(Observable):
    def __init__(self, ipaddress: str, port: int, clientId: int):
        Observable.__init__(self)
        self._wrapper = _Wrapper()
        self._client = _Client(wrapper=self._wrapper)
        self.connect(ipaddress, port, clientId)

    @property
    def lowLevelClient(self):
        return self._client

    def buildContract(self, symbol: str, secType: str, exchange: str, currency: str):
        contract = IBcontract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        return self._client.resolveContract(contract)

    def fetchHistoricalData(self, contract, fromDate, endDate, barSizeSetting):
        timeFormat = "%Y%m%d %H:%M:%S %Z"
        dateDuration = endDate - fromDate
        chunks = []
        currentMonth = (fromDate + relativedelta(months=1)) - fromDate
        if dateDuration.days <= currentMonth.days:
            dateStr = endDate.strftime(timeFormat)
            chunks = [(dateStr, f'{dateDuration.days} D')]
        else:
            offsetDate = endDate
            daysCount = dateDuration.days
            while True:
                dateStr = offsetDate.strftime(timeFormat)
                chunks.append((dateStr, '1 M'))
                month = offsetDate - (offsetDate - relativedelta(months=1))
                offsetDate -= relativedelta(months=1)
                daysCount -= month.days
                if daysCount <= 0:
                    break
        historicData = []
        counter = 1
        chunks.reverse()
        for endDateStr, durationStr in chunks:
            self.notify(endDateStr)
            data = self._client.fetchHistoricalData(contract, endDataTime=endDateStr, durationStr=durationStr,
                                                    barSizeSetting=barSizeSetting, tickerId=counter)
            historicData += data
            counter = counter + 1
        return historicData

    def connect(self, ipaddress: str, port: int, clientId: int):
        self._client.connect(ipaddress, port, clientId)
        thread = Thread(target=self._client.run)
        thread.start()
        setattr(self, "_thread", thread)
        self._wrapper.initError()

    def disconnect(self):
        if self._client:
            self._client.disconnect()

    def saveAsCsv(self, historicData, tickerName):
        df = pandas.DataFrame(historicData, columns=['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['DateTime'] = pandas.to_datetime(df['DateTime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        df.to_csv(f'{tickerName}.csv')
        self.notify(f'Data has been saved as {tickerName}.csv')

    def register(self, listener: Callable):
        super().register(listener)
        self._client.register(listener)

    def unregister(self, listener: Callable):
        super().unregister(listener)
        self._client.unregister(listener)


if __name__ == '__main__':
    app = BrokerClient("127.0.0.1", 4001, 2)
    app.register(print)
    contract = app.buildContract("AMD", "STK", "SMART", "USD")
    fromDate = date(2020, 11, 1)
    toDate = date(2020, 11, 11)  # = date.today()
    data = app.fetchHistoricalData(contract, fromDate, toDate, '1 min')
    app.saveAsCsv(data, 'AMD')
    app.disconnect()
