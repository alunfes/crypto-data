import websocket
import threading
import time
import json
import asyncio
from statistics import mean, median,variance,stdev
from datetime import datetime
import pandas as pd
import pytz
import numpy as np
from sortedcontainers import SortedDict
from S3Master import S3Master
from LineNotification import LineNotification


class WebsocketMaster:
    def __init__(self, channel, symbol=''):
        self.symbol = symbol
        self.ticker = None
        self.message = None
        self.exection = None
        self.channel = channel
        self.connect()

    def connect(self):
        self.ws = websocket.WebSocketApp(
            'wss://ws.lightstream.bitflyer.com/json-rpc', header=None,
            on_open = self.on_open, on_message = self.on_message,
            on_error = self.on_error, on_close = self.on_close)
        self.ws.keep_running = True
        websocket.enableTrace(False)
        #self.thread = threading.Thread(target=lambda: self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}))
        self.thread = threading.Thread(target=lambda: self.ws.run_forever())
        self.thread.daemon = True
        self.thread.start()

    def is_connected(self):
        return self.ws.sock and self.ws.sock.connected

    def disconnect(self):
        print('disconnected')
        self.ws.keep_running = False
        self.ws.close()


    def on_message(self, ws, message):
        message = json.loads(message)['params']
        self.message = message['message']
        if self.channel == 'lightning_executions_' and self.symbol =='FX_BTC_JPY':
            if self.message is not None:
                self.exection = self.message
                TickData.add_exec_data(self.exection)
        elif self.channel == 'lightning_board_snapshot_':
            TickData.add_board(message)
        elif self.channel == 'lightning_board_':
            TickData.add_board(message)
        elif self.channel == 'lightning_ticker_' and self.symbol =='FX_BTC_JPY':
            if self.message is not None:
                self.ticker = self.message
                TickData.add_ticker_data(self.ticker)
        elif self.channel == 'lightning_executions_' and self.symbol =='BTC_JPY':
            if self.message is not None:
                TickData.add_btc_data(self.message)

    def on_error(self, ws, error):
        print('websocket error!')
        try:
            if self.is_connected():
                self.disconnect()
        except Exception as e:
            print('websocket - '+str(e))
        time.sleep(3)
        self.connect()

    def on_close(self, ws):
        print('Websocket disconnected')


    def on_open(self, ws):
        ws.send(json.dumps( {'method':'subscribe',
            'params':{'channel':self.channel + self.symbol}} ))
        time.sleep(1)
        print('Websocket connected for '+self.channel + self.symbol)


    async def loop(self):
        while True:
            await asyncio.sleep(1)



'''
TickData class
'''
class TickData:
    @classmethod
    def initialize(cls):
        cls.exec_lock = threading.Lock()
        cls.ticker_lock = threading.Lock()
        cls.board_lock = threading.Lock()
        cls.btc_lock = threading.Lock()
        cls.exec_data = []
        cls.ticker_data = []
        cls.btc_data = []
        cls.num_exec_write = 0
        cls.num_btc_write = 0
        cls.num_board_write = 0
        cls.board_bids = SortedDict()
        cls.board_asks = SortedDict()
        cls.ws_execution = WebsocketMaster('lightning_executions_', 'FX_BTC_JPY')
        cls.ws_ticker = WebsocketMaster('lightning_ticker_', 'FX_BTC_JPY')
        cls.ws_board = WebsocketMaster('lightning_board_', 'FX_BTC_JPY')
        cls.ws_board_snap = WebsocketMaster('lightning_board_snapshot_', 'FX_BTC_JPY')
        cls.ws_btc = WebsocketMaster('lightning_executions_', 'BTC_JPY')
        cls.JST = pytz.timezone('Asia/Tokyo')
        th = threading.Thread(target=cls.start_thread)
        th.start()

    @classmethod
    def start_thread(cls):
        while True:
            time.sleep(10)

    @classmethod
    def add_exec_data(cls, exec):
        if len(exec) > 0:
            with cls.exec_lock:
                cls.exec_data.extend(exec)
                if len(cls.exec_data) >= 50000:
                    file_name = 'executions_'+str(cls.num_exec_write)+'.csv'
                    cls.__write_executions(file_name)
                    cls.exec_data = []
                    cls.num_exec_write += 1
                    S3Master.save_file('./Data/'+file_name)
                    LineNotification.send_error('uploaded '+str(file_name))

    @classmethod
    def add_ticker_data(cls, ticker):
        if len(ticker) is not None:
            with cls.ticker_lock:
                cls.ticker_data.append(ticker)
                if len(cls.ticker_data) >= 30000:
                    del cls.ticker_data[:-10000]
        else:
            print(ticker)

    @classmethod
    def add_board(cls, message):
        message = message['message']
        with cls.board_lock:
            cls.board_bids = cls.update_board(message['bids'])
            cls.board_asks = cls.update_board(message['asks'])

    @classmethod
    def add_btc_data(cls, message):
        if len(message) > 0:
            with cls.btc_lock:
                cls.btc_data.extend(message)
                if len(cls.btc_data) >= 36000:
                    file_name = 'btc_' + str(cls.num_btc_write) + '.csv'
                    cls.__write_btc(file_name)
                    cls.btc_data = []
                    cls.num_btc_write += 1
            S3Master.save_file('./Data/' + file_name)
            S3Master.remove_trush()
            LineNotification.send_error('uploaded ' + str(file_name))

    @classmethod
    def update_board(cls,d):
        board = SortedDict()
        for i in d:
            p, s = int(i['price']), i['size']
            if s != 0:
                    board[p] = s
            elif p in board:
                del board[p]
        return board

    @classmethod
    def get_bids(cls):
        with cls.board_lock:
            return cls.board_bids

    @classmethod
    def get_asks(cls):
        with cls.board_lock:
            return cls.board_asks


    '''
    {'id': 1142130291, 'side': 'SELL', 'price': 1314238.0, 'size': 0.01736666, 'exec_date': '2019-06-30T12:33:40.1800448Z',
     'buy_child_order_acceptance_id': 'JRF20190630-123338-155640', 'sell_child_order_acceptance_id': 'JRF20190630-123340-374671'}]
    '''
    @classmethod
    def __write_executions(cls, name):
        df = pd.DataFrame(cls.exec_data, columns=['id', 'side', 'price', 'size', 'exec_date',
                                                  'buy_child_order_acceptance_id', 'sell_child_order_acceptance_id'])
        df.to_csv('./Data/'+name)

    '''
    [{'id': 1142143550, 'side': 'BUY', 'price': 1258499.0, 'size': 0.199, 'exec_date': '2019-06-30T13:03:49.0080924Z', 
    'buy_child_order_acceptance_id': 'JRF20190630-130348-228620', 'sell_child_order_acceptance_id': 'JRF20190630-130344-552633'}]
    '''
    @classmethod
    def __write_btc(cls, name):
        df = pd.DataFrame(cls.btc_data, columns=['id', 'side', 'price', 'size', 'exec_date',
                                                  'buy_child_order_acceptance_id', 'sell_child_order_acceptance_id'])
        df.to_csv('./Data/'+name)

    @classmethod
    def __write_boards(cls):
        pass




if __name__ == '__main__':
    TickData.initialize()
    i = 0
    while True:
        print()
        '''
        for p, s in reversed(TickData.get_asks().items()[:10]):
            print(p, '{:>10.3f}'.format(s))
        print('======== BOARD ========')
        for p, s in reversed(TickData.get_bids().items()[-10:]):
            print(p, '{:>10.3f}'.format(s))
        print('')
        '''
        time.sleep(10)

