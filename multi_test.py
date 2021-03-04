#!/usr/bin/python3

import threading
import time
import baostock as bs
import pandas as pd

result = pd.DataFrame() 
class myThread (threading.Thread):
    def __init__(self, threadID, stocks, startDay, endDay, lastTradeDay):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.stocks = stocks
        self.startDay = startDay
        self.endDay = endDay
        self.lastTradeDay = lastTradeDay
    def run(self):
        print ("开启线程：", self.threadID)
        global result
        # 获取锁，用于线程同步
        threadLock.acquire()
        result = result.append(getStocksDetail(self.stocks, self.startDay, self.endDay, 'd', '2', self.lastTradeDay))
        # 释放锁，开启下一个线程
        threadLock.release()
        print ("关闭线程：", self.threadID)

# 获取交易日
def getTradeDays(start_date, end_date) :
    tradeDays = bs.query_trade_dates(start_date, end_date)
    # print('query_trade_dates respond error_code:'+tradeDays.error_code)
    # print('query_trade_dates respond  error_msg:'+tradeDays.error_msg)
    tradeDays_data_list = []
    while (tradeDays.error_code == '0') & tradeDays.next():
        tradeDays_data_list.append(tradeDays.get_row_data())
    return pd.DataFrame(tradeDays_data_list, columns=tradeDays.fields)

#获取股票列表
def getStocks(tradeDay) :
    stocks = bs.query_all_stock(tradeDay)
    # print('query_all_stock respond error_code:'+stocks.error_code)
    # print('query_all_stock respond  error_msg:'+stocks.error_msg)
    stocks_data_list = []
    while (stocks.error_code == '0') & stocks.next():
        stocks_data_list.append(stocks.get_row_data())
    return pd.DataFrame(stocks_data_list, columns=stocks.fields)

# 获取股票详情
def getStocksDetail(stocks, startDay, endDay, frequency, adjustflag, lastTradeDay):
    stock_data_list=[]
    stock_fields = []
    for index, stock in stocks.iterrows():
        if stock['tradeStatus'] == '1' and 'sh.600' in stock['code']:
            rs=bs.query_history_k_data(stock['code'], "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",startDay,endDay,frequency, adjustflag)
            print('请求历史数据返回信息:'+rs.error_msg)
            stock_fields = rs.fields
            while(rs.error_code=='0')&rs.next():
                rowData = rs.get_row_data()
                stock_data_list.append(rowData)

    # todo 添加名称、添加昨日涨跌幅
    return pd.DataFrame(stock_data_list,columns=stock_fields)

lg = bs.login()
# print('login respond error_code:'+lg.error_code)
# print('login respond  error_msg:'+lg.error_msg)

startDay = "2021-03-01"
endDay = "2021-03-01"

threads = []
lastTradeDay = ""
threadLock = threading.Lock()
tradeDaysResult = getTradeDays("2021-03-01", "2021-03-01")
for index, tradeDay in tradeDaysResult.iterrows():
    if tradeDay['is_trading_day'] == '1':
        stocksResult = getStocks(tradeDay['calendar_date'])
        page = 1 
        limit = 50 
        threadId = 1
        while (page - 1) * limit < stocksResult.shape[0] :
            thread = myThread(threadId, stocksResult[(int(page) - 1) * int(limit): (int(page) * int(limit))], tradeDay['calendar_date'], tradeDay['calendar_date'])
            thread.start()
            threads.append(thread)
            page += 1
            threadId += 1

# 等待所有线程完成
for t in threads:
    t.join()

result.to_csv("~/Desktop/trade2.csv", encoding="gbk", index=False)
print ("退出主线程")