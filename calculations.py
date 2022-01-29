import psycopg2
import pandas.io.sql as sqlio
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date, timedelta
import datetime
import exchange 
import time

print(datetime.datetime.now())

# fee rates
Spot_Trading_Fees = pd.read_excel('Spot Trading Fees.xlsx')
Spot_Trading_Fees = Spot_Trading_Fees.replace('inf', np.Infinity)

def get_fee_rate(volume, trader_type):
    for row in Spot_Trading_Fees.iterrows():
        if row[1]['min'] <= volume and volume < row[1]['max']:
            if trader_type == 'maker':
                return row[1]['Maker']
            elif trader_type == 'taker':
                return row[1]['Taker']
    
def get_fee_level(volume):
    for row in Spot_Trading_Fees.iterrows():
        if row[1]['min'] <= volume and volume < row[1]['max']:
                return row[1]['Level']
            
#!!! makers
makers = pd.DataFrame()
makers = makers.append(pd.read_excel('makers_03-09_03-23.xlsx'))
makers = makers.append(pd.read_excel('makers_03_23_04_06.xlsx'))
makers = makers.append(pd.read_excel('makers_04_06_04_13.xlsx'))
makers = makers.append(pd.read_excel('makers_04_13_04_25.xlsx'))

#process makers
# convert volumes to USDT
quote_tags = makers['quote_tag'].unique()
start_date = datetime.datetime(2021, 3, 9).date()
end_date = datetime.datetime(2021, 4, 25).date()
rates_dict = exchange.get_rates(quote_tags, start_date, end_date)
makers = exchange.quick_convert(rates_dict, makers, columns=['volume'])
makers.drop(columns=['currency_tag', 'quote_tag'], inplace=True)

# aggregate volumes for each id, date
makers = makers.groupby(['id', 'date']).agg({'volume':'sum'})

# create copy for safity
tmp_makers = makers.copy()
tmp_makers.reset_index(level=0, inplace=True)
tmp_makers.reset_index(level=0, inplace=True)

#convert date from str to date format
tmp_makers['date'] = tmp_makers['date'].apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d'))
tmp_makers['date'] = tmp_makers['date'].apply(lambda x: x.date())

# add missed dates
tmp_makers = tmp_makers.set_index(['date', 'id']).unstack(fill_value=0).asfreq('D', fill_value=0).stack().sort_index(level=1).reset_index()

# calculate cimilative volume for each id
tmp_makers['cum_day_volume'] = tmp_makers.groupby(['id'])['volume'].cumsum(axis=0)

# calculate rolling sum for cumulatiove volume
rol_sum_series = tmp_makers.groupby(['id'])['cum_day_volume'].rolling(30, min_periods=1).sum()
rol_sum_series = rol_sum_series.reset_index(level=[0,1])
tmp_makers['rolling_sum_30_days'] = rol_sum_series['cum_day_volume']
tmp_makers.drop(columns=['cum_day_volume','volume'], inplace=True)

# calculate fees
tmp_makers['fee_rate'] = tmp_makers['rolling_sum_30_days'].apply(lambda x: get_fee_rate(x, 'maker'))
tmp_makers['fee'] = tmp_makers['rolling_sum_30_days'] * tmp_makers['fee_rate']

tmp_makers['level'] = tmp_makers['rolling_sum_30_days'].apply(lambda x: get_fee_level(x))
tmp_makers.to_excel('makers.xlsx')

#!!! takers --
takers = pd.DataFrame()
takers = takers.append(pd.read_excel('takers_03_09_03_23.xlsx'))
takers = takers.append(pd.read_excel('takers_03_23_04_06.xlsx'))
takers = takers.append(pd.read_excel('takers_04_06_04_13.xlsx'))
takers = takers.append(pd.read_excel('takers_04_13_04_25.xlsx'))

#process takers
# convert volumes to USDT
quote_tags = takers['quote_tag'].unique()
start_date = datetime.datetime(2021, 3, 9).date()
end_date = datetime.datetime(2021, 4, 25).date()
#we have already got rates_dict
#rates_dict = exchange.get_rates(quote_tags, start_date, end_date)
takers = exchange.quick_convert(rates_dict, takers, columns=['volume'])
takers.drop(columns=['currency_tag', 'quote_tag'], inplace=True)

# aggregate volumes for each id, date
takers = takers.groupby(['id', 'date']).agg({'volume':'sum'})

# create copy for safity
tmp_takers = takers.copy()
tmp_takers.reset_index(level=0, inplace=True)
tmp_takers.reset_index(level=0, inplace=True)

#convert date from str to date format
tmp_takers['date'] = tmp_takers['date'].apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d'))
tmp_takers['date'] = tmp_takers['date'].apply(lambda x: x.date())

# add missed dates
tmp_takers = tmp_takers.set_index(['date', 'id']).unstack(fill_value=0).asfreq('D', fill_value=0).stack().sort_index(level=1).reset_index()

# calculate cimilative volume for each id
tmp_takers['cum_day_volume'] = tmp_takers.groupby(['id'])['volume'].cumsum(axis=0)

# calculate rolling sum for cumulatiove volume
rol_sum_series = tmp_takers.groupby(['id'])['cum_day_volume'].rolling(30, min_periods=1).sum()
rol_sum_series = rol_sum_series.reset_index(level=[0,1])
tmp_takers['rolling_sum_30_days'] = rol_sum_series['cum_day_volume']
tmp_takers.drop(columns=['cum_day_volume','volume'], inplace=True)

# calculate fees
tmp_takers['fee_rate'] = tmp_takers['rolling_sum_30_days'].apply(lambda x: get_fee_rate(x, 'taker'))
tmp_takers['fee'] = tmp_takers['rolling_sum_30_days'] * tmp_takers['fee_rate']

#print to file for safity
tmp_takers['date'] = tmp_takers['date'].apply(lambda x: x.date())
tmp_takers['level'] = tmp_takers['rolling_sum_30_days'].apply(lambda x: get_fee_level(x))
tmp_takers.to_csv('takers.csv')

#!!! aggregate makers and takers
traders = tmp_makers.append(tmp_takers)
#merge simillar ids (maker=taker -> sum its fees and volumes in each date)
unique_traders = traders.groupby(['id', 'date']).agg({'rolling_sum_30_days':'sum', 'fee':'sum', 'traders_num':'count'})
#unique_traders = traders.drop_duplicates(keep='first')
unique_traders['traders_num'] = 1
aggregated_unique_traders = unique_traders.groupby(['date', 'level']).\
    agg({'rolling_sum_30_days':'sum', \
         'fee':'sum',\
         'traders_num':'count'})
aggregated_unique_traders.to_excel('aggregated_data.xlsx')

#exclude all out of 2021-04-12 to 2021-04-25
result_data = aggregated_unique_traders.copy()
result_data.reset_index(level=0, inplace=True)
result_data.reset_index(level=0, inplace=True)
result_start_date = datetime.datetime(2021, 4, 12).date()
result_end_date = datetime.datetime(2021, 4, 25).date()
result_data = result_data[result_data['date'] >= result_start_date]
result_data = result_data[result_data['date'] <= result_end_date]
result_data.to_excel('result.xlsx')

print(datetime.datetime.now())
