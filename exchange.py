import requests
from datetime import datetime
from datetime import timezone
from datetime import timedelta  

def binance_request(start_date, end_date, tag):
    start_bin = int(str(int(datetime(start_date.year, start_date.month, start_date.day, 0, 0).replace(tzinfo=timezone.utc).timestamp()))+'000')
    end_bin = int(str(int(datetime(end_date.year, end_date.month, end_date.day, 0, 0).replace(tzinfo=timezone.utc).timestamp()))+'000')
    request = f'https://api.binance.com/api/v3/klines?symbol='+tag+f'USDT'+f'&interval=1d&startTime={start_bin}&endTime={end_bin}'
    tagUSDTexchange = requests.get(request).json()
    
    success = True
    try:
        if tagUSDTexchange.get('msg') == 'Invalid symbol.':
            success = False
            return success, [], []
    except:
        pass
    
    keys = [] # date + tagUSDT
    values = [] # rate
    current_date = start_date
    for item in tagUSDTexchange:
        
        keys.append(tag + 'USDT' + ' ' + str(current_date))
        values.append(item[4])
        
        current_date = current_date + timedelta(days=1) 
        
        if current_date > end_date:
            break
               
    return success, keys, values

def latoken_request(start_date, end_date, tag):
    start_lat = int(str(int(datetime(start_date.year, start_date.month, start_date.day, 0, 0).replace(tzinfo=timezone.utc).timestamp())))
    end_lat = int(str(int(datetime(end_date.year, end_date.month, end_date.day, 0, 0).replace(tzinfo=timezone.utc).timestamp())))
    request = f'https://api.latoken.com/v2/tradingview/history?symbol='+tag+f'%2FUSDT&resolution=1d&from={start_lat}&to={end_lat}'
    tagUSDTexchange = requests.get(request).json()

    success = True
    # correct it later
    try:
        if False:#tagUSDTexchange.get('msg') == 'Invalid symbol.':
            success = False
            return success, [], []
    except:
        pass
    
    keys = [] # date + tagUSDT
    values = [] # rate
    current_date = start_date
    for item in tagUSDTexchange.get('c'):
        
        keys.append(tag + 'USDT' + ' ' + str(current_date))
        values.append(item)
        
        current_date = current_date + timedelta(days=1) 
        
        if current_date > end_date:
            break
            
    return success, keys, values

def check_dates(start_date, end_date, data):
    dates_delta = end_date - start_date
    rest_days = dates_delta.days - len(data)
    new_start_date = start_date + timedelta(days=len(data)) 
    return rest_days, new_start_date

# work for days
def convert_to_USDT(data, columns):
    
    start_date = data['date'].iloc[0]
    end_date = data['date'].iloc[-1]
    
    quote_tags = data['quote_tag'].unique()

    # dict creation
    dict_keys = [] # date + tagUSDT
    dict_values = [] # rate
    for tag in quote_tags:
            
        if (tag == 'USDT'):
            continue
        
        first_date = start_date
        while True:
            success, keys, values = binance_request(first_date, end_date, tag)
            if success == False:
                success, keys, values = latoken_request(first_date, end_date, tag)
            
            dict_keys = dict_keys + keys
            dict_values = dict_values + values
            
            # cheack whather we have got data for all days
            rest_days, new_start_date = check_dates(first_date, end_date, keys)
            if rest_days > 0:
                first_date = new_start_date
                continue
            elif rest_days <= 0:
                break
        
    exchange_dict = dict(zip(dict_keys, dict_values))
    
    def convert(info):
        volume = info[0]
        date = info[1]
        tag = info[2]
        
        if tag == 'USDT':
            return volume
        else:
            try:
                rate = float(exchange_dict.get(tag + 'USDT' + ' ' + str(date)))
                return volume * rate
            except:
                print('OOO')
                pass
            #rate = float(exchange_dict.get(tag + 'USDT' + ' ' + str(date)))
            #return volume * rate
            
    for column in columns:
        data[column+'_USDT'] = data[[column, 'date', 'quote_tag']].apply(convert, axis=1)
    
    return data

def get_rates(quote_tags, start_date, end_date):
    
    # dict creation
    dict_keys = [] # date + tagUSDT
    dict_values = [] # rate
    for tag in quote_tags:
            
        if (tag == 'USDT'):
            continue
        
        first_date = start_date
        while True:
            success, keys, values = binance_request(first_date, end_date, tag)
            if success == False:
                success, keys, values = latoken_request(first_date, end_date, tag)
            
            dict_keys = dict_keys + keys
            dict_values = dict_values + values
            
            # cheack whather we have got data for all days
            rest_days, new_start_date = check_dates(first_date, end_date, keys)
            if rest_days > 0:
                first_date = new_start_date
                continue
            elif rest_days <= 0:
                break
        
    exchange_dict = dict(zip(dict_keys, dict_values))
    
    return exchange_dict

def quick_convert(exchange_dict, data, columns):
    def convert(info):
        volume = info[0]
        date = info[1]
        tag = info[2]
        
        if tag == 'USDT':
            return volume
        else:
            try:
                rate = float(exchange_dict.get(tag + 'USDT' + ' ' + str(date)))
                return volume * rate
            except:
                print('OOO')
                pass
            
    for column in columns:
        data[column] = data[[column, 'date', 'quote_tag']].apply(convert, axis=1)
        
    return data
    