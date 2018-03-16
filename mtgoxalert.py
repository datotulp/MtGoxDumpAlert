import requests
import twitter
import re
import csv
import json
import time
import datetime
import sqlite3
import threading
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
test = False
RICHLIST_START = 1                                          #For the richlist
RICHLIST_END = 11                                           #TOP1000 = page 10
DATABASE_FILE = 'address_db_mtgox.db'                       #DB file. Will not be created.
MAX_UPDATE = 1000                                           #How many balances to update max.
UPDATE_INTERVAL = 60*5                                      #How often to run the check. Interval in seconds.
POST_HOUR = 18                                              #When to post the daily update
POST_MINUTE_MIN = 1                                         #When to post the daily update          
POST_MINUTE_MAX = POST_MINUTE_MIN + (UPDATE_INTERVAL/60)    #Do not change this
TWITTER_CONSUMER_KEY = config['TWITTER']['consumer_key']
TWITTER_CONSUMER_SECRET = config['TWITTER']['consumer_secret']
TWITTER_ACCESS_TOKEN_KEY = config['TWITTER']['access_token_key']
TWITTER_ACCESS_TOKEN_SECRET = config['TWITTER']['access_token_secret']

if test == True:
    RICHLIST_START = 1 
    RICHLIST_END = 11 #TOP 1000
    DATABASE_FILE = 'address_db.db'
    MAX_UPDATE = 1001

def get_balance ( account ):
    main_api = 'https://blockchain.info/rawaddr/'
    url = main_api + account #https://blockchain.info/address/1PZ8zEQvSeV9ytmLheWZS2nQuqT6hLqpwL
    json_data = requests.get(url).json()
    #print(json_data)
    return json_data['final_balance'] / ( 1000 * 1000 * 100 )

def richlist_get_balances ( richlist ):
    i = 0
    for address in richlist:
        balance =  get_balance(address[0])
        balance_old = address[1]
        db_update(address[0], balance, balance_old)
        i = i+1
        if i > MAX_UPDATE:
            print(i, "balances updated.")
            break
    print(i, "balances updated.")

def twitter_init():
    api = twitter.Api(consumer_key=TWITTER_CONSUMER_KEY,
                      consumer_secret=TWITTER_CONSUMER_SECRET,
                      access_token_key=TWITTER_ACCESS_TOKEN_KEY,
                      access_token_secret=TWITTER_ACCESS_TOKEN_SECRET)

    return api

def twitter_post_dump ( address, amount ):
    twitteracc = twitter_init()
    post = u'\U000026A0' + "ALERT: Mt. Gox just moved " + str(amount) + "BTC!" + u'\U000026A0' + "\n" + str(address)
    twitteracc.PostUpdate(post)
    print(post)

def twitter_post_sum( ):
    twitteracc = twitter_init()
    sum = int(richlist_get_sum())
    already_dumped = 197946 - sum 
    post = u'\U0001F4B0' + 'Mt.Gox still has ' + str(sum) + ' BTC left to sell! Already dumped: ' + str(already_dumped) + ' BTC!' + u'\U0001F4B0' + "\n" + get_output_text_for_hours_since_dump()
    twitteracc.PostUpdate(post)
    print(post)

def twitter_post_whatever ( whatever ):
    twitteracc = twitter_init ()
    twitteracc.PostUpdate(whatever)
    print(whatever)

def richlist_parse_from_website():
    richlist = []
    for x in range(RICHLIST_START, RICHLIST_END):
        link = "https://bitinfocharts.com/de/top-100-richest-bitcoin-addresses-" + str(x) + ".html"
        f = requests.get(link)
        p = re.compile('[13][a-km-zA-HJ-NP-Z1-9]{25,34}')
        res = set(p.findall(f.text))
        richlist.extend(res)
    return set(richlist)

# def richlist_write_to_file(rich_list,filename):
#     res = rich_list
#     csvfile = filename
#     with open(csvfile, "w") as output:
#         writer = csv.writer(output, lineterminator='\n')
#         for val in res:
#             writer.writerow([val])    

def db_entry(address):
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread = False)
    c = conn.cursor()
    timestamp = time.time()
    timestamp_v = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    c.execute("INSERT INTO btcaddresses (btc_address, insert_time, insert_time_v) VALUES(?, ?, ?)", (address, timestamp, timestamp_v))
    conn.commit()

def db_sum_entry():
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread = False)
    c = conn.cursor()
    timestamp = time.time()
    sum = int(richlist_get_sum())
    timestamp_v = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    c.execute("INSERT INTO dumphistory (sum, timestamp, timestamp_v, misc) VALUES(?, ?, ?, ?)", (sum, timestamp, timestamp_v, "lul"))
    conn.commit()

def db_sum_get_latest():
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread = False)
    c = conn.cursor()
    c.execute('SELECT max(id), sum, timestamp FROM dumphistory')
    data = c.fetchone()
    conn.commit()

    if data[1] is None:
        return None
    else:
        return data

def db_update(address, balance, balance_old):
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread = False)
    c = conn.cursor()
    c.execute('SELECT * FROM btcaddresses')
    timestamp = time.time()
    timestamp_v = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    timestamp_v = datetime.datetime.fromtimestamp(timestamp).min
    check_indicator = 0
    if balance_old is not None:
        check_indicator = int(balance)-int(balance_old)
    c.execute('UPDATE btcaddresses SET btc_balance=?,check_time=?,check_time_v=?,btc_balance_old=?,check_indicator=? WHERE btc_address = ?'
    ,(balance, timestamp, timestamp_v, balance_old, check_indicator, address) )
    conn.commit()

def db_clear():
    c.execute("DELETE FROM btcaddresses")
    conn.commit()
    print("!!DB Cleared!! - ", DATABASE_FILE)

def db_read():
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread = False)
    c = conn.cursor()
    c.execute('SELECT * FROM btcaddresses')
    conn.commit()
    return c.fetchall()

def db_sum_get_hours_since_latest_dump():
    sumrow = db_sum_get_latest()
    if sumrow[0] > 1:
        timestamp = time.time()
        diff = timestamp - sumrow[2]
        return int(round(diff/60/60,0))
    else: 
        return None

def get_output_text_for_hours_since_dump():
    hours = db_sum_get_hours_since_latest_dump()
    if hours is not None and hours < 337: #2 weeks
        text = "The last dump occured " + str(int(hours/24)) + " days and " + str(int(hours - (int(hours/24)*24))) + " hours ago."
    else: 
        text = "No recent coin movements (14 days)."
    return text

def richlist_get_sum():
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread = False)
    c = conn.cursor()
    c.execute('SELECT SUM(BTC_BALANCE) FROM btcaddresses')
    conn.commit()
    data = c.fetchone()
    return data[0]

def richlist_write_to_db():
    db_clear()
    richlist = richlist_parse_from_website() #get from website
    i = 1
    for address in richlist:
        print(i, ":", "Inserting: ", address)
        db_entry(address)
        i = i + 1

conn = sqlite3.connect(DATABASE_FILE)
c = conn.cursor()
# #
# #richlist_write_to_db() #get new richlist from website, resets db
def letshitrun():
    change = False
    richlist = db_read()
    richlist_get_balances ( richlist )
    richlist = db_read()
    for row in richlist:
        if int(row[1]) - int(row[2]) < 0:
                print("https://blockchain.info/address/", row[0], int(row[1]) - int(row[2]))
                twitter_post_dump(row[0], int(row[1]) - int(row[2]) )
                change = True
    
    if change == True: #Dump happened, update sum db
        db_sum_entry()
    else: 
        print("no dump happened")

    timestamp = time.time()
    hour = datetime.datetime.fromtimestamp(timestamp).hour
    minute = datetime.datetime.fromtimestamp(timestamp).minute
    if ( hour == POST_HOUR and minute >= POST_MINUTE_MIN and minute < POST_MINUTE_MAX ):
        twitter_post_sum()
    else: 
        print("no sum update (wrong time)")

def runrunrun():
  conn = sqlite3.connect(DATABASE_FILE, check_same_thread = False)
  c = conn.cursor()
  threading.Timer(UPDATE_INTERVAL, runrunrun).start()
  timestamp = time.time()
  timestamp_v = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
  print (timestamp_v, "running...")
  letshitrun()
  c.close()
  conn.close()
  print ("next run in " + UPDATE_INTERVAL + "seconds.")

# already_dumped = 197946 - sum 
# text = u'\U0001F4B0' + 'Mt.Gox still has ' + str(sum) + ' BTC left to sell! Already dumped: ' + str(already_dumped) + 'BTC! No recent coin movements!' + u'\U0001F4B0'
# print(text)
#twitter_post_whatever(text)
#twitter_post_dump("https://blockchain.info/address/1PZ8zEQvSeV9ytmLheWZS2nQuqT6hLqpwLf", 200)
#twitter_post_sum( )
runrunrun()