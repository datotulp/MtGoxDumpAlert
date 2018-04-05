import requests
import twitter
import re
import gc
import json
import time
import datetime
import sqlite3
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
RICHLIST_START = 1                                              #For the richlist
RICHLIST_END = 11                                               #TOP1000 = page 10
DATABASE_FILE = 'address_db.db'                                 #DB file. Will not be created.
MAX_UPDATE = 2000                                               #How many balances to update max.
UPDATE_INTERVAL = 60*60*2                                       #How often to run the check. Interval in seconds.
UPDATE_MINUTE_MIN = 1                                           #When to post the daily update          
UPDATE_MINUTE_MAX = UPDATE_MINUTE_MIN + (UPDATE_INTERVAL/60)    #Do not change this
TWITTER_CONSUMER_KEY = config['TWITTER']['consumer_key']
TWITTER_CONSUMER_SECRET = config['TWITTER']['consumer_secret']
TWITTER_ACCESS_TOKEN_KEY = config['TWITTER']['access_token_key']
TWITTER_ACCESS_TOKEN_SECRET = config['TWITTER']['access_token_secret']

class btc:
    @staticmethod
    def get_balance ( account ):
        main_api = 'https://blockchain.info/rawaddr/'
        url = main_api + account #https://blockchain.info/address/1PZ8zEQvSeV9ytmLheWZS2nQuqT6hLqpwL
        json_data = requests.get(url).json()
        #print(json_data)
        return json_data['final_balance'] / ( 1000 * 1000 * 100 )

    @staticmethod
    def richlist_get_balances ( richlist ):
        i = 0
        lo_db = db()
        for address in richlist:
            balance =  btc.get_balance(address[0])
            balance_old = address[1]
            lo_db.update(address[0], balance, balance_old)
            i = i+1
            if i > MAX_UPDATE:
                print(i, "balances updated, max update param reached.")
                break
        print(i, "balances updated.")

    @staticmethod
    def richlist_parse_from_website():
        richlist = []
        for x in range(RICHLIST_START, RICHLIST_END):
            link = "https://bitinfocharts.com/de/top-100-richest-bitcoin-addresses-" + str(x) + ".html"
            f = requests.get(link)
            p = re.compile('[13][a-km-zA-HJ-NP-Z1-9]{25,34}')
            res = set(p.findall(f.text))
            richlist.extend(res)
        return set(richlist)

class twt:
    api = None
    
    def __init__(self):
        self.api = twitter.Api(consumer_key=TWITTER_CONSUMER_KEY,
                        consumer_secret=TWITTER_CONSUMER_SECRET,
                        access_token_key=TWITTER_ACCESS_TOKEN_KEY,
                        access_token_secret=TWITTER_ACCESS_TOKEN_SECRET)

    def post_dump ( self, address, amount ):
        post = u'\U000026A0' + "ALERT: Mt. Gox just moved " + str(amount) + "BTC!" + u'\U000026A0' + "\n" + str(address)
        self.api.PostUpdate(post)
        print(post)

    def post_whatever ( self, whatever ):
        self.api.PostUpdate(whatever)
        print(whatever)

    @staticmethod
    def get_output_text_for_hours_since_dump():
        lo_db = db()
        hours = lo_db.sum_get_hours_since_latest_dump()
        if hours is not None and hours < 337: #2 weeks
            text = "The last dump occured " + str(int(hours/24)) + " days and " + str(int(hours - (int(hours/24)*24))) + " hours ago."
        else: 
            text = "No recent coin movements (14 days)."
        return text

class db:

    def __init__(self):
        self.conn = sqlite3.connect(DATABASE_FILE)
        self.c = self.conn.cursor()

    def entry(self, address):
        timestamp = time.time()
        timestamp_v = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        self.c.execute("INSERT OR IGNORE INTO btcaddresses (btc_address, insert_time, insert_time_v) VALUES(?, ?, ?)", (address, timestamp, timestamp_v))
        self.conn.commit()

    def sum_entry(self):
        timestamp = time.time()
        sum = int(self.richlist_get_sum())
        timestamp_v = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        self.c.execute("INSERT INTO dumphistory (sum, timestamp, timestamp_v, misc) VALUES(?, ?, ?, ?)", (sum, timestamp, timestamp_v, "lul"))
        self.conn.commit()

    def sum_get_latest(self):
        self.c.execute('SELECT max(id), sum, timestamp FROM dumphistory')
        data = self.c.fetchone()
        self.conn.commit()

        if data[1] is None:
            return None
        else:
            return data

    def update(self, address, balance, balance_old):
        self.conn = sqlite3.connect(DATABASE_FILE, check_same_thread = False)
        self.c = self.conn.cursor()
        self.c.execute('SELECT * FROM btcaddresses')
        timestamp = time.time()
        timestamp_v = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        check_indicator = 0
        if balance_old is not None:
            check_indicator = int(balance)-int(balance_old)
        self.c.execute('UPDATE btcaddresses SET btc_balance=?,check_time=?,check_time_v=?,btc_balance_old=?,check_indicator=? WHERE btc_address = ?'
        ,(balance, timestamp, timestamp_v, balance_old, check_indicator, address) )
        self.conn.commit()

    def clear(self):
        self.c.execute("DELETE FROM btcaddresses")
        self.conn.commit()
        print("!!DB Cleared!! - ", DATABASE_FILE)

    def clean_low_balances(self):
        self.c.execute("DELETE FROM btcaddresses WHERE btc_balance_old < 1")
        self.conn.commit()
        print("Low balances cleared from DB.")

    def read(self):
        self.conn = sqlite3.connect(DATABASE_FILE, check_same_thread = False)
        self.c = self.conn.cursor()
        self.c.execute('SELECT * FROM btcaddresses')
        self.conn.commit()
        return self.c.fetchall()

    def sum_get_hours_since_latest_dump(self):
        sumrow = self.sum_get_latest()
        if sumrow[0] > 1:
            timestamp = time.time()
            diff = timestamp - sumrow[2]
            return int(round(diff/60/60,0))
        else: 
            return None

    def richlist_write_to_db(self):
        #self.clear()
        richlist = btc.richlist_parse_from_website() #get from website
        i = 1
        for address in richlist:
            self.entry(address)
            print(i, ":", "Inserting: ", address)
            i = i + 1

    def richlist_get_sum(self):
        self.c.execute('SELECT SUM(BTC_BALANCE) FROM btcaddresses')
        self.conn.commit()
        data = self.c.fetchone()
        return data[0]

def letshitrun():
    print ("letshitrun()")
    min = 0
    max = 0
    diff = 0
    lo_db = db()
    timestamp = time.time()
    minute = datetime.datetime.fromtimestamp(timestamp).minute + (datetime.datetime.fromtimestamp(timestamp).hour * 60)

    richlist = lo_db.read()
    btc.richlist_get_balances ( richlist )
    richlist = lo_db.read()

    for row in richlist:
        diff = int(row[1]) - int(row[2])
        if min > diff:
            min = diff

        if max < diff:
            max = diff
        
        if diff < -5000 or diff > 5000:
            print("https://blockchain.info/address/", row[0], "\tChange: ", int(row[1]) - int(row[2]), sep='')

    print("biggest +: ", str(max), "\tdump -: ", str(min))

    if ( minute >= UPDATE_MINUTE_MIN and minute < UPDATE_MINUTE_MAX ): # UPDATE the richlist-time (once a day)
        print("Update Time!!")
        lo_db.richlist_write_to_db()
        lo_db.clean_low_balances()

def runrunrun():
  starttime=time.time()
  runcount = 0
  while True:
    runcount = runcount + 1
    timestamp = time.time()
    timestamp_v = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    print (timestamp_v, " - runcount: ", str(runcount))
    letshitrun()
    timestamp = time.time()
    print ("Run Interval:" + str(UPDATE_INTERVAL) + " seconds. Remaining: " + str((UPDATE_INTERVAL - ((time.time() - starttime) % UPDATE_INTERVAL))) )
    time.sleep(UPDATE_INTERVAL - ((time.time() - starttime) % UPDATE_INTERVAL))
    gc.collect()

runrunrun()
