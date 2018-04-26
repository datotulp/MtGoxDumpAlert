import requests
import twitter
import re
import json
import time
import datetime
import sqlite3
import configparser
from threading import Thread

#TODO:
#DONE: batch sql updates into one
#batch sql inserts
#DONE: exception twitter doublepost 
        # Traceback (most recent call last):
        #   File "btcalert.py", line 345, in <module>
        #     runrunrun()
        #   File "btcalert.py", line 332, in runrunrun
        #     goxdumpmonitor()
        #   File "btcalert.py", line 317, in goxdumpmonitor
        #     lo_twitter.gox_post_sum()
        #   File "btcalert.py", line 148, in gox_post_sum
        #     self.api.PostUpdate(post)
        #   File "C:\Users\T1\AppData\Local\Programs\Python\Python36\lib\site-packages\python_twitter-3.4-py3.6.egg\twitter\api.py", line 1172, in PostUpdate
        #   File "C:\Users\T1\AppData\Local\Programs\Python\Python36\lib\site-packages\python_twitter-3.4-py3.6.egg\twitter\api.py", line 4905, in _ParseAndCheckTwitter
        #   File "C:\Users\T1\AppData\Local\Programs\Python\Python36\lib\site-packages\python_twitter-3.4-py3.6.egg\twitter\api.py", line 4925, in _CheckForTwitterError
        # twitter.error.TwitterError: [{'code': 187, 'message': 'Status is a duplicate.'}]

#Global settings
MODE_RICHLIST = True #enables the top1000 tracking
MODE_GOXALERTER = True #enables mtgox tracking / tracking of certain balances
MODE_RUNINLOOP = 0 #lets the program run in loop X times. 0 = infinite
config = configparser.ConfigParser()
config.read('config.ini')
BITLY_ACCESS_TOKEN = config['BITLY']['access_token']

#Richlist settings
RICHLIST_START = 1                                              #For the richlist
RICHLIST_END = 11                                               #TOP1000 = page 10
RICHLIST_DB_FILE = 'address_db.db'                              #DB file. Will not be created.
MAX_UPDATE = 2000                                               #How many balances to update max.
UPDATE_INTERVAL = 60*45                                         #How often to run the check. Interval in seconds.
UPDATE_MINUTE_MIN = 256                                         #When to post the daily update          
UPDATE_MINUTE_MAX = UPDATE_MINUTE_MIN + (UPDATE_INTERVAL/60)    #Do not change this
DUMP_TRESHHOLD = 10000

#MtGox settings
GOX_DB_FILE = 'address_db_mtgox.db'                                         #DB file. Will not be created.
GOX_POST_MINUTE_MIN = UPDATE_MINUTE_MIN                                     #When to post the daily update          
GOX_POST_MINUTE_MAX = GOX_POST_MINUTE_MIN + (UPDATE_INTERVAL/60)            #Do not change this

class helpers:
    @staticmethod
    def shorten_url (url):
        if BITLY_ACCESS_TOKEN == None:
            return url
        else:
            main_api = 'https://api-ssl.bitly.com/v3/shorten?'
            requrl = main_api + 'access_token=' + BITLY_ACCESS_TOKEN + '&longUrl=' + url
            json_data = requests.get(requrl).json()
            return json_data['data']['url']

class btc:
    @staticmethod
    def get_balance ( account ):
        main_api = 'https://blockchain.info/rawaddr/'
        url = main_api + account #https://blockchain.info/address/1PZ8zEQvSeV9ytmLheWZS2nQuqT6hLqpwL
        json_data = requests.get(url).json()
        #print(json_data)
        return json_data['final_balance'] / ( 1000 * 1000 * 100 )

    @staticmethod 
    def get_balance_multi ( dbfile ): #updates whole db
        main_api = 'https://blockchain.info/balance'
        addrlist = ''
        i = 0

        richlist = db(dbfile).read()

        for address in richlist:
            addrlist = addrlist + '|' + str(address[0])

        try:
            data = requests.post(main_api, data = {'active':addrlist})
            data.raise_for_status()
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            print(e)
            print("Request error. Tryin' again in 10 seconds..")
            time.sleep(10)
            try:
                data = requests.post(main_api, data = {'active':addrlist})
                data.raise_for_status()
            except requests.exceptions.RequestException as e:
                print("Request failed again. Quit this run.")
                return False	
        try:
            json_data = data.json()
        except ValueError: #JSONDecodeError 
            print(data)  
            print('!!!JSON ERROR!!! (this should never happen..)')
            return False

        new_richlist = []
        for address in richlist:
            i = i+1
            balance_old = address[1]
            balance = json_data[address[0]]['final_balance'] / ( 1000 * 1000 * 100 )
            new_item = (address[0], balance, balance_old)
            new_richlist.append(new_item)       
        db().update_many(new_richlist)
        print(i, "balances updated with multi-post method.")
        return True

    @staticmethod
    def richlist_get_balances ( richlist ):
        i = 0
        for address in richlist:
            balance =  btc.get_balance(address[0])
            balance_old = address[1]
            db().update(address[0], balance, balance_old)
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
    CONSUMER_KEY = config['TWITTER']['consumer_key']
    CONSUMER_SECRET = config['TWITTER']['consumer_secret']
    ACCESS_TOKEN_KEY = config['TWITTER']['access_token_key']
    ACCESS_TOKEN_SECRET = config['TWITTER']['access_token_secret']
    TEST_CONSUMER_KEY = config['T_TWITTER']['consumer_key']
    TEST_CONSUMER_SECRET = config['T_TWITTER']['consumer_secret']
    TEST_ACCESS_TOKEN_KEY = config['T_TWITTER']['access_token_key']
    TEST_ACCESS_TOKEN_SECRET = config['T_TWITTER']['access_token_secret']
    api = None
    
    def __init__(self, test = None):
        if test == True:
            self.api = twitter.Api(consumer_key=self.TEST_CONSUMER_KEY,
                        consumer_secret=self.TEST_CONSUMER_SECRET,
                        access_token_key=self.TEST_ACCESS_TOKEN_KEY,
                        access_token_secret=self.TEST_ACCESS_TOKEN_SECRET)
        else:
            self.api = twitter.Api(consumer_key=self.CONSUMER_KEY,
                        consumer_secret=self.CONSUMER_SECRET,
                        access_token_key=self.ACCESS_TOKEN_KEY,
                        access_token_secret=self.ACCESS_TOKEN_SECRET)

    def gox_post_dump ( self, address, amount ):
        post = u'\U000026A0' + "ALERT: Mt. Gox just moved " + str(amount) + "BTC!" + u'\U000026A0' + "\n" + str(address)
        self.post_execute(post)

    def gox_post_sum( self ):
        sum = int(db(GOX_DB_FILE).richlist_get_sum())
        already_dumped = 197946 - sum 
        post = u'\U0001F4B0' + 'Mt.Gox still has ' + str(sum) + ' BTC left to sell! Already dumped: ' + str(already_dumped) + ' BTC!' + u'\U0001F4B0' + "\n" + twt.get_output_text_for_hours_since_dump()
        self.post_execute(post)

    def post_move ( self, address, amount ):
        url = 'https://blockchain.info/address/' + str(address)
        shorturl = helpers.shorten_url(url)
        post = u'\U000026A0' + "ALERT: " + shorturl + " just moved " + str(amount) + " BTC!" + u'\U000026A0'
        self.post_execute(post)

    def post_whatever ( self, whatever ):
        self.post_execute(whatever)

    def post_execute ( self, text ):
        try:
            self.api.PostUpdate(text)
            print(text)
        except twitter.error.TwitterError as e:
            print(e.message)

    @staticmethod
    def get_output_text_for_hours_since_dump():
        hours = db(GOX_DB_FILE).sum_get_hours_since_latest_dump()
        if hours is not None and hours < 337: #2 weeks
            text = "The last dump occured " + str(int(hours/24)) + " days and " + str(int(hours - (int(hours/24)*24))) + " hours ago."
        else: 
            text = "No recent coin movements (14 days)."
        return text

class db:

    def __init__(self, db_file = RICHLIST_DB_FILE):
        self.db_file = db_file
        self.conn = sqlite3.connect(self.db_file)
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

    def update_many(self, new_richlist):
        timestamp = time.time()
        timestamp_v = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        check_indicator = 0
        insert_array = []

        for account in new_richlist:
            address = account[0]
            balance = account[1]
            balance_old = account[2]
            if balance_old is not None:
                check_indicator = int(balance)-int(balance_old)
            new_item  = (balance, timestamp, timestamp_v, balance_old, check_indicator, address)
            insert_array.append(new_item)
        self.c.executemany('UPDATE btcaddresses SET btc_balance=?,check_time=?,check_time_v=?,btc_balance_old=?,check_indicator=? WHERE btc_address = ?', insert_array)
        self.conn.commit()
        print("test")

    def update(self, address, balance, balance_old):
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
        print("!!DB Cleared!! - ", self.db_file)

    def clean_low_balances(self):
        self.c.execute("DELETE FROM btcaddresses WHERE btc_balance_old < 500")
        self.conn.commit()
        print("Low balances cleared from DB.")

    def read(self):
        #self.conn = sqlite3.connect(self.db_file, check_same_thread = False)
        #self.c = self.conn.cursor()
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
            i = i + 1
        print(i, ":", "inserted: ")

    def richlist_get_sum(self):
        self.c.execute('SELECT SUM(BTC_BALANCE) FROM btcaddresses')
        self.conn.commit()
        data = self.c.fetchone()
        return data[0]

def top1000monitor():
    print ("top1000monitor()")
    min = 0
    max = 0
    diff = 0
    lo_db = db()
    timestamp = time.time()
    minute = datetime.datetime.fromtimestamp(timestamp).minute + (datetime.datetime.fromtimestamp(timestamp).hour * 60)

    if btc.get_balance_multi ( RICHLIST_DB_FILE ) == False:
        print("Error while updating balances.")
        return

    richlist = lo_db.read()
    for row in richlist:
        if (row[1] is None) or (row[2] is None):
            break
            
        diff = int(row[1]) - int(row[2])
        if min > diff:
            min = diff

        if max < diff:
            max = diff
        
        if diff < (-1 * DUMP_TRESHHOLD) or diff > DUMP_TRESHHOLD:
            print("https://blockchain.info/address/", row[0], "\tChange: ", int(row[1]) - int(row[2]))
            lo_twt = twt()
            lo_twt.post_move(row[0], int(row[1]) - int(row[2])) # tweet out the big move!

    print("biggest +: ", str(max), "\tdump -: ", str(min))

    if ( minute >= UPDATE_MINUTE_MIN and minute < UPDATE_MINUTE_MAX ): # UPDATE the richlist-time (once a day)
        print("Update Time!!")
        lo_db.richlist_write_to_db()
        lo_db.clean_low_balances()

def goxdumpmonitor():
    print ("Gox Dump Monitor()")
    change = False
    lo_db = db(GOX_DB_FILE)
    lo_twitter = twt()
    timestamp = time.time()
    minute = datetime.datetime.fromtimestamp(timestamp).minute + (datetime.datetime.fromtimestamp(timestamp).hour * 60)

    if btc.get_balance_multi(GOX_DB_FILE) == False:
        print("Error while updating balances(GOX).")
        return    

    richlist = lo_db.read()
    for row in richlist:
        if int(row[1]) - int(row[2]) < 0:
                print("https://blockchain.info/address/", row[0], int(row[1]) - int(row[2]))
                lo_twitter.gox_post_dump(row[0], int(row[1]) - int(row[2]) )
                change = True
    
    if change == True: #Dump happened, update sum db
        lo_db.sum_entry()
    else: 
        print("no dump happened")

    if ( minute >= GOX_POST_MINUTE_MIN and minute < GOX_POST_MINUTE_MAX ):
        lo_twitter.gox_post_sum()
    else: 
        print("no sum update (wrong time for posting)")

def runrunrun():
  starttime=time.time()
  runcount = 0
  while (runcount < MODE_RUNINLOOP) or (MODE_RUNINLOOP == 0):
    runcount = runcount + 1
    timestamp = time.time()
    timestamp_v = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    print (timestamp_v, " - runcount: ", str(runcount), "/", MODE_RUNINLOOP)
    #########################################################################################
    top1000monitor()
    time.sleep(5)
    goxdumpmonitor()
    #########################################################################################
    timestamp = time.time()
    print ("Run Interval:" + str(UPDATE_INTERVAL) + " seconds. Remaining: " + str((UPDATE_INTERVAL - ((time.time() - starttime) % UPDATE_INTERVAL))) )
    time.sleep(UPDATE_INTERVAL - ((time.time() - starttime) % UPDATE_INTERVAL))

# MODE_RICHLIST = True
# MODE_GOXALERTER = True
# MODE_RUNINLOOP = 10
# UPDATE_INTERVAL = 60*5

runrunrun()
