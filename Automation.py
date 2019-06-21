import hmac
import hashlib
import time
import requests as r
import pandas as pd

class Exchange:
    def __init__(self, exchange):
        """
        params: exchange = "bittrex","bitstamp"
        """
        self.exchange = exchange.lower()
        
        possible_entries = ["bittrex","bitstamp"]
        if self.exchange not in possible_entries:
            raise Exception('Exchange "{}" not a valid entry. Possible entries are: {}'.format(self.exchange, possible_entries))
        
        if self.exchange == "bittrex":
            self.api_secret = "99dda87bf43242fda03d6bc4e9d6d265"
            self.api_key= "eab24c013517400d827712e92f849d4d"
        elif self.exchange == "bitstamp":
            self.api_secret = "l0zJfcq0L2QggqIFFnRbizVva2NOj90R"
            self.api_key= "seTkU8rZLvuYgUbi2doti4aC0UvUxdC9"
    
    def create_nonce(self):
        nonce = str(int(time.time() * 1000))
        return nonce
    
    def create_apisign(self, key_nonce_url):
        apisign = hmac.new(self.api_secret.encode(),key_nonce_url.encode(),hashlib.sha512).hexdigest()
        return apisign
        
    def get_balances(self):
        if self.exchange == "bittrex":
            nonce = self.create_nonce()
            get_balances_url = "https://api.bittrex.com/api/v1.1/account/getbalances?apikey={}&nonce={}".format(self.api_key,nonce) 
            apisign = self.create_apisign(get_balances_url)
            balances_json = r.get(get_balances_url,headers={"apisign":apisign}).json()
            if balances_json["success"] == True:
                balances_df = pd.DataFrame.from_dict(balances_json["result"])
                return balances_df[["Currency","Available","Balance","Pending"]]
            else:
                return "Couldn't retreive balances, try again"
    
    def get_balance(self, currency):
        if self.exchange == "bittrex":
            nonce = self.create_nonce()
            get_balance_url = "https://api.bittrex.com/api/v1.1/account/getbalance?apikey={}&currency={}&nonce={}".format(self.api_key,currency, nonce) 
            apisign = self.create_apisign(get_balance_url)
            try:
                balance_json = r.get(get_balance_url, headers={"apisign":apisign}).json()
                return balance_json["result"]["Available"]
            except:
                print('Something went wrong, API response message: "{}"'.format(balance_json["message"]))
    
    def get_price(self, currency, price_type = "buy"):
        if self.exchange == "bittrex":
            price_url = "https://api.bittrex.com/api/v1.1/public/getticker?market=USD-{}".format(currency.upper())
            try:
                price_json = r.get(price_url).json()
                if price_type.lower() == "buy":
                    return price_json["result"]["Ask"]
                elif price_type.lower() == "sell":
                    return price_json["result"]["Bid"]
            except:
                print('Something went wrong, API response message: "{}"'.format(price_json["message"]))
            
    def buy_crypto(self, currency, quantity, rate=1):
        if self.exchange == "bittrex":
            avail_balance = self.get_balance("usd")
            buy_price = self.get_price(currency, price_type="buy")
            if quantity*buy_price > avail_balance:
                raise Exception("Insufficient funds, available balance: {}, buy order price: {}USD".format(avail_balance,quantity*buy_price))
            else:
                nonce = self.create_nonce()
                buy_url = "https://api.bittrex.com/api/v1.1/market/buylimit?apikey={}&market=USD-{}&quantity={}&rate={}&nonce={}".format(self.api_key,currency.upper(),quantity,rate,nonce)
                apisign = self.create_apisign(buy_url)
    #             try:
    #                 buy_json = r.get(buy_url, headers={"apisign":apisign}).json()
    #                 print("Buy executed successfully!" ({})".format(buy_json["result"]["uuid"])))
    #             except:
    #                 print('Something went wrong, API response message: "{}"'.format(buy_json["message"]))  

    def sell_crypto(self, currency, quantity, rate=1):
        if self.exchange == "bittrex":
            avail_balance = self.get_balance(currency)
            if quantity > avail_balance:
                raise Exception("Not enough {} in balance, defecit of {}{}".format(currency,avail_balance-quantity, currency))
            else:
                nonce = self.create_nonce()
                sell_url = "https://api.bittrex.com/api/v1.1/market/selllimit?apikey={}&market=USD-{}&quantity={}&rate={}&nonce={}".format(self.api_key,currency.upper(),quantity,rate,nonce)
                apisign = self.create_apisign(sell_url)

    #             try:
    #                 sell_json = r.get(sell_url, headers={"apisign":apisign}).json()
    #                 print("Sell executed successfully! ({})".format(sell_json["result"]["uuid"]))
    #             except:
    #                 print('Something went wrong, API response message: "{}"'.format(sell_json["message"]))  



