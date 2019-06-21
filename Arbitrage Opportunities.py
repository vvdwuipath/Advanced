import requests as r
import pandas as pd
import datetime as dt
import bs4

class Arbitrage:
    def __init__(self, exchanges = "all"):
        """
        params: exchanges = "all","usd","zar"
        """
        self.exchanges = exchanges
        date_time = dt.datetime.now()
        self.date = "{}.{}.{}".format(date_time.year,date_time.month, date_time.day)
        self.time = "{}:{}".format(date_time.hour, date_time.minute)
        
        if self.exchanges.lower() == "all" or self.exchanges.lower() == "zar":
            print("Executed at {} {}".format(self.date, self.time))
            print("-----------------------------")
            Luno_pairs_url = "https://api.mybitx.com/api/1/tickers"
            luno_pairs_json = r.get(Luno_pairs_url).json()["tickers"]
            luno_pairs_list = [luno_pairs_json[i]["pair"] for i in range(len(luno_pairs_json))]
            luno_list = []
            for i,pair in enumerate(luno_pairs_list):
                luno_url = "https://api.mybitx.com/api/1/orderbook_top?pair={}".format(pair)
                luno_json = r.get(luno_url).json()
                pair = pair.replace("XBT","BTC")
                mdict = {"pair":"{}/{}".format(pair.lower()[:3],pair.lower()[3:]),"buy_price":luno_json["asks"][0]["price"],
                         "buy_amount":round(float(luno_json["asks"][0]["volume"]),4),"sell_price":luno_json["bids"][0]["price"],
                         "sell_amount":round(float(luno_json["bids"][0]["volume"]),4),"exchange":"Luno"}
                luno_list.append(mdict)
            self.luno_df = pd.DataFrame.from_dict(luno_list)
            print("Luno DF completed!")
            #-----------------------------------------------------------------------------
            Ice3x_url = "https://ice3x.com/api/v1/orderbook/ticker"
            ice3x_json = r.get(Ice3x_url).json()["response"]["entities"]
            ice3x_list = []
            for i in range(len(ice3x_json)):
                if (ice3x_json[i]["pair_name"].lower().split("/")[0] != "ngn") or (ice3x_json[i]["pair_name"].lower().split("/")[1] != "ngn"):
                    mdict = {"pair":ice3x_json[i]["pair_name"].lower(),"buy_price":ice3x_json[i]["ask"]["price"],
                             "buy_amount":round(float(ice3x_json[i]["ask"]["amount"]),4),"sell_price":ice3x_json[i]["bid"]["price"],
                             "sell_amount":round(float(ice3x_json[i]["bid"]["amount"]),4),"exchange":"Ice3x"}
                    ice3x_list.append(mdict)

            self.ice3x_df = pd.DataFrame.from_dict(ice3x_list)   
            print("Ice3X DF completed!")
            #------------------------------------------------------------------------------
            Alttrader_url = "https://api.altcointrader.co.za/v3/live-stats"
            alttrader_json = r.get(Alttrader_url).json()
            alttrader_df = pd.DataFrame.from_dict(alttrader_json,orient="index")
            alttrader_df.drop(columns=["High","Low","Price","Volume"],axis=1,inplace=True)
            alttrader_df.rename(columns={"Sell":"buy_price","Buy":"sell_price"}, inplace=True)
            alttrader_df["pair"] = alttrader_df.index
            alttrader_df["pair"] = alttrader_df["pair"].apply(lambda x: "{}/zar".format(x.lower()))
            alttrader_df['exchange'] = ["Altcoin" for i in range(len(alttrader_df))]
            alttrader_df['new_idx'] = [i for i in range(len(alttrader_df))]
            alttrader_df.set_index('new_idx', inplace=True)
            self.alttrader_df = alttrader_df
            print("AltcoinTrader DF completed!")
            #--------------------------------------------------------------------------------
            Coindir_url = "https://api.coindirect.com/api/currency/snapshot"
            coindir_json = r.get(Coindir_url).json()

            coindir_list = []
            for i in range(len(coindir_json)):
                mdict = coindir_json[i]
                cur_code = {"pair":"{}/{}".format(coindir_json[i]['currencyForSale']['code'].lower(),
                                                  coindir_json[i]['currencyAccepted']['code'].lower())}
                mdict.update(cur_code)
                coindir_list.append(mdict)

            coindir_df = pd.DataFrame.from_dict(coindir_list)
            coindir_df['exchange'] = ["Coindirect" for i in range(len(coindir_df))]
            coindir_df.drop(columns=['circulatingSupply','currencyAccepted','currencyForSale','id','percentChange1h','percentChange24h',
                                     'percentChange7d','rank','totalSupply','volumeUsd24h'], axis=1,inplace=True)
            coindir_df.rename(columns={"bestSellOfferPrice":"buy_price","bestBuyOfferPrice":"sell_price"}, inplace=True)
            coindir_df.dropna(inplace=True)
            self.coindir_df = coindir_df
            print("CoinDirect DF completed!")
        
        if self.exchanges.lower() == "all" or self.exchanges.lower() == "usd":
            print("Executed at {} {}".format(self.date, self.time))
            print("-----------------------------")
            usd_zar_url = "https://free.currencyconverterapi.com/api/v6/convert?q=USD_ZAR&compact=ultra&apiKey=fb4f95dd55f13715b400"
            aud_zar_url = "https://free.currencyconverterapi.com/api/v6/convert?q=AUD_ZAR&compact=ultra&apiKey=fb4f95dd55f13715b400"
            try:
                usd_zar = r.get(usd_zar_url).json()["USD_ZAR"]
            except:
                usd_zar = 14.5
                print("FOREX conversion API fail, set usd/zar manually!")

            try:
                aud_zar = r.get(aud_zar_url).json()["AUD_ZAR"]
            except:
                aud_zar = 10.3
                print("FOREX conversion API fail, set aud/zar manually!")
            print("USD & AUD Conversions completed!")
            #--------------------------------------------------------------------------
            Bittrex_url = "https://api.bittrex.com/api/v1.1/public/getmarketsummaries"
            bittrex_json = r.get(Bittrex_url).json()
            bittrex_df = pd.DataFrame.from_dict(bittrex_json["result"])
            bittrex_df['exchange'] = ["Bittrex" for i in range(len(bittrex_df))]
            bittrex_df["pair"] = bittrex_df["MarketName"].apply(lambda x: "{}/{}".format(x.split(sep="-")[0].lower(),
                                                                                         x.split(sep="-")[1].lower()))
            bittrex_df.rename(columns={"Ask":"buy_price","Bid":"sell_price"}, inplace=True)
            bittrex_df["check"] = bittrex_df["pair"].apply(lambda x: 1 if (x.split("/")[0]=="usd" or x.split("/")[1]=="usd") else 0)
            usd_idx = bittrex_df[bittrex_df["check"]==1]["buy_price"].index
            bittrex_df.drop(columns=["BaseVolume","Created","OpenBuyOrders","OpenSellOrders","PrevDay","TimeStamp","Volume","MarketName",
                                     "Last","High","Low","check"], axis=1, inplace=True)
            for idx in usd_idx:
                bittrex_df.loc[idx,"buy_price"] = bittrex_df.loc[idx,"buy_price"]*usd_zar
                bittrex_df.loc[idx,"sell_price"] = bittrex_df.loc[idx,"sell_price"]*usd_zar
                pair = bittrex_df.loc[idx,"pair"].split("/")
                if pair[0] == "usd":
                    pair[0] = "zar"
                elif pair[1] == "usd":
                    pair[1] = "zar"
                bittrex_df.loc[idx,"pair"] = "{}/{}".format(pair[1],pair[0])
            self.bittrex_df = bittrex_df
            print("Bittrex DF completed!")
            #------------------------------------------------------------------------
            #******* sukkel om 2-way auth op te stel **********
            Bitstamp_pairs_url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
            cur_pairs = [r.get(Bitstamp_pairs_url).json()[i]["name"].lower() for i in range(len(r.get(Bitstamp_pairs_url).json()))]
            for pair in cur_pairs:
                if "eur" in pair:
                    cur_pairs.remove(pair)
            bitstamp_list = []
            for pair in cur_pairs:
                Bitstamp_url = "https://www.bitstamp.net/api/v2/ticker/{}".format(pair.split("/")[0]+pair.split("/")[1])  
                mdict = r.get(Bitstamp_url).json()
                if pair.split("/")[0] == "usd":
                    pair = {"pair":"zar/{}".format(pair.split("/")[1])}
                    check = {"check":1}
                elif pair.split("/")[1] == "usd":
                    pair = {"pair":"{}/zar".format(pair.split("/")[0])}
                    check = {"check":1}
                else:
                    pair = {"pair":pair}
                    check = {"check":0}
                mdict.update(pair)
                mdict.update(check)
                bitstamp_list.append(mdict)

            bitstamp_df = pd.DataFrame.from_dict(bitstamp_list)
            bitstamp_df["exchange"] = ["Bitstamp" for i in range(len(bitstamp_df))]
            bitstamp_df.rename(columns={"ask":"buy_price","bid":"sell_price"}, inplace=True)
            zar_idx = bitstamp_df[bitstamp_df["check"]==1]["buy_price"].index
            for idx in zar_idx:
                bitstamp_df.loc[idx,"buy_price"] = float(bitstamp_df.loc[idx,"buy_price"])*usd_zar
                bitstamp_df.loc[idx,"sell_price"] = float(bitstamp_df.loc[idx,"sell_price"])*usd_zar

            bitstamp_df.drop(inplace=True, axis=1, columns=['high', 'last', 'low', 'open', 'timestamp', 'volume',
                  'vwap','check'])
            self.bitstamp_df = bitstamp_df
            print("Bitstamp DF completed!")
            #------------------------------------------------------------------------
            cex_pairs_url = "https://cex.io/api/currency_limits"
            cex_pairs_json = r.get(cex_pairs_url).json()["data"]["pairs"]
            cex_pairs_list = ["{}/{}".format(cex_pairs_json[i]["symbol1"],cex_pairs_json[i]["symbol2"]) for i in range(len(cex_pairs_json))]
            cex_list = []
            for pair in cex_pairs_list:
                if pair.split("/")[1] == "USD":
                    cex_url = "https://cex.io/api/order_book/{}".format(pair)
                    cex_json = r.get(cex_url).json()
                    mdict = {"pair":"{}/zar".format(pair.split("/")[0].lower()),"buy_price":cex_json["asks"][0][0]*usd_zar,
                             "buy_amount":round(float(cex_json["asks"][0][1]),4),"sell_price":cex_json["bids"][0][0]*usd_zar,
                             "sell_amount":round(float(cex_json["bids"][0][1]),4),"exchange":"CEX"}
                    cex_list.append(mdict)
            self.cex_df = pd.DataFrame.from_dict(cex_list)
            print("CEX DF completed!")
            #----------------------------------------------------------------------------------------------------------------
            kraken_pair_url = "https://api.kraken.com/0/public/AssetPairs"
            kraken_pair_json = r.get(kraken_pair_url).json()["result"]
            kraken_pair_list = list(kraken_pair_json.keys())
            kraken_list = []
            for pair in kraken_pair_list:
                if pair[-3:] == "USD":
                    pair1 = kraken_pair_json[pair]['altname'][:-3]
                    kraken_url = "https://api.kraken.com/0/public/Ticker?pair={}USD".format(pair1)
                    kraken_json = r.get(kraken_url).json()
                    mdict = {"pair":"{}/zar".format(pair1.lower()),"buy_price":float(kraken_json["result"][pair]["b"][0])*usd_zar,
                    "buy_amount":round(float(kraken_json["result"][pair]["b"][1]),4),"sell_price":float(kraken_json["result"][pair]["a"][0])*usd_zar,
                    "sell_amount":round(float(kraken_json["result"][pair]["a"][1]),4),"exchange":"Kraken"}
                    kraken_list.append(mdict)
            kraken_df = pd.DataFrame.from_dict(kraken_list)
            self.kraken_df = kraken_df
            print("Kraken DF completed!")
            #----------------------------------------------------------------------------------------------------------------           
            # ****** Geen XXX/ZAR pairs ******
            # Poli_url = "https://poloniex.com/public?command=returnTicker"
            # poli_json = r.get(Poli_url).json()
            # poli_keys = list(poli_json.keys())
            # poli_list=[]
            # for pair in poli_keys:
            #     mdict = poli_json[pair]
            #     mdict.update({"pair":"{}/{}".format(pair.split("_")[0].lower(),pair.split("_")[1].lower())})
            #     poli_list.append(mdict)
            # poli_df = pd.DataFrame.from_dict(poli_list)
            # poli_df['exchange'] = ["Poliniex" for i in range(len(poli_df))]

            # poli_df.rename(columns={"highestBid":"buy_price","lowestAsk":"sell_price"}, inplace=True)
            # poli_df.drop(columns=["baseVolume","high24hr","id","isFrozen","percentChange","quoteVolume","low24hr","last"], 
            #              axis=1, inplace=True)

            # print("Poliniex DF completed!")

            #----------------------------------------------------------------------------------------------------------------------

            #******** kan nie account verify nie kort Aus nommer **********
            # Coinspot_url = "https://www.coinspot.com.au/pubapi/latest"
            # coinspot_json = r.get(Coinspot_url).json()["prices"]
            # coinspot_keys = coinspot_json.keys()
            # coinspot_list = []
            # for pair in coinspot_keys:
            #     mdict = coinspot_json[pair]
            #     pair = {"pair":"{}/zar".format(pair)}
            #     mdict.update(pair)
            #     coinspot_list.append(mdict)

            # coinspot_df = pd.DataFrame.from_dict(coinspot_list)
            # coinspot_df["exchange"] = ["CoinSpot" for i in range(len(coinspot_df))]
            # coinspot_df.rename(columns={"ask":"buy_price","bid":"sell_price"}, inplace=True)
            # coinspot_df.drop(inplace=True,columns=["last"])
            # coinspot_df["buy_price"] = coinspot_df["buy_price"].apply(lambda x: float(x)*aud_zar)
            # coinspot_df["sell_price"] = coinspot_df["sell_price"].apply(lambda x: float(x)*aud_zar)

            # print("CoinSpot DF completed!")

            #----------------------------------------------------------------------------------------------------------------------

            # ******* Kan nie registreer sonder AUS nommer nie ********
            # cur_pairs_url = "https://api.independentreserve.com/Public/GetValidPrimaryCurrencyCodes"
            # cur_pairs_list = r.get(cur_pairs_url).json()
            # mcw_list = []
            # for pair in cur_pairs_list:
            #     pair_url = "https://api.independentreserve.com/Public/GetMarketSummary?primaryCurrencyCode={}&secondaryCurrencyCode=aud".format(pair.lower())
            #     mcw_list.append(r.get(pair_url).json())
            # mcw_df = pd.DataFrame.from_dict(mcw_list)
            # mcw_df["pair"] = mcw_df["PrimaryCurrencyCode"].apply(lambda x: "{}/zar".format(x.lower()))
            # mcw_df["pair"] = mcw_df["pair"].apply(lambda x: "btc/zar" if x.split("/")[0] == "xbt" else x)
            # mcw_df["exchange"] = ["myCryptoWallet" for i in range(len(mcw_df))]
            # mcw_df.drop(inplace=True,axis=1,columns=['CreatedTimestampUtc','DayAvgPrice','DayHighestPrice','DayLowestPrice','DayVolumeXbt',
            #                                          'DayVolumeXbtInSecondaryCurrrency','LastPrice',"SecondaryCurrencyCode","PrimaryCurrencyCode"])
            # mcw_df.rename(columns={"CurrentLowestOfferPrice":"buy_price","CurrentHighestBidPrice":"sell_price"}, inplace=True)
            # mcw_df["sell_price"] = mcw_df["sell_price"].apply(lambda x: x*aud_zar)
            # mcw_df["buy_price"] = mcw_df["buy_price"].apply(lambda x: x*aud_zar)

            #print("myCryptoWallet DF completed!")

            # -----------------------------------------------------------------------------------------------------------

            # ****** Kort 10000$ om rekening oop te maak ***** 
            #
            #bitfin_pairs_list = list(bittrex_df["pair"])
            # bitfin_list = []
            # for pair in bitfin_pairs_list:
            #     pair1 = pair.split("/")[0]
            #     pair2 = pair.split("/")[1]
            #     if pair2 == "zar":
            #         pair = "{}usd".format(pair1)
            #         bitfin_url = "https://api.bitfinex.com/v1/book/{}".format(pair.upper())
            #         try:
            #             bitfin_json = r.get(bitfin_url).json()
            #             mdict = {"pair":"{}/zar".format(pair1),"buy_price":float(bitfin_json["asks"][0]["price"])*usd_zar,
            #                       "buy_amount":round(float(bitfin_json["asks"][0]["amount"]),4),"sell_price":float(bitfin_json["bids"][0]["price"])*usd_zar,
            #                       "sell_amount":round(float(bitfin_json["bids"][0]["amount"]),4),"exchange":"BitFinex"}
            #             bitfin_list.append(mdict)

            #         except:
            #             continue

            # bitfin_df = pd.DataFrame.from_dict(bitfin_list)
            # print("BitFinex DF completed!")
            
    def arb_opportunities(self, arb_percentage = 4):
        if self.exchanges == "all":
            tot_df = pd.concat([self.luno_df,self.ice3x_df,self.alttrader_df, self.bittrex_df, self.coindir_df, self.cex_df, self.kraken_df], sort=True)
        elif self.exchanges == "usd":
            tot_df = pd.concat([self.bittrex_df, self.bitstamp_df, self.cex_df,self.kraken_df])
        elif self.exchanges == "zar":
            tot_df = pd.concat([self.luno_df, self.alttrader_df, self.ice3x_df])
        
        tot_df.reset_index(inplace=True)
        tot_df.drop(columns=['index'],inplace=True)
        tot_df["buy_price"] = pd.to_numeric(tot_df["buy_price"])
        tot_df["sell_price"] = pd.to_numeric(tot_df["sell_price"])
        unique_pairs = tot_df["pair"].unique()
        result = []
        print("{} unique crypto pairs found".format(len(unique_pairs)))
        for i, curr_pair in enumerate(unique_pairs):
            print("{} of {} pairs processed!".format(i+1,len(unique_pairs)), end='\r')
            min_val = tot_df[tot_df["pair"]==curr_pair]["buy_price"].min()
            min_exch = tot_df["exchange"][tot_df["buy_price"]==min_val]
            min_amnt = tot_df["buy_amount"][tot_df["buy_price"]==min_val].values[0]
            max_val = tot_df[tot_df["pair"]==curr_pair]["sell_price"].max()
            max_exch = tot_df["exchange"][tot_df["sell_price"]==max_val]
            max_amnt = tot_df["sell_amount"][tot_df["sell_price"]==max_val].values[0]
            if (min_exch.values[0] != max_exch.values[0]) and (float(min_val) != 0) and (curr_pair.split("/")[0]=="zar" or curr_pair.split("/")[1]=="zar"):
                arb_per = 100*(float(max_val)-float(min_val))/(float(max_val))
                if arb_per >  arb_percentage:

                    if min_exch.values[0].lower() == "bittrex":
                        pair = "USD-{}".format(curr_pair.split("/")[0].upper())
                        bittrex_url = "https://api.bittrex.com/api/v1.1/public/getorderbook?market={}&type=both".format(pair)
                        bittrex_json = r.get(bittrex_url).json()["result"]
                        min_amnt = round(float(bittrex_json["buy"][0]["Quantity"]),2)
                    elif max_exch.values[0].lower() == "bittrex":
                        pair = "USD-{}".format(curr_pair.split("/")[0].upper())
                        bittrex_url = "https://api.bittrex.com/api/v1.1/public/getorderbook?market={}&type=both".format(pair)
                        bittrex_json = r.get(bittrex_url).json()["result"]
                        max_amnt = round(float(bittrex_json["sell"][0]["Quantity"]),2)

                    if min_exch.values[0].lower() == "bitstamp":
                        pair = "{}usd".format(curr_pair.split("/")[0].lower())
                        bitstamp_url = "https://www.bitstamp.net/api/v2/order_book/{}".format(pair)
                        bitstamp_json = r.get(bitstamp_url).json()["bids"][0]
                        min_amnt = round(float(bitstamp_json[1]),2)
                    elif max_exch.values[0].lower() == "bitstamp":
                        pair = "{}usd".format(curr_pair.split("/")[0].lower())
                        bitstamp_url = "https://www.bitstamp.net/api/v2/order_book/{}".format(pair)
                        bitstamp_json = r.get(bitstamp_url).json()["asks"][0]
                        max_amnt = round(float(bitstamp_json[1]),2)

                    if min_exch.values[0].lower() == "altcoin":
                        pair = curr_pair.split("/")[0]
                        if pair == "btc":
                            altcoin_url = "https://www.altcointrader.co.za/"
                        else:
                            altcoin_url = "https://www.altcointrader.co.za/{}".format(pair)
                        resp = r.get(altcoin_url,params="lxml")
                        soup = bs4.BeautifulSoup(resp.text)
                        buy_info = soup.findAll("tr",{"class":"orderUdSell"})[0].text.split("\n")
                        min_amnt = buy_info[2]
                    elif max_exch.values[0].lower() == "altcoin":
                        pair = curr_pair.split("/")[0]
                        if pair == "btc":
                            altcoin_url = "https://www.altcointrader.co.za/"
                        else:
                            altcoin_url = "https://www.altcointrader.co.za/{}".format(pair)
                        resp = r.get(altcoin_url,"lxml")
                        soup = bs4.BeautifulSoup(resp.text)
                        sell_info = soup.findAll("tr",{"class":"orderUdBuy"})[0].text.split("\n")
                        max_amnt = sell_info[2]

                    my_dict = {"Pair":curr_pair,"Minimum Buy Price":min_val,"Buy Amount":min_amnt,"Buy Exchange":min_exch.values[0],
                               "Maximum Sell Price":max_val,"Sell Amount":max_amnt,"Sell Exchange":max_exch.values[0],"Percentage":arb_per}
                    result.append(my_dict)

        result_df = pd.DataFrame.from_dict(result)
        result_df = result_df[["Pair","Minimum Buy Price","Buy Amount","Buy Exchange","Maximum Sell Price","Sell Amount","Sell Exchange","Percentage"]]
        result_df.sort_values(by="Percentage", ascending=False, inplace=True)
        result_df.reset_index(inplace=True)
        result_df.drop("index", axis=1, inplace=True)
        
        self.tot_df = tot_df
        self.result_df = result_df
        return result_df
    
    def archive(self,my_df):
        x = dt.datetime.now()
        date = "{}.{}.{}".format(x.year,x.month, x.day)
        time = "{}:{}".format(x.hour, x.minute)
        my_df["Timestamp"] = ["{} {}".format(date,time) for i in range(len(my_df))]
        my_df.set_index(["Timestamp"],inplace=True)
        with open("archive_data.csv", "a") as f:
            my_df.to_csv(f, sep=";" )

    def counter_arb(self,zar_df,usd_df):
        my_df = pd.DataFrame.copy(zar_df)
        my_df.set_index('pair', inplace=True)
        usd_copy = pd.DataFrame.copy(usd_df)
        usd_copy.set_index('pair', inplace=True)

        new_df = my_df.join(usd_copy, lsuffix="_")
        new_df.dropna(inplace=True)
        new_df.drop(columns=["sell_price_","buy_price"], axis=1,inplace=True)
        i = list(new_df["buy_price_"])
        j = list(new_df["sell_price"])
        data = []
        for k in range(len(i)):
            som = -100*(float(i[k])-float(j[k]))/float(j[k])
            data.append(som)
        new_df["per"] = data
        return new_df



