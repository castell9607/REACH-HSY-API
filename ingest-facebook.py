#!/usr/bin/env python3
import csv
import json
import os
import pprint
import sys
import time

#Dentsu Packages
from dentsu_pkgs.misc_helpers import flatten_json

from datetime import datetime, timedelta
from facebook_business.adobjects.advideo import AdVideo


#Facebook Api
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.business import Business
from facebook_business.adobjects.adsinsights import AdsInsights




headers = []
pp = pprint.PrettyPrinter(width=1)



def make_call(start_date, end_date, ad_account_id, ad_account_name):
    fields = [
        "account_id",
        "account_name",        
        "account_currency",
        "date_start",
        "date_stop",
        "reach"    
    ]
        

    retryCounter = 0
    #2 Times will try to get the data
    while retryCounter < 3:
        async_job = AdAccount(ad_account_id).get_insights_async(fields=fields, params=params)    
        async_job.api_get()
        while (async_job._json["async_percent_completion"] < 100 or async_job._json["async_status"] != 'Job Completed' ):
            time.sleep(1)
            async_job.api_get()
            if (async_job._json["async_status"] == 'Job Failed'):
                break
            
        if (async_job._json["async_status"] == "Job Failed"):
            print (f"Error Al obtener los datos de la cuenta {ad_account_id} - {ad_account_name}")                
            if (retryCounter==2):
                return
            retryCounter = retryCounter + 1
            print(f"Esperando 10 segundos y Reintentando obtener datos para la cuenta {ad_account_id} - {ad_account_name}")
            time.sleep(10)
        else:
            retryCounter = 3

            
    flat_DataValidation = None
    for insight in async_job.get_result():        
        py_insight = dict(insight)
        flat_data = flatten_json(py_insight)
        flat_DataValidation = flat_data
            #print(flat_data)
        writer.writerow(flat_data)

                
        if not flat_DataValidation:
            print("NO DATA FOR {} IN: {} to {}".format(ad_account_name, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
            return




if __name__ == "__main__":
    # vars;
    
    startDate = datetime(2021,9,13)
    endDate = datetime(2021,9,19)
    
    params = {
        "level": "account",
        "time_increment": "all_days",
        "time_range": {"since": startDate.strftime("%Y-%m-%d"), "until": endDate.strftime("%Y-%m-%d")} 
    #"date_preset" : "last_week_mon_sun"
    }
    
    
    accessToken = "EAAKe2A26BmABAE0VYI9sLrA9uufbj2YPjGxOUZA5XDvOFlGcPc5QNhawq7ZAvaloeNmp1vFFK00JjOm8eA6iu1JbrwmhkHUkCZB5jA0p2rCzVuCMOOlzR0HuizSTMFplwftasTtShO7xfoWWKBuvGZB0ueXQNaVomDPz9yp5AkVCUfaYC5zL"
    appId = "737600733840992"
    appSecret = "7d7da246ded1721624c4bfdeebe4e1b3"
    business_id = "112026591182845"
    

    # auth facebook, init;
    FacebookAdsApi.init(appId, appSecret, accessToken, api_version="v12.0")
    business = Business(fbid=business_id)
    business_accounts = business.get_owned_ad_accounts()

    rows = []

    keys = [
        "account_id",
        "account_name",        
        "account_currency",
        "date_start",
        "date_stop",
        "reach"    
    ]


    filename = "{}-{}_to_{}.csv".format(business_id, startDate.strftime("%Y-%m-%d"),endDate.strftime("%Y-%m-%d"))
    

with open(filename, "w", newline='') as f_csv:      
    filename = "{}-{}_to_{}.csv".format(business_id + "_ReachReport_", startDate.strftime("%Y-%m-%d"),endDate.strftime("%Y-%m-%d"))  
    writer = csv.DictWriter(f_csv, fieldnames=keys, restval="", extrasaction="ignore")
    writer.writeheader() 
    for act in business_accounts:
        account_name = act.api_get(fields=[AdAccount.Field.name])["name"]           
        print(f"Imprimiendo Data para {act['id']} - {account_name}")
        make_call(startDate, endDate, act["id"], account_name)



    print("File Success: {}".format(filename))
