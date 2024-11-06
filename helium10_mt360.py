'''
Loop through MT360 accounts

Initially get all data up from all accounts
Weekly data + monthly data (when running in a week starting the new month)
All products data once 

Store payload in proper local place and upload to proper cloud storage
Save last run date
First time do download everything from 1.1.2021 until yesterday
Every next run download last six months data
    Monthly download occurs only once per month, if it is first week of month
    
    JSON payload store in Pandas and stitch together the data
    Store in the following structure
    
    type_of_mt360 (keyword, brand, subcat)
      +-- market (country)
       +-- segment type (keyword, subcategory)
        +-- identifier (VMS, Category name, Diet...)
         +-- week or month
          +-- date of ingestion
'''
import pandas as pd
import os
import requests
from datetime import datetime, timedelta
import time
# storing the data in google cloud
from google.cloud import storage
import json
import argparse

# load credentials
with open('../credentials_h10.json', 'r') as f:
    config = json.load(f)

# GCS credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='../bayer-ch-ecommerce-282069d49dcf.json'

account_id = config['account_id']
api_key  = config['api_key']

# GCS root for all exported data
gcs_bucket = 'ch_commerce_incoming'
gcs_root_path = 'global/h10/mt360_api'

segments_to_download = [
 {
      "segment_type":"subcategory",
      "segment_name": "Diet_Nutrition",
      "country": "UK",
      "mt360id": 192080,
      "1p": 264503,
      "3p":264504
    },
    {
        "segment_type":"subcategory",
    "segment_name": "VMS",
    "country": "UK",
    "mt360id": 173153,
      "1p": 264506,
      "3p":264507
    },
    {
        "segment_type":"subcategory",
    "segment_name": "VMS",
    "country": "ES",
    "mt360id": 173155,
      "1p": 264514,
      "3p":264515
    },
    {
        "segment_type":"subcategory",
    "segment_name": "Diet_Nutrition",
    "country": "ES",
    "mt360id": 192082,
      "1p": 264512,
      "3p":264513
    },
    {
        "segment_type":"subcategory",
    "segment_name": "Diet_Nutrition",
    "country": "IT",
    "mt360id": 192084,
      "1p": 264520,
      "3p":264521
    },
    {
        "segment_type":"subcategory",
    "segment_name": "VMS",
    "country": "IT",
    "mt360id": 173157,
      "1p": 264522,
      "3p":264523
    },
    {
        "segment_type":"subcategory",
    "segment_name": "Diet_Nutrition",
    "country": "FR",
    "mt360id": 192083,
      "1p": 264516,
      "3p":264517
    },
    

    {
        "segment_type":"subcategory",
    "segment_name": "VMS",
    "country": "FR",
    "mt360id": 173156,
      "1p": 264518,
      "3p":264519
    },
    {
        "segment_type":"subcategory",
    "segment_name": "Diet_Nutrition",
    "country": "DE",
    "mt360id": 192081,
      "1p": 264508,
      "3p":264509
    },
    
    {
        "segment_type":"subcategory",
    "segment_name": "VMS",
    "country": "DE",
    "mt360id": 173154,
      "1p": 264510,
      "3p":264511
    }
  ]

# List of markets to download data from
backup_segments_to_download = [
  {
      "segment_type":"subcategory",
      "segment_name": "Diet_Nutrition",
      "country": "UK",
      "mt360id": 192080
    },
    {
        "segment_type":"subcategory",
    "segment_name": "VMS",
    "country": "UK",
    "mt360id": 173153
    },
    {
        "segment_type":"subcategory",
    "segment_name": "VMS",
    "country": "ES",
    "mt360id": 173155
    },
    {
        "segment_type":"subcategory",
    "segment_name": "Diet_Nutrition",
    "country": "ES",
    "mt360id": 192082
    },
    {
        "segment_type":"subcategory",
    "segment_name": "Diet_Nutrition",
    "country": "IT",
    "mt360id": 192084
    },
    {
        "segment_type":"subcategory",
    "segment_name": "VMS",
    "country": "IT",
    "mt360id": 173157
    },
    {
        "segment_type":"subcategory",
    "segment_name": "Diet_Nutrition",
    "country": "FR",
    "mt360id": 192083
    },
    
    {
        "segment_type":"subcategory",
    "segment_name": "Diet_Nutrition",
    "country": "DE",
    "mt360id": 192081
    },
    
    {
        "segment_type":"subcategory",
    "segment_name": "VMS",
    "country": "DE",
    "mt360id": 173154
    },

    {
        "segment_type":"subcategory",
    "segment_name": "VMS",
    "country": "FR",
    "mt360id": 173156
    }

    ]

def run_argument_parser():
    # Create the parser
    parser = argparse.ArgumentParser(description="Running H10 MT360 report data download.")

    # Add arguments
    parser.add_argument("--market",         help="name of the market DE, UK, IT...",                type=str, default="ALL")
    parser.add_argument("--segment_type",   help="segment type keyword, category, brand based...",  type=str, default="ALL")
    parser.add_argument("--segment_name",   help="segment name VMS, diet, etc...",                  type=str, default="ALL")
    parser.add_argument("--type_of_report", help="do you need products, revenue or units...",       type=str, default="ALL")

    # Parse arguments
    args = parser.parse_args()

    # Implement your application logic here using the parameters
    # For example: print the parameters
    print(f"Value for market:           {args.market}")
    print(f"Value for segment type:     {args.segment_type}")
    print(f"Value for segment name:     {args.segment_name}")
    print(f"Value for type of report:   {args.type_of_report}")
    
    return args

# GOogle Cloud Storage Upload
def upload_to_bucket(gcs_path_for_storing, local_filename):
    ''' Upload data to bucket '''
    # get actual filename from local_filename
    filename = local_filename.split('/').pop()
    # define gcs_storage_path from parameters
    # type_of_files - revenue, units, products
    # market - de, uk, es, it
    # segment_type - keyword, category, brand...
    # segment - VMS, diet, energy, etc...
    # type_of_download - week or month
    # date_to_store - date of ingestion yyyy-mm-dd
    gcs_storage_path_for_upload = gcs_root_path + '/'+ gcs_path_for_storing + '/'+filename
    
    # initiate storage client for GCS
    storage_client = storage.Client()

    # select the bucket
    bucket = storage_client.get_bucket(gcs_bucket)

    # get the location
    blob = bucket.blob(gcs_storage_path_for_upload)
    
    # upload the file
    blob.upload_from_filename(local_filename)
    
    '''
    print("If you reached this point it means that the data was saved on Cloud...")
    print(" ")
    print(" ")
    '''
    return None

def make_api_call(mt360id, period, type_of_report, start_date, end_date):
    # translate string to value
    if(period=='weekly'):
        period_value = 0 
    else:
        period_value = 1

    headers = {
        'Authorization': 'Bearer '+api_key,
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    # based on documentation from https://mt360.helium10.com/public-api/v1/documentation
    params_performance = {
        "groupByType": 1, # NO_GROUP = 0, PRODUCT = 1, BRAND = 2, COMPETITORS = 3, SELLER = 4
        "trendsType":0, # NO_TRENDS = 0, YEAR_OVER_YEAR = 1, MONTH_OVER_MONTH = 2, WEEK_OVER_WEEK = 3
        "viewByType": period_value,# WEEKLY = 0, MONTHLY = 1, YEARLY = 2
        "dataShowType":0, # VALUES = 0, PERCENTS = 1
        "showActuals":0, # WITHOUT_ACTUALS_DATA = 0, WITH_ACTUALS_DATA = 1
        "parentDataMode":0, # VARIATIONS_MODE = 0, PARENT_MODE = 1
        "filter[dateFrom]":start_date,
        "filter[dateTo]":end_date,
        # only for testing and saving API payloads, should be removed or commented in production
        "limit": 1000
    }
    
    params_products = {
        "parentDataMode":0, # VARIATIONS_MODE = 0, PARENT_MODE = 1
        "filter[dateFrom]":start_date,
        "filter[dateTo]":end_date,
        # only for testing and saving API payloads, should be removed or commented in production
        #"page":1,
        #"per_page":10
    }
   
    # pickup the right params for API call
    if (type_of_report=='products'):
        params = params_products
    else:
        params = params_performance

    # shoot the request to API
    response = requests.get(f'https://mt360.helium10.com/public-api/v1/markets/{mt360id}/{type_of_report}', params=params, headers=headers)
    
    '''    
    print("This is the API request: ")
    print(f'https://mt360.helium10.com/public-api/v1/markets/{mt360id}/{type_of_report}')
    print(params)
    print(headers)
    '''
    
    # what we got?
    return response

def period_to_run():
    today = datetime.now()
    #today = datetime(2022, 6, 2)
    # manipulate with starting date of download, default is 26 weeks (or 182 days)
    # length_of_period = 128
    length_of_period = 188
    first_day_of_month = today.replace(day=1)
    first_week_of_month = [first_day_of_month + timedelta(days=i) for i in range(16)]
    
    # Determine periods
    last_week = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    last_month = (first_day_of_month - timedelta(days=1)).strftime('%Y-%m-%d')
    start_of_period = (today - timedelta(days=length_of_period)).strftime('%Y-%m-%d')
        
    # Determine if within first week of the month
    if today in first_week_of_month:
        periods = [("weekly", last_week,start_of_period), ("monthly", last_month,start_of_period)]
    else:
        periods = [("weekly", last_week,start_of_period)]
    
    return periods

def process_json_payload(json_payload, type_of_report):
    print(f'Processing report type {type_of_report}')

    # load JSON file
    data = json.dumps(json_payload)
    parsed_data = json.loads(data)
    # make the panda
    df = pd.json_normalize(parsed_data)
    
    # pickup the right name of the field to change
    if(type_of_report=='sales'):
        value_column = 'totalSales'
    else:
        value_column = 'totalRevenue'
    
    if (type_of_report!='current-data'):
        # run the whole pickup, transform, stitch and delivery
        df_data = pd.DataFrame(df['data'][0])
        df_additionalData = pd.DataFrame(df['additionalData'][0])
        df_intervals = pd.DataFrame(df['meta.intervals'][0])
        
        # melt stuff
        transformer_units = df_data.melt(["resultAsin",value_column], var_name='intervalName', value_name='Value')

        # manipulate intervals
        df_intervals['startDate']       = pd.to_datetime(df_intervals['intervalStart'], format='ISO8601')
        df_intervals['endDate']         = pd.to_datetime(df_intervals['intervalEnd'], format='ISO8601')
        df_intervals['intervalName']    = 'interval'+df_intervals['intervalNumber'].astype(str)
        
        # remove unnecessary columns from intervals
        del df_intervals['intervalStart']
        del df_intervals['intervalEnd']
        del df_intervals['intervalNumber']
        
        # create first merge
        bigPanda_units = pd.merge(transformer_units,df_intervals, on='intervalName',how='left')

        # rename column and remove unnecessary column
        bigPanda_units.rename(columns = {'resultAsin':'asin'}, inplace = True)
        del bigPanda_units['intervalName']

        # merge bigPanda with additional data (products information)
        finalPanda = pd.merge(bigPanda_units,df_additionalData,on='asin',how='left')

        # remove index
        finalPanda = finalPanda.reset_index(drop=True)
    else:
        print('Processing products...')
        df_data = pd.DataFrame(df['data'][0])
        finalPanda = df_data
        
        # remove index
        finalPanda = finalPanda.reset_index(drop=True)
        
    return finalPanda

def running_segments_and_reports(period,start_date,end_date):

    # running stuff for selected mt360id
    for segment in segments_to_download:
        segment_type = segment["segment_type"]
        segment_name = segment["segment_name"]
        country = segment["country"]
        mt360id = segment["mt360id"]
        
        # types of reports that can be taken from API, where revenue and units are performance reports, and products is product information  
        report_types = ('revenue','sales') #,'current-data'

        for reports in report_types:
            print(f'Running MT360 : {mt360id} and market name {country} {segment_name} with report {reports}')
            
            # use different names for report storage
            if (reports == 'current-data'):
                real_report_name = 'products'
            elif (reports == 'sales'):
                real_report_name = 'units'
            else:
                real_report_name = 'revenue'
            
            # where to store the data
            gcs_path_to_save_output = (real_report_name+'/country='+country+'/segment_type='+segment_type+'/segment_name='+segment_name+'/period='+period+'/dt='+end_date).lower()
            print(f'Where the data will be saved ... {gcs_path_to_save_output}')
            time.sleep(5)
            
            # make API call 
            response = make_api_call(mt360id, period, reports, start_date, end_date)
            
            print(f'Downloading data...for {reports}')
            # work it out based on reports status
            if response.status_code == 200:
                data = response.json()
                data_to_save = process_json_payload(data,reports)
                save_data(segment_type, country, segment_name, period, end_date, data_to_save,gcs_path_to_save_output,reports,start_date)
            # if we have to wait for the report to generate
            elif response.status_code == 202:
                print('Wait a little bit as there is data being generated by H10 system')
                while response.status_code == 202:
                    time.sleep(60)
                    response = make_api_call(mt360id, period, reports, start_date, end_date)
                    #where_are_we_at = response.json()
                    #print("Still working on it. Currently at : "+str(where_are_we_at['data']['progress']))
                if response.status_code == 200:
                    data = response.json()
                    data_to_save = process_json_payload(data,reports)
                    save_data(segment_type, country, segment_name, period, end_date, data_to_save,gcs_path_to_save_output,reports,start_date)
            # if everything fails
            elif response.status_code >= 400:
                print('There is some kind of error.')
                print(response.json)
                print(response.raw)
                save_error(segment_type, country, segment_name, period, start_date, response.status_code)

def save_data(segment_type, country, segment_name, period, end_date, data, gcs_path,report_name, start_date):
    ''' Save data localy '''
    # date_str = datetime.now().strftime('%Y-%m-%d')
    directory = os.path.join('local_storage',report_name.lower(),segment_type.lower(), country.lower(), segment_name.lower(), period, end_date)
    os.makedirs(directory, exist_ok=True)
    
    file_path = os.path.join(directory, 'data.csv')
    # save localy
    data.to_csv(file_path,index=False)
    # save to cloud
    upload_to_bucket(gcs_path, file_path)
    print('If you reached this space, it means we saved the file on Cloud')

def save_error(segment_type, country, segment_name, period, start_date, status_code):
    ''' Save errors  '''
    error_log = {
        "segment_type": segment_type,
        "country": country,
        "segment_name": segment_name,
        "period": period,
        "start_date": start_date,
        "status_code": status_code,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    directory = os.path.join('errors', segment_type, country, segment_name, period)
    os.makedirs(directory, exist_ok=True)
    
    file_path = os.path.join(directory, 'error_log.json')
    with open(file_path, 'a') as f:
        f.write(json.dumps(error_log) + '\n')

def find_segment(segment_type, segment_name, country, segments_list):
    for segment in segments_list:
        if (segment['segment_type'] == segment_type and segment['segment_name'] == segment_name and segment['country'] == country):
            # Amazing we found the value of mt360 market analysis
            found_segment_id = segment['mt360id']
            print(f'Found segment: {found_segment_id}')
            print('Waiting for you to break the process...')
            time.sleep(5)
            return segment['mt360id']
    return None

if __name__ == "__main__":
    
    # pickup arguments from command line
    arguments = run_argument_parser()
    
    # set initial variables
    running_market          = arguments.market
    running_segment_type    = arguments.segment_type
    running_segment_name    = arguments.segment_name
    running_type_of_report  = arguments.type_of_report
    
    # what are the periods to run weekly or weekly&&monthly with specific start date
    periods = period_to_run()
    
    print(periods)
    print('Wait couple of seconds to check where are we...')
    time.sleep(10)
    
    # download either weekly or weekly&&monthly download
    for period, end_date, start_date in periods:
        # find the right market to download
            print('Running ALL MT360 markets and specific type of report(s)')
            print(" ")
            running_segments_and_reports(period,start_date,end_date)
