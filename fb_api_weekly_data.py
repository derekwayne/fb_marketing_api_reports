import datetime
import json
import os
import logging
import pandas as pd
import numpy as np
import time
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi

# define pathnames
file_pathname = __file__
directory_pathname = os.path.dirname(file_pathname)

#++++++++++++++++++++++++++++++++++++++
# CLIENT AUTHENTICATION
#++++++++++++++++++++++++++++++++++++++
client_secrets_path = '/home/wayned/acquire/credentials/facebook_business/client_secrets.json'
try:
    with open(client_secrets_path) as authentication_file:
            authentication_result = json.load(authentication_file)
    # READ AUTHENTICATION JSON FILE AND EXTRACT
    my_app_id = authentication_result['my_app_id']
    my_app_secret = authentication_result['my_app_secret']
    my_access_token = authentication_result['my_access_token']
    # AUTHENTICATE FACEBOOK API CALL WITH APP/USER CREDENTIALS
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    print("Authentication Success")
except:
    print("Authentication Failed")

#++++++++++++++++++++++++++++++++++++++++
# REPORTING FIELDS AND PARAMETERS
#++++++++++++++++++++++++++++++++++++++++
# define date ranges relative to today for
# consistent reporting periods
today = datetime.date.today()
yesturday = today - datetime.timedelta(days=1)
yesturday = yesturday.strftime('%Y-%m-%d')
first_of_month = datetime.date.today()
first_of_month = first_of_month.replace(day=1)
first_of_month = first_of_month.strftime('%Y-%m-%d')

params = {
    'time_range': {'since':first_of_month,
                   'until':yesturday},
        'time_increment': 1,
        }
fields = [AdsInsights.Field.date_start,
        AdsInsights.Field.spend,
        AdsInsights.Field.actions,
        ]

#++++++++++++++++++++++++++++++++++++++++
my_account = AdAccount('act_<ACCOUNT ID>')

my_file_name = 'my_file_name.csv'
# extract data into json
insights_cursor = my_account.get_insights(fields=fields, params=params)

#+++++++++++++++++++++++++++++++++++++++++
# CLEANING
#+++++++++++++++++++++++++++++++++++++++++
# column names must match eg: AdsInsights.Field.spend = spend
columns = ['date_start', 'spend', 'actions']
# iterable (insights_cursor) will not work in older version of pandas
df = pd.DataFrame(insights_cursor, columns=columns)

df['spend'] = pd.to_numeric(df['spend'])


def find(lst, key, value):
    """
    lst: a list of dictionaries
    returns: index of dictionary in list with (key,value)
    """
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return i
    # value not found:
    return -1

def extract_col(row, value):
    """row: a row from a pandas df
    value: a type of action  -- eg: mobile app installs
    returns: data associated with key, value pair
    """
    if type(row) != list:
        return 0
    else:
        index = find(lst=row, key='action_type', value=value)
        # index will return -1 if it cannot find the value
        # in the dictionary
        if index == -1:
            return 0
        return row[index]['value']

# example data manipulation
# apply it to create new columns
df['mobile_app_installs'] = df['actions'].apply(extract_col, value='mobile_app_install')
df["mobile_app_installs"] = pd.to_numeric(df["mobile_app_installs"])
df['cost_per_install'] = df['spend'] / df['mobile_app_installs']

df['registrations_completed'] = df['actions'].apply(extract_col, value='app_custom_event.fb_mobile_complete_registration')
df['registrations_completed'] = pd.to_numeric(df['registrations_completed'])
df['cost_per_registration'] = df['spend'] / df['registrations_completed']


# drop actions column
df = df.drop(columns=['actions'])

#++++++++++++++++++++++
# DATA EXPORT TO CSV
#++++++++++++++++++++++

df.to_csv(my_file_name, encoding='utf-8', index=False)
print('csv generated in: ' + my_file_name)
