# -*- coding: utf-8 -*-
"""
Created on Thu Apr  4 13:05:05 2024

@author: NXP
"""

import requests
import time
import json
import gspread
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload
import io
from googleapiclient.errors import HttpError
import gspread_dataframe as gd
import datetime
from dateutil.relativedelta import relativedelta
from io import BytesIO
import os
import gspread as gs
import glob
import numpy as np

credentials ={
}
gc= gs.service_account_from_dict(credentials)


def export_to_sheets(file_name,sheet_name):
    ws = gc.open(file_name).worksheet(sheet_name)
    return gd.get_as_dataframe(worksheet=ws,value_render_option='FORMATTED_VALUE')

def export_to_sheets2(file_name, sheet_name):
    ws = gc.open(file_name).worksheet(sheet_name)
    data = ws.get_all_values()
    headers = data.pop(0)
    return pd.DataFrame(data, columns=headers)
    
def poll_job(s, redash_url, job):
    # TODO: add timeout
    while job['status'] not in (3,4):
        response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
        job = response.json()['job']
        time.sleep(1)

    if job['status'] == 3:
        return job['query_result_id']
    
    return None


def get_fresh_query_result(redash_url, query_id, api_key, params):
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})

    payload = dict(max_age=0, parameters=params)

    response = s.post('{}/api/queries/{}/results'.format(redash_url, query_id), data=json.dumps(payload))

    if response.status_code != 200:
        return 'Refresh failed'
        raise Exception('Refresh failed.')

    result_id = poll_job(s, redash_url, response.json()['job'])

    if result_id:
        response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
        if response.status_code != 200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')

    return response.json()['query_result']['data']['rows']

print("pulling mapping...")

def round_billing_weight(data):  
    for i in range(int(data["Billing Weight"].max())):
        
        high_line = i + 1.3
        low_line = i + 0.3
        if i == 0:
            condition = (data.loc[:, 'Billing Weight'] <= high_line)
            data.loc[condition, 'Billing Weight'] = i + 1
        elif i == data["Billing Weight"].max():
            condition = (data.loc[:, 'Billing Weight'] <= high_line) & (data.loc[:, 'Billing Weight'] > low_line)
            data.loc[condition, 'Billing Weight'] = i + 1
            condition_2 = data.loc[:, 'Billing Weight'] > high_line
            data.loc[condition_2, 'Billing Weight'] = i + 2
        else:
            condition = (data.loc[:, 'Billing Weight'] <= high_line) & (data.loc[:, 'Billing Weight'] > low_line)
            data.loc[condition, 'Billing Weight'] = i + 1
    return data
#Read Mapping files, there are 3, vol tiering, rate card, and region_mapping
vol_tier = export_to_sheets('TikTok Rate Card Mapping','Volume Tier1')
vol_tier = vol_tier.iloc[:, [0,1,2,3,4,5,6,7,8,9,10]]
rate_card = export_to_sheets2('TikTok Rate Card Mapping','Rate Card')
rate_card.columns = rate_card.iloc[0]
rate_card = rate_card.iloc[:, [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]]
region_mapping = export_to_sheets('TikTok Rate Card Mapping','Mapping to Region Grouping').iloc[:, [1,3,5,8,9,10]]
region_mapping = region_mapping.iloc[:, [4,5]]
region_mapping.drop_duplicates(inplace=True)



print(vol_tier.info())
print(rate_card.info())
print(region_mapping.info())


#Read SSB
ssb_new = []
path2 = r'C:\Users\NXP\Desktop\TikTok Ratecard\SSB_Tiktok' # use your path
all_files = glob.glob(os.path.join(path2 + "/*.csv"))
for filename in all_files:
    print(filename)
    ssb = pd.read_csv(filename, index_col=None, header=0, low_memory=False,skip_blank_lines=False)
    ssb.dropna(how='all', inplace=True)
    ssb = ssb.iloc[:, 0:36]
    #ssb=ssb[['Legacy Shipper ID','Shipper Name','Billing Name','Tracking ID','Shipper Order Ref','Order Granular Status','Customer Name','Delivery Type Name','Delivery Type ID','Service Type','Service Level','Parcel Size ID','Billing Weight','Create Time','Delivery Date','From City','From Billing Zone','Origin Hub','L1 Name','L2 Name','L3 Name','To Postcode','To Billing Zone','Destination Hub','Delivery Fee','COD Value','COD Fee','Insured Value','Insurance Fee','RTS Fee','Handling Fee','Total','Script ID','Script Version','Last Calculated Date']]
    ssb_new.append(ssb)
    
ssb = pd.concat(ssb_new)
print("Initial data")
print(ssb.info())

ssb_w_region= pd.merge(ssb,region_mapping[['Ninja l2','Region Group for Vol Tier']], left_on =['L2 Name'],right_on=['Ninja l2'],how ='left')
# ssb_w_region.to_csv("ssb_w_region.csv")


###########pivoting###############################################################
pivot_destl2 = ssb_w_region.pivot_table(index='Region Group for Vol Tier', values='Tracking ID', aggfunc='count').reset_index()
pivot_destl2 = pivot_destl2.rename(columns={'Tracking ID': 'Count TRID'})


pivot_target = pd.merge(pivot_destl2,vol_tier,left_on =['Region Group for Vol Tier'],right_on=['Dest Island'],how = 'left')
pivot_target['Set'] = np.where(pivot_target['Count TRID'] < pivot_target['Tier 1'], 'Existing Rate',
                    np.where((pivot_target['Count TRID'] >= pivot_target['Tier 1']) & (pivot_target['Count TRID'] < pivot_target['Tier 2']), 'Tier 1 Rate',
                        np.where((pivot_target['Count TRID'] >= pivot_target['Tier 2']) & (pivot_target['Count TRID'] < pivot_target['Tier 3']), 'Tier 2 Rate',
                            np.where((pivot_target['Count TRID'] >= pivot_target['Tier 3']) & (pivot_target['Count TRID'] < pivot_target['Tier 4']), 'Tier 3 Rate',
                                np.where((pivot_target['Count TRID'] >= pivot_target['Tier 4']) & (pivot_target['Count TRID'] < pivot_target['Tier 5']), 'Tier 4 Rate',
                                    'Tier 5 Rate'
                                )
                            )
                        )
                    )
                )
pivot_target.to_excel(r'C:\Users\NXP\Desktop\TikTok Ratecard\output\rate_categ_perdest.xlsx')



#####################join rate to ssb##########################################
rate_card["njv_oril2_destl2"] = rate_card["Ori L2 Name"] + rate_card["Dest L2 Name"]
ssb_w_region["njv_oril2_destl2"] = ssb_w_region["From City"] + ssb_w_region["L2 Name"]
ssb_w_rate= pd.merge(ssb_w_region,rate_card[["njv_oril2_destl2","Existing Rate","Tier 1 Rate","Tier 2 Rate","Tier 3 Rate","Tier 4 Rate","Tier 5 Rate","Region Grouping for Vol Tier","Intra Jawo?"]], left_on =["njv_oril2_destl2"],right_on=["njv_oril2_destl2"],how ='left')
ssb_w_rate_and_tier = pd.merge(ssb_w_rate,pivot_target, left_on =["Region Grouping for Vol Tier"],right_on=["Region Group for Vol Tier"],how ='left')


###################count rate#######################################################
def extract_value(row):
    return row[row['Set']]

ssb_w_rate_and_tier['rate_used'] = ssb_w_rate_and_tier.apply(lambda row: extract_value(row), axis=1)
# Convert the strings to integers
ssb_w_rate_and_tier['rate_used'] = ssb_w_rate_and_tier['rate_used'].str.replace(',', '')
ssb_w_rate_and_tier['rate_used'] = ssb_w_rate_and_tier['rate_used'].astype(int)

#Round weight
ssb_w_rate_and_tier= round_billing_weight(ssb_w_rate_and_tier)


#Count rate
ssb_w_rate_and_tier['weight_x_rate'] = ssb_w_rate_and_tier['Billing Weight'] * ssb_w_rate_and_tier['rate_used']
#ssb_w_rate_and_tier['weight_x_rate_rts'] = (ssb_w_rate_and_tier['Billing Weight'] * ssb_w_rate_and_tier['rate_used'])+(ssb_w_rate_and_tier['Billing Weight'] * ssb_w_rate_and_tier['rate_used'] * 0.25)

rts_nonjawo_df = ssb_w_rate_and_tier[(ssb_w_rate_and_tier['Order Granular Status'] == 'Returned to Sender') & (ssb_w_rate_and_tier['Intra Jawo?'] == 'No')]
therest_df = ssb_w_rate_and_tier[~((ssb_w_rate_and_tier['Order Granular Status'] == 'Returned to Sender') & (ssb_w_rate_and_tier['Intra Jawo?'] == 'No'))]

rts_nonjawo_df['weight_x_rate_rts'] =(rts_nonjawo_df['Billing Weight'] * rts_nonjawo_df['rate_used'])+(rts_nonjawo_df['Billing Weight'] * rts_nonjawo_df['rate_used'] * 0.25)
therest_df['weight_x_rate_rts'] = ''

ssb_w_rate_and_tier = pd.concat([rts_nonjawo_df,therest_df])




# ssb_w_rate_and_tier.to_csv("ssbwrateandtier_2.csv")
# def set_weight_x_rate_rts(row):
#     if row['Order Granular Status'] == 'Returned to Sender' and row['Intra Jawo?'] == 'No':
#         return row['weight_x_rate_rts']
#     else:
#         return ''

# ssb_w_rate_and_tier['weight_x_rate_rts'] = ssb_w_rate_and_tier.apply(set_weight_x_rate_rts, axis=1)


############export to csv########################################################
Greater_Jakarta_df = ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'] == 'Greater Jakarta']
West_Java_df = ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'] == 'West Java']
Central_Java_df = ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'] == "Central Java"]
East_Java_df =  ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'] == "East Java"]
Bali_df =  ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'] == "Bali"]
Sumatera_df =  ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'] == "Sumatera"]
Kalimantan_df =  ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'] == "Kalimantan"]
Sulawesi_df =  ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'] == "Sulawesi"]
Maluku_df =  ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'] == "Maluku"]
Nusa_df =  ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'] == "Nusa"]
Papua_df =  ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'] == "Papua"]
l3_blank = ssb_w_rate_and_tier[ssb_w_rate_and_tier['Dest Island'].isna()]



print("final data info")
print(len(ssb_w_rate_and_tier))
print("GJ info")
print(len(Greater_Jakarta_df))
print("WJ info")
print(len(West_Java_df))
print("CJ info")
print(len(Central_Java_df))
print("EJ info")
print(len(East_Java_df))
print("Bali info")
print(len(Bali_df))
print("Sumatera info")
print(len(Sumatera_df))
print("Kalimantan info")
print(len(Kalimantan_df))
print("Sulawesi info")
print(len(Sulawesi_df))
print("Maluku info")
print(len(Maluku_df))
print("Nusa info")
print(len(Nusa_df))
print("Papua info")
print(len(Papua_df))
print("l3 blank")
print(len(l3_blank))
print(" ")
print("total")
print(len(Greater_Jakarta_df)+len(West_Java_df)+len(Central_Java_df)+len(East_Java_df)+len(Bali_df)+len(Sumatera_df)
      +len(Kalimantan_df)+len(Sulawesi_df)+len(Maluku_df)+len(Nusa_df)+len(Papua_df)+len(l3_blank))





def export_to_csv_in_chunks(df, file_prefix,params):
    chunk_size = 500000
    num_chunks = len(df) // chunk_size + 1
    output_dir = r"C:\Users\NXP\Desktop\TikTok Ratecard\output"
    if params =='Greater_Jakarta':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\Greater_Jakarta\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            
            print(f"Exported {chunk_file_name}")
    elif params =='West_Java':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\West_Java\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            print(f"Exported {chunk_file_name}")
    elif params =='Central_Java':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\Central_Java\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            print(f"Exported {chunk_file_name}")
    elif params =='East_Java':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\East_Java\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            print(f"Exported {chunk_file_name}")
    elif params =='Bali':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\Bali\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            print(f"Exported {chunk_file_name}")
    elif params =='Sumatera':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\Sumatera\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            print(f"Exported {chunk_file_name}")
    elif params =='Kalimantan':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\Kalimantan\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            print(f"Exported {chunk_file_name}")
    elif params =='Sulawesi':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\Sulawesi\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            print(f"Exported {chunk_file_name}")
    elif params =='Papua':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\Papua\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            print(f"Exported {chunk_file_name}")
    elif params =='Maluku':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\Maluku\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            print(f"Exported {chunk_file_name}")
    elif params =='Nusa':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\Nusa\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            print(f"Exported {chunk_file_name}")
    elif params =='blank':
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            chunk_file_name = rf"{output_dir}\blank\{file_prefix}_{i+1}.csv"
            chunk_df.to_csv(chunk_file_name, index=False)
            print(f"Exported {chunk_file_name}")

export_to_csv_in_chunks(Greater_Jakarta_df, 'Greater_Jakarta','Greater_Jakarta')
export_to_csv_in_chunks(West_Java_df, 'West_Java','West_Java')
export_to_csv_in_chunks(Central_Java_df, 'Central_Java','Central_Java')
export_to_csv_in_chunks(East_Java_df, 'East_Java','East_Java')
export_to_csv_in_chunks(Bali_df , 'Bali','Bali')
export_to_csv_in_chunks(Sumatera_df , 'Sumatera','Sumatera')
export_to_csv_in_chunks(Kalimantan_df, 'Kalimantan','Kalimantan')
export_to_csv_in_chunks(Sulawesi_df, 'Sulawesi','Sulawesi')
export_to_csv_in_chunks(Maluku_df, 'Maluku','Maluku')
export_to_csv_in_chunks(Nusa_df, 'Nusa','Nusa')
export_to_csv_in_chunks(Papua_df, 'Papua','Papua')
export_to_csv_in_chunks(l3_blank, 'blank','blank')


