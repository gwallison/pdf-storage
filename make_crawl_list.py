# -*- coding: utf-8 -*-
"""
Created on Fri Mar 11 13:33:06 2022

@author: Gary

This script is used to gather a list of all the new and changed disclosures
so that the crawler script can collect their pdfs.
"""

###############################  Used to make repository and common accessible ####################
import sys
sys.path.insert(0,'c:/MyDocs/OpenFF/src/')
import common.code.Analysis_set_remote as ana_set
import pandas as pd
import numpy as np
import datetime
from PDF_storage_handler import Storage_handler
from filename_info_extractor import Filename_info_extractor as fnie


# adjust this date to prevent from recrawling too much
last_crawl = datetime.datetime(year=2022,month=1,day=1)

trans_dir = r"C:\MyDocs\OpenFF\data\transformed"

tw_summary = pd.read_csv(trans_dir+'/tripwire_summary.csv',quotechar='$',encoding='utf-8',
                         dtype={'APINumber':str})
tw_summary.new_date = pd.to_datetime(tw_summary.new_date,errors='coerce')
tw_api = tw_summary[tw_summary.new_date>last_crawl].APINumber.str[:10].unique().tolist()

# now get list of already downloaded api
sh = Storage_handler()
extr = fnie()
have_pdf = []
for i, row in sh.master_df.iterrows():
    have_pdf.append(extr.get_API(row.fn))
print(len(have_pdf))



repo_df = ana_set.Catalog_set(repo='v12_BETA_2022-02-19').get_set()
repo_df['api10'] = repo_df.APINumber.str[:10]
gb = repo_df.groupby(['api10','UploadKey'],as_index=False)['bgCAS'].count()
gb = gb.groupby('api10',as_index=False)['UploadKey'].count()
mg = pd.merge(gb,pd.DataFrame({'api10':have_pdf}),on='api10',how='outer',indicator=True)

out = mg[mg._merge=='left_only'][['api10']]
out = pd.concat([out,pd.DataFrame({'api10':tw_api})])
out.to_csv('./to_scrape_mar2022.csv',quotechar='$')

if __name__ == '__main__':
    pass