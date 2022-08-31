# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 13:28:53 2022

@author: Gary

Used to investigate teh structure of different pdf types to 
learn how to read them
"""

import pandas as pd
import os
import camelot

from filename_info_extractor import Filename_info_extractor as fnie
from metadata_extractor import Metadata_extractor as mde
from PDF_storage_handler import Storage_handler


def store_old_file(sh,idx = 0,filter='not collected'):
    print('testing...')
    t = sh.meta_df[sh.meta_df.status==filter]
    fn = t.iloc[idx].fn
    store_dir = t.iloc[idx].store_dir
    print(fn,store_dir)
    sh.fetch_to_work_file(fn,store_dir)
    

def get_meta_from_old_type(sh):
    return camelot.read_pdf(sh.workfn)
    # t = tables[0].df
    # print(t[0][0])


if __name__=='__main__':
    sh = Storage_handler()
    store_old_file(sh,filter='not collected',idx=23)
    t = get_meta_from_old_type(sh)
    print(t[0].df.iloc[3].T)