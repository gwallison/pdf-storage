# -*- coding: utf-8 -*-
"""
Created on Wed Jan 19 19:36:27 2022

@author: Gary
"""
import pandas as pd
import os

from filename_info_extractor import Filename_info_extractor as fnie
from PDF_storage_handler import Storage_handler

trans_dir = 'c:/MyDocs/OpenFF/data/transformed/'

def get_filename_info():
    sh = Storage_handler()
    extr = fnie()
    out = []
    dstr = []
    fn = []
    store_dir = []
    api = []
    for i,row in sh.master_df.iterrows():
        out.append(extr.return_ftype(row))
        dstr.append(extr.return_date(row))
        api.append(extr.get_API(row.fn))
        fn.append(row.fn)
        store_dir.append(row.store_dir)
    tst = pd.DataFrame({'fn':fn,'store_dir':store_dir,'ftype':out,
                        'APINumber':api,'datestr':dstr})
    tst['parse_date'] = tst.datestr.map(lambda x: extr.parse_fndate(x))
    print(tst.ftype.value_counts())
    tst.to_csv(os.path.join(trans_dir,'PDF_fn_info.csv'),quotechar='$',
               encoding='utf-8',index=False)

if __name__ == '__main__':
    df = get_filename_info()    