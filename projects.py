# -*- coding: utf-8 -*-
"""
Created on Wed Jan 19 19:36:27 2022

@author: Gary
"""

###############################  Used to make repository and common accessible ####################
import sys
sys.path.insert(0,'c:/MyDocs/OpenFF/src/')
import common.code.Analysis_set_remote as ana_set
import pandas as pd
import numpy as np
import os
import string

from filename_info_extractor import Filename_info_extractor as fnie
from metadata_extractor import Metadata_extractor as mde
from PDF_storage_handler import Storage_handler

trans_dir = 'c:/MyDocs/OpenFF/data/transformed/'
print('fetching repo df')
repo_df = ana_set.Catalog_set(repo='v12_BETA_2022-02-19').get_set()

def get_filename_info():
    # use the filename of each pdf to extract APINumber and creation date
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

# def get_disc_df(df,fn,dirname,lst):
#     df['fn'] = fn
#     df['store_dir'] = dirname
#     lst.append(df)
#     return lst

    
def batch_meta_data(batchdf,sh):
    extr = mde()
    fnextr = fnie()
    alldfs = []
    for i,row in batchdf.iterrows():
        print(i, row.fn)
        sh.fetch_to_work_file(row.fn,row.store_dir)
        #print('transferred')
        extr.init_file_meta_dic()
        extr.get_file_meta(row.fn, row.store_dir)
        fntype = fnextr.get_filetype(row.fn)
        status = 'not collected'
        df = pd.DataFrame()
        try:
            if fntype=='old':
                df = extr.get_meta_from_old_type()
            if fntype=='new':
                df = extr.get_meta_from_new_type()
            if len(df)>0:
                status = 'ok'
        except:
            print('***Could not process')
        if len(df)==0:
            df = pd.DataFrame({'data_scraped':[False]})
        else:
            df['data_scraped'] = True
        df['status'] = status
        df['fn'] = row.fn
        df['store_dir'] = row.store_dir
        df = pd.merge(df,pd.DataFrame(extr.file_meta_dic),
                              on=['fn','store_dir'],how='left')
        
        # test if df is ok for concat (some disclosures have problems here)
        try:
            _ = pd.concat([sh.meta_df[:5],df])
        except:
            df = pd.DataFrame({'data_scraped':[False]})
            df['status'] = 'concat error'
            df['fn'] = row.fn
            df['store_dir'] = row.store_dir
            
        #print(df.T)
        alldfs.append(df)
    return pd.concat(alldfs,sort=True,ignore_index=True)

def get_metadata(batchsize=100):
    sh = Storage_handler()    
    batch = sh.get_meta_todo(batchsize)
    while len(batch)>0:
        newdf = batch_meta_data(batch, sh)
        sh.concat_and_save_meta_df(newdf)
        batch = sh.get_meta_todo(batchsize)
        
def has_been_scraped(sh,fn,store_dir):
    try:
        return os.path.isfile(os.path.join(sh.scraping_dir,
                                           store_dir,
                                           fn[:-4]+'.csv'))
    except:
        return False
    
def has_scraped_dir(sh,store_dir):
    try:
        return os.path.isdir(os.path.join(sh.scraped_loc,
                                           store_dir))
    except:
        return False
    
def get_scraped(sh,fn,store_dir):
    return pd.read_csv(os.path.join(sh.scraped_loc,
                                       store_dir,
                                       fn[:-4]+'.csv'))

def scrape_to_dfs(idx_start=0,types=['old','?','new']):
    sh = Storage_handler()
    extr = fnie()
    me = mde()
    idx = idx_start
    while len(sh.master_df):
        row = sh.master_df.iloc[idx]
        if extr.get_filetype(row.fn) in types:
            print(f'\n{idx}: {row.fn}')
            if not has_been_scraped(sh, row.fn, row.store_dir):
                sh.fetch_to_work_file(row.fn, row.store_dir)
                outdf = me.get_tables_from_any_type()
                if not has_scraped_dir(sh, row.store_dir):
                    print(f'making new dir: {row.store_dir}')
                    os.mkdir(os.path.join(sh.scraped_loc,row.store_dir))
                outdf.to_csv(os.path.join(sh.scraped_loc,
                                          row.store_dir,
                                          row.fn[:-4]+'.csv'))
                    
                print(outdf.describe())

        else:
            print(f'{idx}: Not in targeted type.')
        idx += 1
    
def compare_records(row,sh):
    # first see if df version exists yet.
    if not has_been_scraped(sh, row.fn, row.store_dir):
        return 'PDF not downloaded yet'
    df = get_scraped(sh, row.fn, row.store_dir)
    ln = len(df.columns)
    df.columns = list(string.ascii_lowercase)[:ln]        
    #df.columns = ['a','b','c','d','e','f','g','h','i']
    if len(df)<4:
        return 'df is too short'
    try:
        df['CAS'] = df.f.str.strip().str.lstrip('0')
        df['Ing_clean'] = df.e.str.strip().str.lower()
        testcas = row.CASNumber.strip().lstrip('0')
        t = df[df.CAS==testcas].copy()  # row 'f' is CASNumber, h is %HFJ
        t.fillna(0,inplace=True)
    except:
        return f'unexpected structure of scraped df - num col{len(df.columns)}'
    try:
        t.h = t.h.astype('float')
    except:
        try:
            s = f'illegal value in PercentHFJob: {t.h.tolist()}'
        except:
            return f'unexpected structure of scraped df - num col{len(df.columns)}'            
        s = s.replace(',',';')
        return s
    t['hfcomp'] = np.isclose(row.PercentHFJob,t.h,atol=1e-06,rtol=1e-01)
    if len(t)== 0:
        ncas = len(df[df.CAS.notna()].CAS.tolist())
        print(f'\ntarget: {row.CASNumber}; {row.api10}; number CAS in df: {ncas}')
        #print(df.CAS.tolist())
        return f'CAS not found (nCAS: {ncas})'
    if len(t[t.hfcomp])>0:
        s = 'ing comp: '
        for i in t[t.hfcomp].Ing_clean.tolist():
            s += str(i == row.IngredientName.strip().lower())
            s += ' '
        #comp = t[t.hfcomp].Ing_clean==row.IngredientName.strip().lower()
        return f'Success ({s}): has CAS with matching %HFJ'
    return f'error: CAS without %HFJ {row.PercentHFJob}; {t.h.tolist()}'.replace(',',';')
        
def compare_single_disclosure_to_scraped(st_df=None,df=None):
    # and return summary         
    try:
        df['CAS'] = df.f.str.strip().str.lstrip('0').str.lower()
        df['Ing_clean'] = df.e.str.strip().str.lower()
    
        st_df.CASNumber = np.where(st_df.CASNumber=='MISSING',np.NaN,st_df.CASNumber)
        st_df.IngredientName = np.where(st_df.IngredientName=='MISSING',np.NaN,st_df.IngredientName)
        st_df['CAS'] = st_df.CASNumber.str.strip().str.lstrip('0').str.lower()
        st_df['ing_clean'] = st_df.IngredientName.str.strip().str.lower()
    
        # mg = pd.merge(st_df,df,on=['CAS','Ing_clean'],how='left',indicator=True)
        mg = pd.merge(st_df,df,on=['CAS'],how='outer',indicator=True)
        #print(mg[mg._merge=='left_only'][['CAS','ing_clean']].sort_values('CAS'))
        #print(mg[mg._merge=='right_only'][['CAS','Ing_clean']].sort_values('CAS'))
        return mg[mg._merge=='left_only'].CASNumber.unique().tolist()
    except:
        return ['non-standard scrape df']
    
def full_ST_compare(upklst = []):
    sh = Storage_handler()

    # res = pd.read_csv(r"C:\MyDocs\OpenFF\src\pdf-storage\tmp\SDWA_compare_results.csv",
    #                   quotechar='$')
    # ingk = res[res.result.str[:7]=='Success'].IngredientKey.unique().tolist()
    # upklst = repo_df[repo_df.IngredientKey.isin(ingk)].UploadKey.unique().tolist()
    upklst = pd.read_csv(r"C:\MyDocs\OpenFF\src\testing\tmp\ST_CO_teflon_upk.csv").UploadKey.tolist()
    print(len(upklst))
    # if upklst==[]:
    #     upklst = repo_df[repo_df.data_source=='SkyTruth'].UploadKey.unique().tolist()
    # upklst = upklst[:1]
    idx_df = pd.read_csv('./tmp/idx_by_fn.csv',quotechar='$',encoding='utf-8')
    idx_df.date = pd.to_datetime(idx_df.date,errors='coerce')
    outupk = []
    outres = []
    outlst = []
    for j,upk in enumerate(upklst):
        try:
            stdf = repo_df[repo_df.UploadKey==upk]
            api10 = stdf.APINumber.str[:10].unique()[0]
            for i,row in idx_df[idx_df.api10==api10].iterrows():
                scraped_df =  get_scraped(sh, row.fn, row.store_dir)
                ln = len(scraped_df.columns)
                scraped_df.columns = list(string.ascii_lowercase)[:ln]  
                #print(f' **{i}** {row.fn}, {row.store_dir}; {len(scraped_df)} by {ln}')
                result = compare_single_disclosure_to_scraped(stdf.copy(),scraped_df.copy())
                outupk.append(upk)
                outres.append(len(result))
                outlst.append(result)
            if j%100==0:
                print(j)
                pd.DataFrame({'UploadKey':outupk,
                              'n_no_match':outres,
                              'lst_cas':outlst}).to_csv('./tmp/st_cas_compare.csv')

        except:
            print(f'problem with {upk}, skipping...')
    pd.DataFrame({'UploadKey':outupk,
                  'n_no_match':outres,
                  'lst_cas':outlst}).to_csv('./tmp/st_cas_compare.csv')
    
def compare_scraped_with_SkyTruth(chk_fn):
    sh = Storage_handler()
    extr = fnie()
    me = mde()
    chklst = pd.read_csv(chk_fn)
    chklst['api10'] = chklst.APINumber.str[:10]
    chklst.date = pd.to_datetime(chklst.date)
    chklst.fillna(0,inplace=True) # to make detecting na easier
    print(chklst.columns)
    print(chklst.head())
    #print(sh.meta_df.columns)
    meta = sh.meta_df[['API Number:','Fracture Date','Fracture Date:','Job End Date:',
                       'fn','store_dir','status']].copy()
    meta = meta.rename({'API Number:':'APINumber'},axis=1)
    meta = meta[meta.APINumber.notna()]
    meta['api10'] = meta.APINumber.str.replace('-','').str[:10]
    meta['date'] = meta['Fracture Date']
    meta.date = np.where(meta.date.isna(),meta['Fracture Date:'],meta.date)
    meta.date = np.where(meta.date.isna(),meta['Job End Date:'],meta.date)
    meta.date = pd.to_datetime(meta.date,errors='coerce')
    
    mg = pd.merge(chklst,meta[['api10','date','fn','store_dir','status']],on=['api10','date'],how='left')
    #mg.to_csv('./tmp/temp.csv')
    print('start compare')
    with open('./tmp/temp.csv','w') as f:
        f.write('result,api10, date, CASNumber,fn,store_dir\n')
        for i,row in mg.iterrows():
            # if row.store_dir in ['dir_000','dir_001','dir_001','dir_001',
            #                      'dir_001','dir_001','dir_001','dir_001',]:
            res =compare_records(row,sh)
            f.write(f'{res},{row.api10},{row.date},{row.CASNumber},{row.fn},{row.store_dir}\n')
    
def compare_scraped_with_SkyTruth_v2(chk_fn):
    sh = Storage_handler()
    extr = fnie()
    me = mde()
    chklst = pd.read_csv(chk_fn)
    chklst['api10'] = chklst.APINumber.str[:10]
    chklst.date = pd.to_datetime(chklst.date)
    chklst.fillna(0,inplace=True) # to make detecting na easier
    print(chklst.columns)
    print(chklst.head())
    #print(sh.meta_df.columns)
    idx_df = pd.read_csv('./tmp/idx_by_fn.csv',quotechar='$',encoding='utf-8')
    idx_df.date = pd.to_datetime(idx_df.date,errors='coerce')
    
    
    mg = pd.merge(chklst,idx_df[['api10','date','fn','store_dir']],
                  on=['api10','date'],how='left')


    print('start compare')
    with open('./tmp/temp.csv','w') as f:
        f.write('result,api10, date, CASNumber,fn,store_dir,IngredientKey\n')
        for i,row in mg.iterrows():
            # if row.store_dir in ['dir_000','dir_001','dir_001','dir_001',
            #                      'dir_001','dir_001','dir_001','dir_001',]:
            res =compare_records(row,sh)
            f.write(f'{res},{row.api10},{row.date},{row.CASNumber},{row.fn},{row.store_dir},{row.IngredientKey}\n')
    
# def check_file_exists():
#     sh = Storage_handler()
#     #extr = fnie()
#     fn = '42501360280000-12192012-324-XTO EnergyExxonMobil.pdf'
#     sd = 'dir_000'
#     print(has_been_scraped(sh, fn, sd))

def make_idx_by_fn():
    # use master_df to create a df with fn api and date.
    # mostly to try to find FFV1 disclosures
    sh = Storage_handler()
    extr = fnie()

    fnlst = []
    dirlst = []
    apilst = []
    dtlst = []
    for i, row in sh.master_df.iterrows():
        api = extr.get_API(row.fn)
        dl = extr.parse_fndate_vers(extr.get_datestr(row.fn))
        # sometimes the date is ambiguous so more than one is returned
        for dt in dl:
            fnlst.append(row.fn)
            dirlst.append(row.store_dir)
            apilst.append(api)
            dtlst.append(dt)
    out = pd.DataFrame({'fn':fnlst,'store_dir':dirlst,
                        'api10':apilst,'date':dtlst})
    out.to_csv('./tmp/idx_by_fn.csv',quotechar='$',encoding='utf-8',index=False)
    return out

def make_FFV1_scrape_merge():
    idxfn = make_idx_by_fn()
    idxfn.date = pd.to_datetime(idxfn.date,errors='coerce')
    indf = pd.read_csv(r"C:\MyDocs\OpenFF\src\testing\tmp\FFV1_scrape_merge.csv",
                       dtype={'api10':str,'APINumber':str})
    indf.date = pd.to_datetime(indf.date,errors='coerce')
    out = pd.merge(indf,idxfn,on=['api10','date'],how='left',indicator=True)
    out = out[out._merge=='both']
    out = out[~(out.UploadKey.duplicated(keep=False))]
    out = out.drop('_merge',axis=1)
    out.to_csv('./tmp/FFV1_scrape_merge_master.csv',quotechar='$',encoding='utf-8',
               index=False)
    return out
    
if __name__ == '__main__':
    #t = make_FFV1_scrape_merge()

    #full_ST_compare()
    #df = get_metadata()    
    #get_metadata()
    # t.to_csv('./tmp/temp.csv',quotechar='$',encoding='utf-8')
    sh = scrape_to_dfs(idx_start=182000)
    
    #check_file_exists()
    #compare_scraped_with_SkyTruth_v2(r"C:\MyDocs\OpenFF\src\testing\tmp\ST_CO_teflon.csv")
    #make_idx_by_fn()