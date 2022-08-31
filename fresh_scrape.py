# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 12:26:05 2022

@author: Gary

These routines are used to create a new data set from the PDFs, using the
FFV1 disclosures as a guide.
"""

import pandas as pd
import numpy as np
import os
#import string
import re

from PDF_storage_handler import Storage_handler
from metadata_extractor import Metadata_extractor 

meta_xlate = {'Fracture Date': 'date', 
              'State:': 'StateName',
              'County:': 'CountyName',
              'API Number:': 'APINumber',
              'Operator Name:': 'OperatorName',
              'Well Name and Number:': 'WellName',
              'Longitude:': 'Longitude', 
              'Latitude:': 'Latitude', 
              'Long/Lat Projection:': 'Projection', 
              'Production Type:': 'ProductionType', 
              'True Vertical Depth (TVD):': 'TVD', 
              'Total Water Volume (gal)*:': 'TotalBaseWaterVolume', 
              #'': 34530, 
              'Fracture Date:': 'date', 
              'Last Fracture Date': 'date', 
              'County/Parish': 'CountyName', 
              'Total Water Volume* (gal):': 'TotalBaseWaterVolume', 
              #'Fracture Date\nState:\nCounty:\nAPI Number:\nOperator Name:': 12, 
              #'Longitude:\nLatitude:\nLong/Lat Projection:\nProduction Type:': 12, 
              'Operator Name': 'OperatorName', 
              'County/Parish:': 'CountyName', 
              'True Vertical\nDepth (TVD):': 'TVD', 
              #'Total Chemical\nMass (lbs)*:': 123, 
              'Total Water\nVolume (gal)*:': 'TotalBaseWaterVolume', 
              #'Total\nChemical\nMass (lbs)*:': 16, 
              #'Total\nChemical\nVolume (gal)*:': 13, 
              #'Well Name and': 2, 
              #'Number:': 8, 
              'True Vertical Depth\n(TVD):': 'TVD', 
              'Total Water Volume\n(gal)*:': 'TotalBaseWaterVolume', 
              #'-102.860493': 1, '47.732709': 1, 
              #'Fracture Date\nState:\nCounty:\nAPI Number:': 4, 
              #'Well Name and Number:\nLongitude:\nLatitude:\nLong/Lat Projection:\nProduction Type:': 4, 
              #'Fracture Date\nState:\nCounty:\nAPI Number:\nOperator Name:\nWell Name and Number:\nLongitude:\nLatitude:': 2, 
              #'Strobeck 27-34 #3TFH': 1, 
              'Fracture\nDate': 'date', 
              'API': 'APINumber', 
              'Operator': 'OperatorName', 
              #'Name:': 3, 
              'Well Name': 'WellName', 
              #'and': 3, 
              #'Long/Lat': 3, 
              'Projection:': 'Projection', 
              #'Fracture Date\nState:\nCounty:\nAPI Number:\nOperator Name:\nWell Name and Number:': 3, 
              'Longitude': 'Longitude', 
              'Latitude': 'Latitude', 
              #'Southman Canyon 923-31L': 1, 
              #'3305303960': 1, 
              #'Zenergy Inc.': 1, 
              #'7/19/2011': 1, 
              'True\nVertical\nDepth\n(TVD):': 'TVD', 
              #'Total\nChemical\nVolume\n(gal)*:': 3, 
              #'Total': 1, 
              #'Additive': 1, 
              #'Mass (lbs):': 1, 
              #'42-445-32340': 1, 
              #'Silver Creek Oil & Gas, LLC': 2, 
              ####'Total Oil Volume (gal)*:': 1, 
              #'4/1/2012': 1, 
              #'10/1/2012': 1, 
              #'Texas': 1, 
              #'Upton': 1, 
              #'GV 44-32NEH': 1, 
              #'42-405-30503': 1, 
              #'42-135-41671': 1, 
              #'3305304206': 1, 
              #'12/11/2012': 1
              }


FFV1_scrape_df_fn = 'FFV1_scrape_merge_master.csv'

# lifted from cas_tools
def is_valid_CAS_code(cas):
    """Returns boolean.
    
    Checks if number follows strictest format of CAS registry numbers:
        
    - three sections separated by '-', 
    - section 1 is 2-7 digits with no leading zeros, 
    - section 2 is two digits (no dropping leading zero),
    - section 3 (check digit) is just one digit that satisfies validation algorithm.
    - No extraneous characters."""
    try:
        for c in cas:
            err = False
            if c not in '0123456789-': 
                err = True
                break
        if err: return False
        lst = cas.split('-')
        if len(lst)!=3 : return False
        if len(lst[2])!=1 : return False # check digit must be a single digit
        if lst[0][0] == '0': return False # leading zeros not allowed
        s1int = int(lst[0])
        if s1int > 9999999: return False
        if s1int < 10: return False
        s2int = int(lst[1])
        if s2int > 99: return False
        if len(lst[1])!=2: return False # must be two digits, even if <10

        # validate test digit
        teststr = lst[0]+lst[1]
        teststr = teststr[::-1] # reverse for easy calculation
        accum = 0
        for i,digit in enumerate(teststr):
            accum += (i+1)*int(digit)
        if accum%10 != int(lst[2]):
            return False
        return True
    except:
        # some other problem
        return False

# lifted from cas_tools
def cleanup_cas(cas):
    """Returns string.
    
    Removes extraneous characters and adjusts zeros where needed:
        
    - need two digits in middle segment and no leading zeros in first.
    Note that we DON'T check CAS validity, here. Just cleanup. 
    """
    cas = re.sub(r'[^0-9-]','',cas)
    lst = cas.split('-') # try to break into three segments
    if len(lst) != 3: return cas # not enough pieces - return filtered cas
    if len(lst[2])!= 1: return cas # can't do anything here with malformed checkdigit
    if len(lst[1])!=2:
        if len(lst[1])==1:
            lst[1] = '0'+lst[1]
        else:
            return cas # wrong number of digits in chunk2 to fix here
    lst[0] = lst[0].lstrip('0')
    if (len(lst[0])<2 or len(lst[0])>7): return cas # too many or two few digits in first segment
    
    return f'{lst[0]}-{lst[1]}-{lst[2]}'


def get_master_df_guide(sh):
    out = pd.read_csv(sh.repo_loc+FFV1_scrape_df_fn,quotechar='$',encoding='utf-8',
                      dtype = {'APINumber':str,
                               'api10':str,
                               })
    return out

def get_scraped_df(sh,row):
    df_loc = os.path.join(sh.scraped_loc,row.store_dir,row.fn[:-4]+'.csv')
    try:
        out = pd.read_csv(df_loc, dtype='str')
        return out
    except:
        return pd.DataFrame()

def num_CAS_in_lst(lst):
    cnt = 0
    for s in lst:
        if type(s)==str:
            clean = cleanup_cas(s)
            if is_valid_CAS_code(clean):
               cnt += 1
    return cnt

def translate_meta_names(innames):
    out = []
    for inname in innames:
        if not isinstance(inname,str):
            out.append('unk')
            continue
        if inname in meta_xlate:
            out.append(meta_xlate[inname])
        else:
            out.append('unk-'+inname)
    return out

def correct_meta_problems(indf):
    # problem 1: TVD gets put into ProductionType and TBWV gets moved to TVD
    #   happens in a small percentage of disclosures
    df = indf.copy()
    check = df['ProductionType'].iloc[0]
    if '\n' in check:
        spl = check.split('\n')
        tbwv = df['TVD'].iloc[0]
        #print(tbwv)
        df.TVD = spl[1]
        df.ProductionType = spl[0]
        df['TotalBaseWaterVolume'] = tbwv
        
    
    # remove commas from TBVW, TVD
    df.TotalBaseWaterVolume = df.TotalBaseWaterVolume.str.replace(',','')
    df.TVD = df.TVD.str.replace(',','')
    
    # remove dashes from APINumber
    df.APINumber = df.APINumber.str.replace('-','')
    
    return df

def get_meta_from_old_type(df):
    if 'Hydraulic Fracturing Fluid Product Component I' in df[1][0]:
        misplaced = df[1][1].split("\n")
        #print(f'type 1: {misplaced}')
        names = df[2].tolist()[1:]
        names = names[:names.index('Supplier')]
        values = df[3].tolist()[1:]
        values = values[:values.index('Purpose')]
        for i,label in enumerate(misplaced):
            names[i+1] = label
            
        out = pd.DataFrame({'values':values})
        out = out.T
        #out.columns = names
        out.columns = translate_meta_names(names)
        out = correct_meta_problems(out)
        #print(out)
        return (out,df[1][0])
    elif 'Fracture' in df[1][0]:
        names = df[1].tolist()[0:]
        names = names[:names.index('Trade Name')]
        allnames = ''
        for cell in names:
            c = str(cell)
            if c == 'nan': c = ''
            else: c += '\n'
            allnames += c
        #print(f'in allnames: {allnames}')
        finalnames = allnames.split("\n")
        #print(finalnames)
        values = df[2].tolist()[0:]
        values = values[:values.index('Supplier')]
        #print(values)
        out = pd.DataFrame({'values':values})
        out = out.T
        out.columns = translate_meta_names(finalnames[:-1])
        out = correct_meta_problems(out)
        #print(out)
        return (out,df[1][0].split()[0])
        #print('other type')
    else: # unknown format so pass back an empty dataframe
        return (pd.DataFrame(),df[1][0].split()[0])
        
# def get_meta_from_new_type():
#     out = tables[0].df
#     out.columns = ['name','values']
#     #index = out.name.tolist()
#     out = out.set_index('name').T
#     #print(out)
#     return out
    
def get_chem_header_index(df):
    hdr_names = ['trade name','chemical abstr','purpose','ingredient','supplier','% by mass']
    bestrow = 0
    bestrowmax = 0
    for i,row in df.iterrows():
        row = row.tolist()
        cnt = 0
        cells = []
        for cell in row:
            cells.append(cell)
            if not cell in [np.NaN,None,'']:            
                c = str(cell).lower()
                #print(c)
                for name in hdr_names:
                    if c in name: 
                        cnt += 1
                        #print(c,name)
        #print(f'i: {i}; cnt = {cnt}; {cells}')
                        
        if cnt>bestrowmax:
            #print(f'new row winner {cnt} {i}')
            bestrowmax = cnt
            bestrow = i
    #print(f'Best row ={bestrowmax} @ {bestrow}')
    return bestrow

def test1():
    # how many of these 40000 disclosures have CAS numbers in col '4'?
    colname = '4'
    sh = Storage_handler()
    df = get_master_df_guide(sh)
    good = 0
    total = 0
    for i,row in df.iterrows():
        if i%1000==0:
            print(i)
        total += 1
        res = get_scraped_df(sh,row)
        if len(res)>0:
            try:
                lst = res[colname].tolist()
                if num_CAS_in_lst(lst)>3:
                    good += 1
                
            except:
                pass
                #print('trouble with the df')
    print(f'Total {total}, with > 3 CAS: {good}')
    
def test1_shift():
    # how many of these disclosures have column shifts in CAS numbers?
    colname = '4'
    sh = Storage_handler()
    df = get_master_df_guide(sh)
    good = 0
    total = 0
    # first, label pages and concatante to single df
    concat_lst = []
    for i,row in df.iterrows():
        #print(f'Row: {row.UploadKey}')
        if i%100==0:
            print(i)
        total += 1
        res = get_scraped_df(sh,row)
        
        try:
            res.columns = range(res.shape[1])
            res['newpage'] = res[0]=='0'
        except: # having problems so skip this one
            print(f'Cant make rows for {row.fn}')
            continue
        res = res.fillna('')
        page = []
        currpage = -1
        for i,srow in res.iterrows():
            if srow.newpage==True:
                currpage +=1
            page.append(currpage)
        res['page'] = pd.Series(page)
        res['UploadKey'] = row.UploadKey
        #res.to_csv('./tmp/temp.csv')
        if len(res)>0:
            concat_lst.append(res)
        else:
            print(f'No rows for {row.fn}. Skipping.')

    big = pd.concat(concat_lst,sort=True)
    big.to_csv('./tmp/test1_shift.csv',quotechar='$',encoding='utf-8')
    return big
    #         tally = {}
    #         for col in res.columns.tolist():
    #             tally[col] = 
    #         try:
    #             lst = res[colname].tolist()
    #             if num_CAS_in_lst(lst)>3:
    #                 good += 1
                
    #         except:
    #             pass
    #             #print('trouble with the df')
    # print(f'Total {total}, with > 3 CAS: {good}')


def test2():
    # Make a metadata frame for all fetchable disclosures
    sh = Storage_handler()
    df = get_master_df_guide(sh)
    good = 0
    other = 0
    olist = []
    total = 0
    alldisc = []
    for i,row in df.iterrows():
        #print(i)
        if i%100==0:
            print(i)
        total += 1
        try:
            res = get_scraped_df(sh,row)
            res.columns = range(res.shape[1])
            res = res.drop(0,axis=1)
            res.to_csv('./tmp/temp.csv')
            if len(res)>0:
                try:
                    t = get_meta_from_old_type(res)
                    tt = t[0]
                    if len(tt)>0:
                        good += 1
                        tt = tt.filter(['APINumber','date','OperatorName',
                                        'Latitude','Longitude','Projection',
                                        'WellName','StateName','CountyName',
                                        'ProductionType','TVD',
                                        'TotalBaseWaterVolume'],axis=1)
                        #print(tt)
                        tt['UploadKey'] = row.UploadKey
                        alldisc.append(tt)
                    else:
                        other += 1
                    olist.append((t[1],row.fn,row.store_dir))
                except:
                    pass
        except:
            pass
    print('Now concat')
    final = pd.concat(alldisc,sort=True)
    print(f'Total {total}, good: {good}, other: {other}')
    #return olist
    final.to_csv('./tmp/fresh_meta.csv',quotechar='$',
             encoding='utf-8',index=False)
    return final

def test3(idx=16):
    # check why data not scraped from test2 olist
    sh = Storage_handler()
    me = Metadata_extractor()
    #testdf = pd.read_csv('./tmp/test2_errorlst.csv',quotechar='$',encoding='utf-8')  
    testdf = pd.read_csv(sh.repo_loc+FFV1_scrape_df_fn,quotechar='$',encoding='utf-8')  

    for i in range(idx,idx+1):   

        fn = testdf.iloc[i].fn
        #stat = testdf.iloc[i].status
        store_dir = testdf.iloc[i].store_dir
        #print(stat)
        sh.fetch_to_work_file(fn, store_dir)
        t = me.test_read()
    return t

def test4():
    # Identify the chemical header row
    sh = Storage_handler()
    df = get_master_df_guide(sh)
    good = 0
    # other = 0
    # olist = []
    total = 0
    namedic = {}

    for i,row in df[1:1000].iterrows():
        #print(i)
        if i%100==0:
            print(i)
        total += 1
        try:
            res = get_scraped_df(sh,row)
            res.columns = range(res.shape[1])
            res = res.drop(0,axis=1)
            res.to_csv('./tmp/temp.csv')
            if len(res)>0:
                #try:
                t = get_chem_header_index(res)
                #print(t)
                if t>0:
                    good += 1
                    lst = res.iloc[t].tolist()
                    #print(f'in lst: {lst}')
                    for c in lst:
                        if c in namedic:
                            namedic[c] += 1
                        else:
                            namedic[c] = 1

                # except:
                #     pass
                
        except:
            pass
    print(f'Total {total}, good: {good}')
    #print(namedic)

def test5():
    # what are the metadata names so we can standardize them
    sh = Storage_handler()
    df = get_master_df_guide(sh)
    namedic = {}
    for i,row in df.iterrows():
        #print(i)
        if i%100==0:
            print(i)
        try:
            res = get_scraped_df(sh,row)
            res.columns = range(res.shape[1])
            res = res.drop(0,axis=1)
            #res.to_csv('./tmp/temp.csv')
            if len(res)>0:
                try:
                    t = get_meta_from_old_type(res)
                    if len(t[0])>0:
                        #print(t[0].columns)
                        lst = t[0].columns.tolist()
                        #print(f'in lst: {lst}')
                        for c in lst:
                            if c in namedic:
                                namedic[c] += 1
                            else:
                                namedic[c] = 1
                except:
                    pass
        except:
            pass
    print(namedic)
    return namedic


def add_row_type(t):
    t['row_type'] = '?'
    c1 = t.TradeName==''
    c2 = t.Purpose==''
    c3 = t.Supplier==''
    c4 = t.IngredientName==''
    c5 = t.CASNumber==''
    c6 = t.PercentHighAdditive==''
    c7 = t.PercentHFJob==''
    c8 = t.Comment==''
    t.row_type = np.where(c1&c2&c3&c4&c5&c6&c7&c8,'empty',t.row_type)
    t.row_type = np.where((~c4)&c1&c2&c3&c5&c6&c7,'only_Ing',t.row_type)
    t.row_type = np.where((~c1)&c2&c3&c4&c5&c6&c7,'only_TN',t.row_type)
    return t

def correct_zero_problem(t):
    # In >1000 disclosures, '0' is used as a placeholder for empty.
    # where there are '0's in TN, Sup and Pur, Ing and CAS, replace with '' (1000 disc)
    c1 = t.TradeName=='0' 
    c2 = t.Purpose=='0' 
    c3 = t.Supplier=='0'
    c4 = t.IngredientName=='0'
    c5 = t.CASNumber=='0'
    #c = c1&c2&c3
    t.TradeName = np.where(c1,'',t.TradeName)
    t.Supplier = np.where(c3,'',t.Supplier)
    t.Purpose = np.where(c2,'',t.Purpose)
    t.IngredientName = np.where(c4,'',t.IngredientName)
    t.CASNumber = np.where(c5,'',t.CASNumber)
    return t

def extend_tn_supp_purp(t):
    exTN = []
    exSup = []
    exPur = []
    #last_was_start = False
    last_tn = ''
    last_pur = ''
    last_sup = ''
    for i,row in t.iterrows():
        #print(f'last: {last_tn}, curr: {row.TradeName},type: <{row.row_type}>')
        #print(type(row.TradeName))
        if (row.TradeName!='')|(row.Purpose!='')|(row.Purpose!=''):
            # if row.row_type=='only_TN': # don't use as next
            #     exTN.append(row.TradeName)
            #     curr_tn = np.NAN
            #     continue
            exTN.append(row.TradeName)
            exSup.append(row.Supplier)
            exPur.append(row.Purpose)
            last_tn = row.TradeName
            last_pur = row.Purpose
            last_sup = row.Supplier
            #last_was_start = True
        else:
            #print(' in else')
            if row.row_type=='empty':
                exTN.append('')
                exSup.append('')
                exPur.append('')
                last_tn = ''
                last_pur = ''
                last_sup = ''
                #print('    > empty')
            else:
                exTN.append(last_tn)
                exSup.append(last_sup)
                exPur.append(last_pur)
    out = pd.merge(t,pd.DataFrame({'row_num':t.row_num.tolist(),
                                    'exTN':exTN,
                                    'exPur':exPur,
                                    'exSup':exSup}),
                    on='row_num',how='left')
    #print(out.columns)
    return out

def heal_new_page_breaks(df):
    cols_to_check = ['TradeName','Supplier','Purpose','IngredientName','Comment']
    t = df.copy()
    # c1 = t.TradeName==''
    # c2 = t.Purpose==''
    # c3 = t.Supplier==''
    # c4 = t.IngredientName==''
    c5 = t.CASNumber==''
    c6 = t.PercentHighAdditive==''
    c7 = t.PercentHFJob==''
    changed = []
    for i,row in t[(t.newpage==True)&c6&c7&c5].iterrows():
        if row.row_type=='empty':
            continue
        prev_row = row.row_num-1
        for col in cols_to_check:
            if len(row[col])>0:
                t[col].iloc[prev_row] = t[col].iloc[prev_row]+' '+t[col].iloc[row.row_num]
                t.row_type.iloc[row.row_num] = 'to_erase'
                t.row_type.iloc[prev_row] = 'healed'
                changed.append(col)
        if len(changed)>0:
            print(f'Healed : {changed}')
    return t[~(t.row_type=='to_erase')]

def drop_new_lines(df, cols = ['TradeName','Supplier','IngredientName','Purpose',
                               'CASNumber','Comment']):
    t = df.copy()
    for col in cols:
        t[col] = t[col].str.replace('\n',' ')
    return t

def test6():
    # Make the chem rec df

    sh = Storage_handler()
    df = get_master_df_guide(sh)
    #good = 0
    # other = 0
    # olist = []
    total = 0
    alldisc = []
    for i,row in df.iterrows():
        #print(i)
        if i%100==0:
            print(i)
        total += 1
        try:
            res = get_scraped_df(sh,row)
            #print(f'Size = {res.shape[1]}')
            res.columns = range(res.shape[1])
            res['newpage'] = res[0]=='0'
            res = res.drop(0,axis=1)
            res = res.fillna('')
            
            res.to_csv('./tmp/temp.csv')
            if len(res)>0:
                try:
                    t = get_chem_header_index(res)
                    if t>0:
                        tt = res.iloc[t+1:].copy()
                        tt.reset_index(drop=True,inplace=True)
                        tt.reset_index(inplace=True)
                        try:
                            tt.columns = ['row_num',
                                          'TradeName','Supplier','Purpose',
                                          'IngredientName','CASNumber','PercentHighAdditive',
                                          'PercentHFJob','Comment','newpage']
                        except:
                            tt.to_csv('./tmp/temp1.csv')
                            print('exception trying to name columns')
                            print(row.fn,row.store_dir)
                        tt = drop_new_lines(tt)
                        tt = correct_zero_problem(tt)
                        tt['UploadKey'] = row.UploadKey
                        tt['row_position'] = ''
                        tt.row_position = np.where(tt.row_num==0,'top',tt.row_position)
                        tt.row_position = np.where(tt.row_num==tt.row_num.max(),
                                                    'bottom',tt.row_position)
                        tt = add_row_type(tt)
                        tt = heal_new_page_breaks(tt)
                        tt = extend_tn_supp_purp(tt)
                        alldisc.append(tt)
                except:
                    pass
                    
        except:
            pass
    print('Now concat')
    final = pd.concat(alldisc,sort=True)[['row_num',
                                          'row_position','row_type','newpage',
                                          'UploadKey',
                                          'TradeName','Supplier','Purpose',
                                          'exTN','exSup','exPur',
                                          'IngredientName','CASNumber',
                                          'PercentHighAdditive','PercentHFJob',
                                          'Comment']]
    final.to_csv('./tmp/fresh_chem_concat.csv',quotechar='$',
             encoding='utf-8',index=False)
    return final

    #print(namedic)
    
if __name__ == '__main__':
    #t = test6()
    t = test1_shift()
    

