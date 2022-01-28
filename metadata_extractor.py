# -*- coding: utf-8 -*-
"""
Created on Sun Jan 23 17:30:34 2022

@author: Gary
"""
import camelot
import pandas as pd
  

class Metadata_extractor():
    def __init__(self):
        self.workfn = './tmp/workfile.pdf'
        pass
        
    def get_date(self):    
        tables = camelot.read_pdf(self.workfn)
        t = tables[0].df.reset_index(drop=True)
        labels = [t[1][1], # New type and old type,v1
                  t[2][1], # New type and old type,v1
                  t[1][0]] # old type, v2    try:
        for i,l in enumerate(labels):
            try:
                date = pd.to_datetime(l)
                print(f'   Type = {i}')
                return date
            except:
                pass
        return '?'
        
    #print(get_date())
    
    def show_meta(self):
        tables = camelot.read_pdf(self.workfn)
        t = tables[0].df
        print(t)
        return t[0][0]
    
    def get_meta_from_old_type(self):
        tables = camelot.read_pdf(self.workfn)
        t = tables[0].df
        if 'Hydraulic Fracturing Fluid Product Component I' in t[0][0]:
            misplaced = t[0][1].split("\n")
            #print(f'type 1: {misplaced}')
            names = t[1].tolist()[1:]
            names = names[:names.index('Supplier')]
            values = t[2].tolist()[1:]
            values = values[:values.index('Purpose')]
            for i,label in enumerate(misplaced):
                names[i+1] = label
                
            out = pd.DataFrame({'values':values})
            out = out.T
            out.columns = names
    #         print(out)
            return out
        else:
            print('other type')
        
        
