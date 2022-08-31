# -*- coding: utf-8 -*-
"""
Created on Sun Jan 23 17:30:34 2022

@author: Gary
"""
import camelot
from PyPDF2 import PdfFileReader    
import pandas as pd
  

class Metadata_extractor():
    def __init__(self):
        self.workfn = './tmp/workfile.pdf'
        self.workfnold = './tmp/workfile_old.pdf'
        self.file_meta_vars = ['Title','Author','Creator',
                               'Producer','CreationDate']
        self.init_file_meta_dic()

    def init_file_meta_dic(self):
        d = {}
        for v in self.file_meta_vars:
            d[v] = []
        others = ['OtherFields','fn','store_dir']
        for v in others:
            d[v] = []
        self.file_meta_dic = d

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
        tables = camelot.read_pdf(self.workfnold)
        t = tables[0].df
        print(t)
        return t[0][0]
    
    def get_tables_from_any_type(self,fn=''):
        if fn == '':
            fn = self.workfn
        try:
            t = camelot.read_pdf(fn,pages='1-end')
            lst = []
            for table in t:
                lst.append(table.df)
            return pd.concat(lst)
        except:
            return pd.DataFrame({'error':['data scraping not successful',
                                          'likely no tables back from Camelot']})

    def test_read(self,fn=''):
        if fn == '':
            fn = self.workfn
        t = camelot.read_pdf(fn,pages='1-end',process_background=False)
        print(t)
        lst = []
        for table in t:
            print(f'Table:\n {table.df}\n')
            lst.append(table.df)
        return pd.concat(lst)


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
            #print(out)
            return out
        else:
            print('other type')
            return pd.DataFrame()
            
    def get_meta_from_new_type(self):
        tables = camelot.read_pdf(self.workfn)
        out = tables[0].df
        out.columns = ['name','values']
        #index = out.name.tolist()
        out = out.set_index('name').T
        #print(out)
        return out
    

    def get_file_meta(self,fn,store_dir):
        pdf_toread = PdfFileReader(open(self.workfnold, 'rb'))
        pdf_info = pdf_toread.getDocumentInfo()
        self.file_meta_dic['fn'].append(fn)
        self.file_meta_dic['store_dir'].append(store_dir)
        for v in self.file_meta_vars:
            try:
                x = pdf_info['/'+v]
            except:                
                x = ''
            self.file_meta_dic[v].append(x)
        others = ''
        for v in pdf_info.keys():
            if not v[1:] in self.file_meta_vars:
                others += v+'; '
        self.file_meta_dic['OtherFields'].append(others)

        
if __name__ == '__main__':
    me = Metadata_extractor()
    t = me.test_read(fn=r"C:\MyDocs\OpenFF\src\pdf-storage\tmp\workfile2.pdf")

    #me.get_file_meta('tfn','tdir')
    #t = me.file_meta_dic
    #print(t)
    # t = pd.concat(test,sort=True)
    # t.to_csv('./tmp/temp.csv',quotechar='$',encoding='utf-8')