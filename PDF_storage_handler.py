# -*- coding: utf-8 -*-
"""
Created on Sun Jan  2 20:10:14 2022

@author: Gary
"""

import pandas as pd
import datetime
import os, shutil
import zipfile
import pdf_checks as pc


pdf = './tmp/05-001-10210-00-00-11242021 111047 AM-5512-Petro Operating Company LLC.pdf'
#repo_loc = 'f:/pdf-storage/'
repo_loc = 'e:/working/pdf-storage/PDFs/'
pdf_loc = repo_loc + ''
scraped_loc = 'e:/working/pdf-storage/scraping_results/'

# for 1T alone
#repo_loc = 'f:/pdf-storage/PDFs/'
#pdf_loc = repo_loc + ''
#scraped_loc = 'f:/pdf-storage/scraping_results/'

zip_dirs = ['FF_pdf_repo']

scraping_dir = 'scraping_results/'

testfn = '03-023-10675-00-00-4192013 13713 PM-1001-XTO EnergyExxonMobil.pdf'
fn2 = '42-173-35536-00-00-562013 31238 PM-791-Merit Energy Company.pdf'

class Storage_handler():    
    def __init__(self):
        self.repo_loc = repo_loc
        #self.pdf_loc = pdf_loc
        self.scraped_loc = scraped_loc
        
        self.masterfn = 'master_df.csv'
        self.master_df = pd.read_csv(os.path.join(self.repo_loc,self.masterfn),
                                      quotechar='$',encoding='utf-8',
                                      low_memory=False)
        self.meta_df_fn = 'meta_df.csv'
        try:
            self.meta_df = pd.read_csv(os.path.join(self.repo_loc,self.meta_df_fn),
                                       quotechar='$',encoding='utf-8',
                                       low_memory=False)
        except:
            self.meta_df = pd.DataFrame({'fn':[],'store_dir':[]})
            print('Starting meta_df from scratch')
            
        self.scraping_dir = os.path.join(self.repo_loc,scraping_dir)
        
        self.workfn = './tmp/workfile.pdf'
        self.max_per_dir = 10000
        self.verbose = True
        self.zip_dirs = zip_dirs
        self.get_dir_list()
        if self.verbose:
            print(f'{self.master_df.info()}')
            
            
    def concat_and_save_meta_df(self,newdf):
        self.meta_df = pd.concat([self.meta_df,newdf],sort=True)
        self.meta_df.to_csv(os.path.join(self.repo_loc,self.meta_df_fn),
                                   quotechar='$',encoding='utf-8',index=False)

    def get_meta_todo(self,num=100):
        out = pd.merge(self.master_df[['fn','store_dir']],
                       self.meta_df[['fn','store_dir']],
                       on=['fn','store_dir'],
                       how='outer',indicator=True)
        t = out[out._merge=='left_only'][['fn','store_dir']]
        print(f'Number of meta still on list: {len(t)}')
        return t[:num]
    
            
            
    
    def modification_date(self,filename):
        # used to fetch scraped date
        t = os.path.getmtime(filename)
        return datetime.datetime.fromtimestamp(t)


    def update_growing_dir(self):
        self.repo_dirs.sort()
        for d in self.repo_dirs:
            dname = os.path.join(self.repo_loc, d)
            dirsize = len(os.listdir(dname))
            if dirsize < self.max_per_dir:
                self.growing_dir = d
                if self.verbose:
                    print(f'Current growing_dir: {dname} with {dirsize} files')
                return
        raise Exception('No directories to add to. Use add_empty_dir')

    def get_from_zipped(self,fn,dirname):
        # copy file from repository to temp work file
        for zname in self.zip_dirs:
            try:
                with zipfile.ZipFile(os.path.join(self.repo_loc,zname+'.zip')) as z:           
                    #print(f'in zipfile {zname}')
                    fullname = zname+'/'+dirname+'/'+fn
                    #lst = z.namelist()
                    #print(len(lst))
                    #print(fullname in lst)
                    with z.open(fullname) as f:
                        stuff = f.read()
                        with open(self.workfn,'wb') as out:
                            out.write(stuff)
                return True
            except:
                if self.verbose:
                    print(f'{dirname}, {fn} not found in {zname}.zip')
        return False
    

    def get_from_unzipped(self,fn,dirname):
        try:
            shutil.copy2(os.path.join(self.repo_loc,dirname,fn),
                         self.workfn)
            if self.verbose:
                print('Success!')
            return True
        except:
            if self.verbose:
                print(f'Problem fetching from unzipped: {dirname} {fn}')
            return False
        
    def fetch_to_work_file(self,fn,dirname):
        flag = False
        if dirname in self.repo_dirs:
            flag = self.get_from_unzipped(fn,dirname)
        else:
            flag = self.get_from_zipped(fn, dirname)    
        if not flag:
            print(f'File not found: {dirname}, {fn}')
                
    def add_empty_dirs(self,howmany=30,startnum = 0):
        for i in range(startnum,howmany+startnum):
            name = 'dir_'+ str(i).zfill(3)
            path = os.path.join(self.repo_loc,name)
            os.mkdir(path,mode=777)
        
    def get_dir_list(self):
        self.repo_dirs = []
        with os.scandir(self.repo_loc) as it:
            for entry in it:
                if entry.is_dir():
                    if entry.name[:4]=='dir_':
                        self.repo_dirs.append(entry.name)
        if self.verbose:
            print(f'Current repository directories: \n{self.repo_dirs}')
            
    def show_zipped_dirs(self):
        for zdir in self.zip_dirs:
            print(f'Directories in {zdir}:')
            with zipfile.ZipFile(self.repo_loc+zdir+'.zip','r') as z:
                for item in z.infolist():
                    if item.is_dir():
                        print(item.filename)
       
    
    def pdf_checkup(self,fn):
        isFull = False
        try:
            isFull = pc.isFullPdf(fn)
        except:
            return False
        return isFull
        
    def add_pdf_to_repo(self,ffn):
        #self.update_growing_dir()
        self.update_growing_dir()
        shutil.copy2(ffn,self.repo_loc+self.growing_dir+'/')
        
    def move_into_storage(self,source_dir):
        mstr_dic = {}
        print('making master dictionary')
        for i,row in sh.master_df.iterrows():
            mstr_dic[row.fn] = row.fhash
        
        dirlst = os.listdir(source_dir)
        print(f'Number of files in sourcedir: {len(dirlst)}')
        
        fnlst = []
        fhashlst = []
        sdlst = []
        sdatelst = []
        
        for i,fn in enumerate(dirlst):
            if i%100==0:
                print(f'working on fn # {i}')
            if fn[-4:]!='.pdf':
                print(f'non pdf ignored: {fn}')
                continue
            ffn = os.path.join(source_dir,fn)
            if self.pdf_checkup(ffn):
                if not fn in mstr_dic.keys():
                    self.update_growing_dir()
                    fnlst.append(fn)
                    fhashlst.append(pc.fetch_hash(ffn))
                    sdatelst.append(self.modification_date(ffn))
                    sdlst.append(self.growing_dir)
                    self.add_pdf_to_repo(ffn)
                else:
                    if not pc.fetch_hash(ffn)==mstr_dic[fn]:
                        print('same fn, diff hash!')
            else:
                print(f'PDF failed checkup {fn}')
        print(f'Adding {len(fnlst)} new PDFs to storage and master_df')
        new = pd.DataFrame({'fn':fnlst,'store_dir':sdlst,
                             'scrape_date':sdatelst,'fhash':fhashlst})
        self.master_df = pd.concat([self.master_df,new],sort=True)
        self.master_df.to_csv(os.path.join(self.repo_loc,self.masterfn),
                              quotechar='$',encoding='utf-8',
                              index=False)
                
        
    #### DON't REMOVE - keep for reference
    ## the following were used to get initial scrape-set organized
    # import glob
    # def prep_old_to_new(self):#,old_dir,old_lst):
    #     odir = "C:/MyDocs/sandbox/data/O&G/scrape_FF_pdfs/"
    #     odir = "E:/workingscrape/"

    #     allpdf = []
    #     isFull = []
    #     fhash = []
    #     modDate = []
    #     for filename in glob.iglob(odir + '**/*.pdf', recursive=True):
    #         allpdf.append(filename)
    
    #     for i,fn in enumerate(allpdf):
    #         if i%1000==0:
    #             print(i)
    #         isFull.append(self.pdf_checkup(fn))
    #         fhash.append(pc.fetch_hash(fn))
    #         modDate.append(self.modification_date(fn))
    #     out = pd.DataFrame({'filename':allpdf,'isFull':isFull,'fhash':fhash,
    #                         'scrape_date':modDate})
    #     out.to_csv('./tmp/raw.csv',quotechar='$',encoding='utf-8',index=False)
    #     return out
    
    # def move_old_to_new(self):
    #     mdf = pd.read_csv('e:/workingscrape/moving_df.csv',quotechar='$',encoding='utf-8')
    #     out = mdf[mdf.to_transfer].copy().sort_values('scrape_date')
    #     store_dir = []
    #     cnt = 0
    #     for i,row in out.iterrows():
    #         print('  ',i,cnt,row.fn)
    #         cnt += 1
    #         if cnt%1000==0:
    #             print(cnt)
    #         #if cnt>50:
    #         #    break
    #         self.add_pdf_to_repo(row)
    #         store_dir.append(self.growing_dir)
    #     out.to_csv(self.masterfn,quotechar='$',encoding='utf-8')
    #     return out
        
    # def record_dir_in_master(self):
    #     allpdf = []
    #     for filename in glob.iglob(self.repo_loc + '**/*.pdf', recursive=True):
    #         allpdf.append(filename)
    #     pdfl = []
    #     dirl = []
    #     for fn in allpdf:
    #         out = fn.split('\\')
    #         pdfl.append(out[-1])
    #         dirl.append(out[-2])
    #     return pdfl, dirl
    
    def upk_to_workfile(self,upk):
        t = pd.read_csv(r"E:\working\pdf-storage\PDFs\FFV1_scrape_merge_master.csv",
                quotechar='$',encoding='utf-8')
        fn = t[t.UploadKey==upk].fn.iloc[0]
        store_dir = t[t.UploadKey==upk].store_dir.iloc[0]
        self.fetch_to_work_file(fn,store_dir)
        print(t[t.UploadKey==upk].T)
        
    def api10_to_upk(self,api10):
        t = pd.read_csv(r"E:\working\pdf-storage\PDFs\FFV1_scrape_merge_master.csv",
                quotechar='$',encoding='utf-8',dtype={'api10':'str',
                                                      'APINumber':'str'})
        print(t[t.api10==api10][['UploadKey','date','fn','store_dir']])
        
    def get_scraped(self,fn,store_dir):
        return pd.read_csv(os.path.join(self.scraped_loc,
                                        store_dir,
                                        fn[:-4]+'.csv'))

    def upk_to_scraped_df(self,upk):
        t = pd.read_csv(r"E:\working\pdf-storage\PDFs\FFV1_scrape_merge_master.csv",
                quotechar='$',encoding='utf-8')
        fn = t[t.UploadKey==upk].fn.iloc[0]
        store_dir = t[t.UploadKey==upk].store_dir.iloc[0]
        t = self.get_scraped(fn,store_dir)
        t.to_csv('./tmp/upk_to_scraped_df.csv')
        return t
    
if __name__ == '__main__':
    sh = Storage_handler()
    #sh.api10_to_upk('0512321884')
    upk = 'SKY_pdf_49250_13'
    sh.upk_to_workfile(upk)
    t = sh.upk_to_scraped_df(upk)
    #t = sh.move_into_storage(r"E:\working\pdf-storage\to_add_to_pdf_repo\out_10_mar2022")
    #sh.add_empty_dirs(startnum=19,howmany=10)
    # cntr = 0
    # for i,row in sh.master_df.iterrows():
    #     cntr += 1
    #     if cntr%1000==0:
    #         print(cntr)
    #     if cntr>10:
    #         break
    #     sh.fetch_to_work_file(row.fn,row.store_dir)
    #     #print(f'{row.store_dir}, {row.fn}')
