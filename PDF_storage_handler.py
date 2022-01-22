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
repo_loc = 'e:/pdf-storage/'
#repo_loc = './tmp/FF_pdf_repo/'
zip_dirs = ['FF_pdf_repo']

testfn = '03-023-10675-00-00-4192013 13713 PM-1001-XTO EnergyExxonMobil.pdf'
fn2 = '42-173-35536-00-00-562013 31238 PM-791-Merit Energy Company.pdf'

class Storage_handler():    
    def __init__(self):
        self.repo_loc = repo_loc
        self.masterfn = 'master_df.csv'
        self.master_df = pd.read_csv(os.path.join(self.repo_loc,self.masterfn),
                                      quotechar='$',encoding='utf-8')
        self.workfn = './tmp/workfile.pdf'
        self.max_per_dir = 10000
        self.verbose = True
        self.zip_dirs = zip_dirs
        self.get_dir_list()
        if self.verbose:
            print(f'{self.master_df.info()}')

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
                    with z.open(zname+'/'+dirname+'/'+fn) as f:
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
    
    def pdf_checkup(self,fn):
        isFull = False
        try:
            isFull = pc.isFullPdf(fn)
        except:
            return False
        return isFull
        
    def add_pdf_to_repo(self,row):
        #self.update_growing_dir()
        self.growing_dir = 'dir_001'
        shutil.copy2(row.filename,self.repo_loc+self.growing_dir+'/')
                
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
    
if __name__ == '__main__':
    sh = Storage_handler()
    cntr = 0
    for i,row in sh.master_df.iterrows():
        cntr += 1
        if cntr%1000==0:
            print(cntr)
        if cntr>100000:
            break
        sh.fetch_to_work_file(row.fn,row.store_dir)
        #print(f'{row.store_dir}, {row.fn}')
