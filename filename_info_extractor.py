# -*- coding: utf-8 -*-
"""
Created on Wed Jan 19 19:10:10 2022

@author: Gary

Used to pull different types of info just from the filename of the pdf.
"""

class Filename_info_extractor():
    def __init__(self):
        y = range(1950,2028)
        self.years = []
        for i in y:
            self.years.append(str(i))
            

    def has_year(self,s):
        #print(s)
        if len(s)>=6:
            if len(s)<=8:
                if s[-4:] in self.years:
                    return True
        return False
    
    def get_filetype(self,s):
        s = s.replace(' ','-')
        l = s.split('-')
        if self.has_year(l[1]):
            return 'old'
        if len(l)>5:
            if self.has_year(l[5]):
                return 'new'
            if self.has_year(l[3]):
                return 'dashed_old'
            else:
                print(s)
                return '?'
    
    def return_ftype(self,row):
        return self.get_filetype(row.fn)
    
    def get_datestr(self,s):
        s = s.replace(' ','-')
        l = s.split('-')
        if self.has_year(l[1]):
            return str(l[1])
        if self.has_year(l[3]):
            return str(l[3])
        if len(l)>5:
            if self.has_year(l[5]):
                return str(l[5])
            else:
                print(s)
                return 'no match'
    
    def return_date(self,row):
        return self.get_datestr(row.fn)
    
    def gen_dt(self,yr,mo,dy):
        return(f'{yr}-{str(mo).zfill(2)}-{str(dy).zfill(2)}')
    
    
#!!! To better parse dates it would be helpful to have fracking date as a hint
#!!!   to decode 3 digit mo_dy codes.
    
    def parse_fndate(self,fnd):
        #print(fnd)
        try:
            yr = fnd[-4:]
            rest = fnd[:-4]
            if (len(rest)>4) | (len(rest)<2):
                print(f'error: {fnd}')
                return 'error'
            if (len(rest)==4):
                mo = rest[:2]
                dy = rest[2:]
                return self.gen_dt(yr,mo,dy)
            if (len(rest)==2):
                mo = rest[0]
                dy = rest[1]
                return self.gen_dt(yr,mo,dy)
            # three digit mo day
            if int(rest[0])!=1: # one digit month for sure
                return self.gen_dt(yr,rest[0],rest[1:])
            if int(rest[:2]) < 13: # most likely a two digit month
                return self.gen_dt(yr,rest[:2],rest[2])
            return yr+'-?-?'
        except:
            return 'error'    

    def parse_fndate_vers(self,fnd):
        # like parse_fndate but returns list of possibles (usu just one),
        #   to accomodate 3 digit day-month situation
        #print(fnd)
        try:
            yr = fnd[-4:]
            rest = fnd[:-4]
            if (len(rest)>4) | (len(rest)<2):
                print(f'error: {fnd}')
                return ['error']
            if (len(rest)==4):
                mo = rest[:2]
                dy = rest[2:]
                return [self.gen_dt(yr,mo,dy)]
            if (len(rest)==2):
                mo = rest[0]
                dy = rest[1]
                return [self.gen_dt(yr,mo,dy)]
            # three digit mo day
            if int(rest[0])!=1: # one digit month for sure
                return [self.gen_dt(yr,rest[0],rest[1:])]
            if int(rest[:2]) < 13: # most likely a two digit month
                # two versions
                out = [self.gen_dt(yr,rest[:2],rest[2]),
                       self.gen_dt(yr,rest[0],rest[1:])]
                return out
            return [yr+'-?-?']
        except:
            return 'error'    

    def get_API(self,s):
        s = s.replace(' ','-')
        l = s.split('-')
        typ = self.get_filetype(s)
        if typ=='old':
            api = l[0]
            return api[:10]
        if typ in ['new','dashed_old']:
            return l[0]+l[1]+l[2] 
        return '?'
            

