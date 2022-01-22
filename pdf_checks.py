# -*- coding: utf-8 -*-
"""
Created on Tue Jan  4 10:47:50 2022

@author: Gary
"""
import os
from hashlib import sha256

def isFullPdf(fn):
    """does this pdf have its proper ending?  Source of code:
       https://www.tutorialexample.com/a-simple-guide-to-python-detect-pdf-file-is-corrupted-or-incompleted-python-tutorial/ 
       """
    end_content = ''
    start_content = ''
    size = os.path.getsize(fn)
    if size < 1024: return False 
    with open(fn, 'rb') as fin: 
        #start content 
        fin.seek(0, 0)
        start_content = fin.read(1024)
        start_content = start_content.decode("ascii", 'ignore' )
        fin.seek(-1024, 2)
        end_content = fin.read()
        end_content = end_content.decode("ascii", 'ignore' )
    start_flag = False
    #%PDF
    if start_content.count('%PDF') > 0:
        start_flag = True

    if end_content.count('%%EOF') and start_flag > 0:
        return True
    eof = bytes([0])
    eof = eof.decode("ascii")
    if end_content.endswith(eof) and start_flag:
        return True
    return False

def getHeaderInfo(fn):
    from PyPDF2 import PdfFileReader    
    #mypath = "../Downloads/banks_gov_bonds_default.pdf"
    pdf_toread = PdfFileReader(open(fn, 'rb'))
    pdf_info = pdf_toread.getDocumentInfo()
    print(pdf_info)

def fetch_hash(fn):
    """ returns a hash of the file named by fn, that should be unique and used
    to verify that pdf files are indeed unique.
    Return False is there is some problem with the process"""
    try:
        with open(fn,'rb') as f:
            return sha256(f.read()).hexdigest()
    except:
        return False
