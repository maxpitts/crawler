import urllib2
import pandas as pd
import re
from urllib2 import Request, urlopen, URLError
from multiprocessing.dummy import Pool as ThreadPool 
import datetime
print datetime.datetime.now()
import urllib2 
from multiprocessing.dummy import Pool as ThreadPool 

keyword_list = []

def get_urls_html(url):
    import re
    try:
        response = urllib2.urlopen(url)
        html = response.read()
        response.close() 
        if any(word in html for word in keyword_list):
          has_keyword = 'Y'
        else:
          has_keyword = 'N'
        return has_keyword
    except URLError, e:
        return e.reason
    except:
        return 'Privit'  

data=pd.read_csv('report1507499915077.csv', sep = ',', header = 0)
data['full_url'] ='http://'+data.Website.map(str)
data.head()
data_len = len(data)
num_data_part = data_len/500

for i in range(0,num_data_part):
    pool = ThreadPool(15) 
    data_part=data[i*500:(i+1)*500]
    data_part = data_part.reset_index()
    data_part['Has_Keywords'] = pool.map(get_urls_html, data_part['full_url'])
    pool.close() 
    pool.join()
    file_name = "webcrawl_report1507499915077_output_%(i)i.csv" % { "i" : i }
    data_part.to_csv(file_name, index = False)
    print i
    
