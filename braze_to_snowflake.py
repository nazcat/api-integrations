  
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json

# defining the api-endpoint  
url = "https://rest.iad-<BRAZE INSTANCE NUMBER>.braze.com/users/export/segment?segment_id=<SEGMENT ID>"
headers = {'content-type': 'application/json', \
           'Accept-Charset': 'UTF-8', \
           'Authorization': 'Bearer <API KEY>'}

r = requests.post(url, headers=headers)


r.json()
r.status_code


from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

zipurl = 'https://sample_aws_data_export.zip'
with urlopen(zipurl) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall('/Users/nuremek/downloads/data')
        
        
import pandas as pd
import os
import glob
import json

# Step 1: get a list of all csv files in target directory
my_dir = '/Users/nuremek/downloads/data'  

# Step 2: Build up list of files:
json_pattern = os.path.join(my_dir,'*.txt')
file_list = glob.glob(json_pattern)


# Step 3: Build up DataFrame:

dfs = [] # an empty list to store the data frames
for file in file_list:
    data = pd.read_json(file, lines=True) # read data frame from json file
    dfs.append(data) # append the data frame to the list

dfs   
df = pd.concat(dfs, ignore_index=True) # concatenate all the data frames in the list.
df.head()


del df['apps']
del df['devices']
del df['email_subscribe']
del df['email_unsubscribed_at']
del df['total_revenue']
del df['user_aliases']
del df['campaigns_received']
del df['canvases_received']
del df['created_at']
del df['email']
del df['braze_id']
del df['appboy_id']
del df['push_tokens']
df.head()


df1 = pd.DataFrame(df['custom_attributes'].values.tolist())
df1.columns = 'custom_attributes_'+ df1.columns

attr = pd.concat([df, df1['custom_attributes_rating'], df1['custom_attributes_ratingcomments']],axis=1)
attr.head()


import json
import pandas as pd
from pandas.io.json import json_normalize

event = attr['custom_events'].apply(pd.Series)
event


event2 = pd.concat([attr, event], axis=1)
#event2.head()
len(event2)


subsetDataFrame = dfObj[dfObj['Product'].isin(['NPS Rating Submitted']) ]


import numpy as np
event3= event2
event3['NPS Rating Submitted'] = np.where(event3.ne('NPS Rating Submitted'), 'NPS Rating Submitted','')
event3.head(50)


event3.to_csv('/Users/nuremek/downloads/test2.csv')


nps.columns
