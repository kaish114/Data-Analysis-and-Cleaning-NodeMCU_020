
import sys

def progressbar(it, prefix="", size=60, file=sys.stdout):
    count = len(it)
    def show(j):
        x = int(size*j/count)
        file.write("%s[%s%s] %i/%i\r" % (prefix, "#"*x, "."*(size-x), j, count))
        file.flush()        
    show(0)
    for i, item in enumerate(it):
        yield item
        show(i+1)
    file.write("\n")
    file.flush()

import time


# Above Function is used to create Progress bar


import numpy as np
import pandas as pd



# # 2. Reading WiSenseData


dff = pd.read_csv('NodeMCU_020.csv' , header = None)  #reading the Outdoor dataset



# # 3. Renaming the Columns


df = dff.rename(columns={0: 'datestamp', 1: 'NodeAdd' , 2: 'Temperature', 3: 'Humidity'})



#Copying the original dataset ('df') into data1
data1 = df.copy() 


#Converting datatype of 'timeStamp' to datetime type
data1['datestamp'] = pd.to_datetime(data1['datestamp'])  


# Now We will create a new columns in our Dataset namely, 'temp1_changed'.
# This column will contain value '1' if temperature1 is changed else it will contain 0
data1['temp_changed'] = 0






#Checking Outliers
'''
# Following Scripts will deal with first value of each node if it is outlier

1. We'll just check if first value of each node for a particular column is outlier (i.e temperature > 100 or temperature < 0), if it is outlier then we'll change its value to next row value

'''


#from tqdm import tqdm_notebook

nodes = data1['NodeAdd'].unique() # this line will create an array having total unique nodes

print('Checking Outlier for Temperature')
for n in progressbar(nodes, "Processing records for Outlier "):
#for n in tqdm_notebook(nodes , desc = 'Processing records for Outlier'):
    for i in range(data1.shape[0] - 1):
        if(data1.loc[i , 'NodeAdd'] == n):
            val0 = float(data1.loc[i,'Temperature'])
            if(val0 < 0 or val0 > 100):
                data1.loc[i,'Temperature'] = data1.loc[i+1,'Temperature']
                print('Outlier Found at', i , 'for node' , n)
                break
            else:
                break






# # Following is the function to clean 'temperature1'

# # Logic behind cleaning the data
# 
# Example: Cleaning temperature1
# 
# To clean 'temperature1', we'll iterate through this column and select two values(rows) of a particular node and compare it.
# 1. If there absolute difference is more than 10C and timeinterval is less than 30 minutes then we'll replace later value with previous one.
# 2. If later value(row) is showing an Outlier and time interval is more than 30 minutes then will just replace it with 'NaN'.
# 
# 
# 




nodes = data1['NodeAdd'].unique() # this line will create an array having total unique nodes

#Function to clean 'temperature'

def temperature_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'NodeAdd'] == n):
            val0 = float(df.loc[i,'Temperature'])
            time0 = (df.loc[i,'datestamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'NodeAdd'] == n):
                val1 = float(df.loc[j , 'Temperature'])
                time1 = (df.loc[j , 'datestamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'Temperature'] = val0
                  df.loc[j, 'temp_changed'] = 1
                  k = j
                  break
                elif(((val1) > 100 or (val1) < 0 ) and minutes > 30.0):
                  df.loc[j,'Temperature'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break
                    
                    
                    
                    




#Call Above function to clean the dataset
print('Cleaning Temperature')
temperature_clean(data1)




data1.to_csv('kaish.csv')
