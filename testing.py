import sci  

bucket = 'tester'
prefix = 'toolbox'
filter = {'tag1': 'val1', 'tag2': 'val2'}
 
print('init swift...')
conn = sci.store.swift(bucket, prefix)

 
print('list bucket...')
names = conn.bucket_list(filter)
print(len(names),names)

names = conn.bucket_list()
print(len(names),names)

import io
import csv
import json

print('read csv...')

table=conn.object_get_csv('toolbox/pi_all.csv',False,'excel')
#headers = next(table, None)
#print(headers)
#print('-----------------------')
#for row in table:
#    print (row)

print('object_put dump ...')
ret = conn.object_put_csv('toolbox/pi_all.dump5.csv', table)

j = conn.object_get_json('toolbox/pi_all.json')
pis = sci.json.jget(j,'pi_dept')
#print(pis)
ret = conn.object_put_pickle('toolbox/pi_list4', pis)
print(ret)

#print(json.dumps(j, indent=2))

print('load slurm jobs')
#table1=conn.object_get_csv('toolbox/slurm_jobs.csv',False,'excel')

print('save slurm jobs to pickle')
#ret = conn.object_put_pickle('toolbox/slurm_jobs.pickle', table1)




