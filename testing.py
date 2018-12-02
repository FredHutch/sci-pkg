import sci, os, sys

filter = {'tag1': 'val1', 'tag2': 'val2'}
 
print('init swift...')
mystor = sci.store.swift('tester', 'toolbox')

c = mystor.object_get_csv()

print('list bucket...')
names = sci.store.
print(len(names),names)

names = mystor.bucket_list()
print(len(names),names)

mydict = mystor.object_meta_get('moin2.csv')
print(mydict)

ret = mystor.object_meta_set('moin2.csv', {'tag1': 'val1', 'tag2': 'val2'})
#print(ret)

import io
import csv
import json

print('read csv...')

table=mystor.object_get_csv('toolbox/pi_all.csv',False,'excel')
#headers = next(table, None)
#print(headers)
#print('-----------------------')
#for row in table:
#    print (row)

print('object_put dump ...')
ret = mystor.object_put_csv('toolbox/pi_all.dump5.csv', table)
print(ret)

j = mystor.object_get_json('toolbox/pi_all.json')
pis = sci.json.jget(j,'pi_dept')
#print(pis)
ret = mystor.object_put_pickle('toolbox/pi_list4', pis)


#print(json.dumps(j, indent=2))

print('load slurm jobs')
#table1=mystor.object_get_csv('toolbox/slurm_jobs.csv',False,'excel')

print('save slurm jobs to pickle')
#ret = mystor.object_put_pickle('toolbox/slurm_jobs.pickle', table1)

ret=mystor.bucket_list()

