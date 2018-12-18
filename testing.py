import sci, os, sys, glob
import io, csv, json 

# testing the sci package 

## sci.store.swift tests 

bucket = 'tester'
prefix = 'toolbox' 
filter = {'tag1': 'val1', 'tag2': 'val2'}

print('***********************************************************************') 
print('   Initialize swift connection ...mystor = sci.store.swift(%s, %s)' % (bucket, prefix))
mystor = sci.store.swift(bucket, prefix)

print('***********************************************************************') 
print('   mystor.bucket_list() gets a list of objects from bucket %s under prefix %s ...' % (bucket, prefix))
names = mystor.bucket_list()
print(len(names),names[:3])

print('***********************************************************************') 
print('   mystor.file_upload(list) to %s/%s ...' % (bucket, prefix))
files=glob.glob("sci/*")
ret = mystor.file_upload(files)

ddir = os.path.expanduser('~/Private')
if not os.path.exists(ddir):
    ret=os.makedirs(ddir)
names = mystor.bucket_list()[:3]
print('***********************************************************************') 
print('   filelist = mystor.file_download(mylist,"~/Private") from %s/%s to private folder ' % (bucket, prefix))
downloaded_files = mystor.file_download(names, '~/Private')
print('   1st file downloaded of a total of %s files is:' % len(downloaded_files))
print('   %s' % downloaded_files[:1])

print('***********************************************************************') 
print('   Uploading 3 files back from %s' % os.path.join(ddir,prefix)) 
print('   mystor.file_upload(list) to %s/%s ...' % (bucket, prefix))
files=glob.glob("%s/%s/*" % (ddir,prefix))
uploaded_objects = mystor.file_upload(files[:3])
print('   1st file uploaded as tester:')
print('   %s' % uploaded_objects[:1])

print('***********************************************************************') 
print('   Uploading 1 file from %s' % os.path.join(ddir,prefix)) 
print('   uploaded_objects=mystor.file_upload(list) to %s/%s ...' % (bucket, prefix))
files=glob.glob("%s/%s/*" % (ddir,prefix))
uploaded_objects = mystor.file_upload(files[:1],'a-single-test-file.txt')
print('   1st file uploaded as tester:')
print('   %s' % uploaded_objects[:1])

print('***********************************************************************') 
print('   Set metadata on uploaded object')
print('   ret=mystor.object_meta_set(uploaded_objects[0], %s)' % (filter))
ret=mystor.object_meta_set(uploaded_objects[0], filter)

print('***********************************************************************') 
print('   Get metadata from uploaded object')
print('   metadict=mystor.object_meta_get(uploaded_objects[0]))')
metadict=mystor.object_meta_get(uploaded_objects[0])
print('  metadata: %s' % metadict)

print('***********************************************************************') 
print('   read in CSV file')
print('   table=mystor.object_get_csv("pi_all.csv",False,"excel")')
print('   headers = next(table, None)')
table=mystor.object_get_csv('pi_all.csv',False,'excel')
headers = next(table, None)
print(headers)

print('***********************************************************************') 
print('   write table to CSV file')
print('   ret = mystor.object_put_csv("pi_all.dump5.csv", table)')
ret = mystor.object_put_csv("pi_all.dump5.csv", table)

print('***********************************************************************') 
print('   read in json file to dictionary')
print('   j = mystor.object_get_json("pi_all.json")')
print('   PIs = sci.json.jget(j,"pi_dept")')
j = mystor.object_get_json("pi_all.json")
PIs = sci.json.jget(j,"pi_dept")
print(PIs[:3])

print('***********************************************************************') 
print('   write dictionary to json file')
print('   ret = mystor.object_put_json("pi_list.json", PIs)')
ret = mystor.object_put_csv("pi_all.dump5.csv", table)

print('***********************************************************************') 
print('   write dictionary to pickle')
print('   ret = mystor.object_put_pickle("pi_list_pickle", PIs)')
ret = mystor.object_put_pickle("pi_list_pickle", PIs)

##  Some more testing here ### 

#print('load slurm jobs')
#table1=mystor.object_get_csv('toolbox/slurm_jobs.csv',False,'excel')

#print('save slurm jobs to pickle')
#ret = mystor.object_put_pickle('toolbox/slurm_jobs.pickle', table1)


