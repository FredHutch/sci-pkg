import sci  

bucket = 'tester'
prefix = 'toolbox'
filter = {'tag1': 'val1', 'tag2': 'val2'}
 
print('init swift...')
conn = sci.store.swift(bucket, prefix)

 
print('list bucket...')
names = conn.bucket_list(filter)
print(len(names),names)



