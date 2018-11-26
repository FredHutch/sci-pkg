# ### sci-pkg ###
# store: functions that handle storage (object/cloud storage, posix file systems)
#

import os

"""
Simplified classes for accessing object storage systems.
Currently only implemented for Swift Storage.

Current limitations: 
 - swift: object_put function limited to 5GB per object
"""
class swift:
    """
    Initialize an Openstack Swift container/bucket. 
    Data will be written to the root of the bucket unless the virtual dir
    prefix is set.The prefix can be a single virtual folder such as 
    'subfolder' or a virtual folder such as 'folder/subfolder/subfolder'.     
    Example:
        mystor = sci.store.swift('the-bucket', 'virtual/sub/dir') 
        my_objects = mystor.bucket_list()
    For authentication you need to have set some environment 
    variables. This can either be OS_AUTH_URL, OS_TENANT_NAME, 
    OS_USERNAME, OS_PASSWORD or OS_STORAGE_URL and OS_AUTH_TOKEN. 
    If you have Swift Commander installed, a new auth token and
    storage URL will be automatically stored in the ~/.swift
    folder after you use the 'swc' command. 
    """

    def __init__(self, bucket, prefix=None):
        """ 
        Examples:
        mystor = swift('the-bucket', 'virtual/sub/directory')
        my_objects = mystor.bucket_list()
        my_filtered_objects = mystor.bucket_list({'proj': 'ABC', 'tag': 'new'})
        dict = mystor.object_put('the-object', "THE CONTENT", {tag': 'new'})
        obj = mystor.object_get('the-object')
        j = mystor.object_get_json('the-object.json')
        ret = mystor.object_meta_set('the-object', {'proj': 'XYZ'):
        dict = mystor.object_meta_get('the-object')
        """
        import swiftclient, json
        sw_auth_version = 2
        sw_authurl =  os.getenv("OS_AUTH_URL","")
        sw_user = os.getenv("OS_USERNAME", "")
        sw_key = os.getenv("OS_PASSWORD", "")
        self.tenant = os.getenv("OS_TENANT_NAME", "")
        self.storageurl = os.getenv("OS_STORAGE_URL","")
        self.authtoken = os.getenv("OS_AUTH_TOKEN","")
        if not sw_key and not self.authtoken:
            #reading authtoken from file cache in ~/.swift folder
            self.authtoken = self._get_set_token_file(sw_authurl)

        options = self._get_swift_options(sw_authurl,sw_user,sw_key)

        self.swiftconn = swiftclient.client.Connection(authurl=sw_authurl, user=sw_user, key=sw_key,  
                auth_version=sw_auth_version, os_options=options)
        try:
            self.storageurl, newauthtoken = self.swiftconn.get_auth()
        except swiftclient.exceptions.ClientException as e:
            print("Connection Error: %s" % e.msg)
            self.bucket = None
            self.prefix = None
            return

        if self.authtoken != newauthtoken:
            self.authtoken == newauthtoken
            # writing new authtoken to file cache in ~/.swift folder
            self._get_set_token_file(sw_authurl, newauthtoken)

        self.bucket = bucket
        self.prefix = prefix

    def bucket_list(self, filter=None):
        """
        return a list of objects from the previously initialized bucket. 
        This list can be a filtered list by passing in a dictionary of
        key / value pairs as filter. 

        Example:
        my_filtered_objects = mystor.bucket_list({'proj': 'ABC', 'tag': 'new'})
        """

        if self.bucket == None:
            return []

        try:
            listing = self.swiftconn.get_container(self.bucket,
                                           prefix=self.prefix,
                                           full_listing=True)
        except swiftclient.exceptions.ClientException as e:
            print("Bucket List Error: %s (%s, %s)" % (e.msg, e.http_status,  e.http_reason))
            return []

        newlist=[]
        for item in listing[1]:
            if item['content_type'] != 'application/directory' and \
                not item['name'].startswith('.') and not '/.' in item['name']:
                    if not filter:
                        newlist.append(item["name"])
                    else:
                        meta = self.object_meta_get(item['name'])
                        # is filter a full subset of meta
                        if filter.items() <= meta.items():
                            newlist.append(item["name"])

        #return [item["name"] for item in listing[1]]
        return newlist

    def object_get(self, objname):
        """
        load the object into memory
        Example:
        content = mystor.object_get('prefix/myobj.json')
        -------------------
        for tools using file handles (such as pandas) use io.StringIO or
        io.BytesIO 
        handle = io.StringIO(content.decode('utf-8')) 
           # or handle = io.BytesIO(content)
        dataframe = pd.read_csv(handle)

        """
        if self.bucket == None:
            return None
        content = self.swiftconn.get_object(self.bucket, objname)[1]
        return content 

    def object_get_json(self, objname):
        """ 
        load object into memory and de-serialize json 
        Example:
        j = mystor.object_get_json('prefix/myobj.json')
        print(json.dumps(j, indent=2))
        """
        import json
        content = self.object_get(objname)
        return json.loads(content.decode('utf-8'))

    def object_get_csv(self, objname, dictreader=True, dialect=None):
        """ 
        load swift object into memory and return a csv.DictReader object.
        The DictReader object has the attribute 'fieldnames' that returns
        the csv header as a list. Optionally you can use csv.reader 
        instead of csv.DictReader and a different dialect, such as 'excel'.
        Examples:
        -----------------------------------------------------------------
        table = mystor.object_get_csv('prefix/myobj.csv')
        for row in table:
            for field in table.fieldnames:
                print(field,':',row[field])
        ---------------
        table = mystor.object_get_csv('toolbox/pi_all.csv',False,'excel')
        headers = next(table, None)
        print(headers)
        for row in table:
            print (row)
        -----------------
        If you do not want to use the internal python csv package you can
        use the io.StringIO function to create an io handle in memory that
        can be used instead of a file handle, e.g. for pandas:
        -----------------
        content= self.object_get(objname)
        handle = io.StringIO(content.decode('utf-8')) 
           # or handle = io.BytesIO(content)
        dataframe = pd.read_csv(handle)
        """
        import io, csv
        content= self.object_get(objname)
        if not content:
            return None
        handle = io.StringIO(content.decode('utf-8'))
        # sniffer does not seem to work here
        #dialect = csv.Sniffer().sniff(handle.read(4096))
        if dictreader:
            if dialect:
                table = csv.DictReader(handle, dialect)
            else:
                table = csv.DictReader(handle)
        else:
            if dialect:
                table = csv.reader(handle, dialect)
            else:
                table = csv.reader(handle)
        return table

    def object_meta_get(self, objname):
        """ retrieve custom metadata from object as a dictionary """
        if self.bucket == None:
            return None
        head = self.swiftconn.head_object(self.bucket,objname)
        newhead = {}
        for k,v in head.items():
            if k.startswith('x-object-meta-'):
                newhead[k.replace('x-object-meta-','')] = v
        return newhead

    def object_meta_set(self, objname, metadict):
        """ set custom metadata on existing object using a dictionary """

        if self.bucket == None:
            return None

        metadict = self._fix_metadict(metadict)

        resp = dict()
        #self.swiftconn.post_object(self.storageurl, token=self.authtoken, \
        #    bucket=self.bucket, name="%s/%s" % (self.prefix,objname), \
        #    headers=metadict, http_conn=None, response_dict=resp, service_token=None)
        ret = self.swiftconn.post_object(self.bucket, "%s/%s" % (self.prefix,objname), \
            headers=metadict, response_dict=resp)        

    def object_put(self, objname, content, metadict=None):
        """ 
        save object to bucket and optionally set metadata using a dict 
        
        import io, pickle
        newcont = pickle.dump(content)
        
        """

        if self.bucket == None:
            return None
        metadict = self._fix_metadict(metadict)

        #self.swiftconn.put_object(self.storageurl, \
        #    bucket=self.bucket, name="%s/%s" % (self.prefix,objname), \
        #    contents=content, content_length=None, etag=None, chunk_size=None, \
        #    content_type=None, headers=metadict, http_conn=None, proxy=None, \
        #    query_string=None, response_dict=None, service_token=None)

        if objname.find('/') > -1:
            obj = objname
        else:
            obj = "%s/%s" % (self.prefix,objname)

        resp = dict()
        ret = self.swiftconn.put_object(self.bucket, obj, content, \
            response_dict=resp, headers=metadict)

    def object_put_json(self, objname, content, metadict=None):
        """ save json object to bucket and optionally set metadata using a dict """
        import json

        if self.bucket == None:
            return None        
        j = json.dumps(content, indent=4)
        ret = self.object_put(objname,j,metadict)

    def object_put_pickle(self, objname, content, metadict=None):
        """ 
        save pickle object to bucket and optionally set metadata using a dict. 
        Large Pickle objects can be faster than large json objects, however
        they are proprietary Python data objects and cannot be used by R, etc.  
        """
        import pickle

        if self.bucket == None:
            return None        
        p = pickle.dumps(content)
        ret = self.object_put(objname,p,metadict)

    def object_put_csv(self, objname, content, metadict=None, 
                        dictwriter=False, dialect=None):
        """ 
        save csv object to bucket and optionally set metadata using a dict
        """
        import io, csv

        if self.bucket == None:
            return None

        handle = io.StringIO()
        # sniffer does not seem to work here
        #dialect = csv.Sniffer().sniff(handle.read(4096))
        if dictwriter:
            if dialect:
                wr = csv.DictWriter(handle, dialect)
                wr.writerows(content)
            else:
                wr = csv.DictWriter(handle)
                wr.writerows(content)
        else:
            if dialect:
                wr = csv.writer(handle, dialect)
                wr.writerows(content)
            else:
                wr = csv.writer(handle)
                wr.writerows(content)

        buffer = handle.getvalue()

        ret = self.object_put(objname,buffer,metadict)

    def _object_ext(self, name):
        """ get the object extention (like content type) """
        ext = name.split(".")[-1]
        if ext == name:
            return ""
        return ext

    def _fix_metadict(self, metadict):
        """ ensure that x-object-meta prefix is added to all metadata keys """
        if not metadict:
            return metadict
        metadict2 = {}
        for k,v in metadict.items():
            if k.startswith('x-object-meta-'):
                j = k
            else:
                j = "x-object-meta-%s" % k
            metadict2[j] = v
        return metadict2

    def _get_swift_options(self, sw_authurl, sw_user, sw_key):

        if sw_key:
            if not sw_authurl:
                print ("Please set env var OS_AUTH_URL, e.g to https://host.domain.org/auth/v2.0")
                return {}
            if not sw_user:
                print ("Please set environment variable OS_USERNAME, e.g. your user name")
                return {}
            if not self.tenant:
                print ("Please set environment variable OS_TENANT_NAME, e.g. AUTH_tenantname")
                return {}
            print('  using password to authenticate ...')
            options = { 
                "tenant_name" : self.tenant,
                # or service_type, endpoint_type, tenant_name, object_storage_url, 
                # region_name, service_username, service_project_name, service_key
            }
        elif self.authtoken:
            if not self.tenant:
                print ("Please set environment variable OS_TENANT_NAME")
                return {}
            if not self.storageurl:
                print ("Please set environment variable OS_STORAGE_URL")
                return {}
            options = { 
                "tenant_name" : self.tenant,
                "object_storage_url" : self.storageurl,
                "auth_token" : self.authtoken,
            }
        else:
            options = {}
            print('Please set either OS_AUTH_TOKEN or OS_PASSWORD environment ' \
                  'variables. (e.g. execute "read -s OS_PASSWORD")')
        return options

    def _get_set_token_file(self, authurl, newauthtoken=None):
        import urllib.parse
        homedir = os.path.expanduser("~")        
        host = urllib.parse.urlparse(authurl).netloc
        authtokenfile = os.path.join(homedir,'.swift','auth_token_%s_v2_%s' % (host,self.tenant))
        storageurlfile = os.path.join(homedir,'.swift','storageurl_%s_v2_%s' % (host,self.tenant))

        if newauthtoken:
            if not os.path.exists(os.path.join(homedir,'.swift')):
                os.mkdir(os.path.join(homedir,'.swift'))
            if os.path.exists(authtokenfile):
                os.remove(authtokenfile)
            with open(authtokenfile, 'w') as f:
                f.write(newauthtoken)
            with open(storageurlfile, 'w') as f:
                f.write(self.storageurl)
            return ""
        else:
            if os.path.exists(authtokenfile):
                with open(authtokenfile, 'r') as f:
                    self.authtoken = f.readline().strip()
                with open(storageurlfile, 'r') as f:
                    self.storageurl = f.readline().strip()
                return self.authtoken
            else:
                return ""


class s3:
    """
    Initialize an s3 bucket using a certain profile
    Data will be written to the root of the bucket unless the virtual dir
    prefix is set.The prefix can be a single virtual folder such as 
    'subfolder' or a virtual folder such as 'folder/subfolder/subfolder'.     
    Example:
        mystor = sci.store.s3('the-bucket', 'virtual/dir', 'profile') 
        my_objects = mystor.bucket_list()
    """

    def __init__(self, bucket, prefix=None, profile='default'):
        """ 
        Examples:
        mystor = s3('the-bucket', 'virtual/sub/directory')
        """
        self.s3conn = "s3"
        self.bucket = bucket
        self.prefix = prefix

    def bucket_list(self, filter=None):
        """
        return a list of objects from the previously initialized bucket. 
        This list can be a filtered list by passing in a dictionary of
        key / value pairs as filter. 

        Example:
        my_filtered_objects = mystor.bucket_list({'proj': 'ABC', 'tag': 'new'})
        """

        if self.bucket == None:
            return []

        try:
            listing = self.s3conn.get_container(self.bucket,
                                           prefix=self.prefix,
                                           full_listing=True)
        except:
            return []

        newlist=[]
        for item in listing[1]:
            if item['content_type'] != 'application/directory' and \
                not item['name'].startswith('.') and not '/.' in item['name']:
                    if not filter:
                        newlist.append(item["name"])
                    else:
                        meta = self.object_meta_get(item['name'])
                        # is filter a full subset of meta
                        if filter.items() <= meta.items():
                            newlist.append(item["name"])

        #return [item["name"] for item in listing[1]]
        return newlist

    def object_get(self, objname):
        """ load the object into memory """
        if self.bucket == None:
            return None
        raw = self.swiftconn.get_object(self.bucket, objname)[1]
        return raw 
        #raw = lzma.decompress(compressed)
        #data = json.loads(raw.decode("utf-8"))
        # do something with the data ...

    def object_get_json(self, objname):
        """ load object into memory and de-serialize json """
        raw = self.object_get(objname)
        return json.loads(raw.decode()) #json.loads(raw.decode("utf-8"))

    def object_get_csv(self, objname):
        """ load object into memory and convert to csv """
        raw = self.object_get(objname)
        return json.loads(raw.decode()) #json.loads(raw.decode("utf-8"))

    def object_meta_get(self, objname):
        """ retrieve custom metadata from object as a dictionary """
        if self.bucket == None:
            return None
        head = self.s3conn.head_object(self.bucket,objname)
 
    def object_meta_set(self, objname, metadict):
        """ set custom metadata on existing object using a dictionary """

        if self.bucket == None:
            return None

        ret = self.s3conn.post_object(self.bucket, "%s/%s" % (self.prefix,objname))        

    def object_put(self, objname, content, metadict=None):
        """ save object to bucket and optionally set metadata using a dict """

        if self.bucket == None:
            return None

        ret = self.s3conn.put_object(self.bucket, "%s/%s" % (self.prefix,objname), content, \
            response_dict=resp, headers=metadict)

    def _object_ext(self, name):
        """ get the object extention (like content type) """
        ext = name.split(".")[-1]
        if ext == name:
            return ""
        return ext


class google:
    """
    Initialize a Google cloud bucket using a certain profile
    Data will be written to the root of the bucket unless the virtual dir
    prefix is set.The prefix can be a single virtual folder such as 
    'subfolder' or a virtual folder such as 'folder/subfolder/subfolder'.     
    Example:
        mystor = sci.store.google('the-bucket', 'virtual/dir', 'profile') 
        my_objects = mystor.bucket_list()
    """

    def __init__(self, bucket, prefix=None, profile='default'):
        """ 
        Examples:
        mystor = s3('the-bucket', 'virtual/sub/directory')
        """
        self.s3conn = "s3"
        self.bucket = bucket
        self.prefix = prefix

    def bucket_list(self, filter=None):
        """
        return a list of objects from the previously initialized bucket. 
        This list can be a filtered list by passing in a dictionary of
        key / value pairs as filter. 

        Example:
        my_filtered_objects = mystor.bucket_list({'proj': 'ABC', 'tag': 'new'})
        """

        if self.bucket == None:
            return []

        try:
            listing = self.s3conn.get_container(self.bucket,
                                           prefix=self.prefix,
                                           full_listing=True)
        except:
            return []

        newlist=[]
        for item in listing[1]:
            if item['content_type'] != 'application/directory' and \
                not item['name'].startswith('.') and not '/.' in item['name']:
                    if not filter:
                        newlist.append(item["name"])
                    else:
                        meta = self.object_meta_get(item['name'])
                        # is filter a full subset of meta
                        if filter.items() <= meta.items():
                            newlist.append(item["name"])

        #return [item["name"] for item in listing[1]]
        return newlist

    def object_get(self, objname):
        """ load the object into memory """
        if self.bucket == None:
            return None
        raw = self.swiftconn.get_object(self.bucket, objname)[1]
        return raw 
        #raw = lzma.decompress(compressed)
        #data = json.loads(raw.decode("utf-8"))
        # do something with the data ...

    def object_get_json(self, objname):
        """ load object into memory and de-serialize json """
        raw = self.object_get(objname)
        return json.loads(raw.decode()) #json.loads(raw.decode("utf-8"))

    def object_get_csv(self, objname):
        """ load object into memory and convert to csv """
        raw = self.object_get(objname)
        return json.loads(raw.decode()) #json.loads(raw.decode("utf-8"))

    def object_meta_get(self, objname):
        """ retrieve custom metadata from object as a dictionary """
        if self.bucket == None:
            return None
        head = self.s3conn.head_object(self.bucket,objname)
 
    def object_meta_set(self, objname, metadict):
        """ set custom metadata on existing object using a dictionary """

        if self.bucket == None:
            return None

        ret = self.s3conn.post_object(self.bucket, "%s/%s" % (self.prefix,objname))        

    def object_put(self, objname, content, metadict=None):
        """ save object to bucket and optionally set metadata using a dict """

        if self.bucket == None:
            return None

        ret = self.s3conn.put_object(self.bucket, "%s/%s" % (self.prefix,objname), content, \
            response_dict=resp, headers=metadict)

    def _object_ext(self, name):
        """ get the object extention (like content type) """
        ext = name.split(".")[-1]
        if ext == name:
            return ""
        return ext


 
class azure:
    """
    Initialize an s3 bucket using a certain profile
    Data will be written to the root of the bucket unless the virtual dir
    prefix is set.The prefix can be a single virtual folder such as 
    'subfolder' or a virtual folder such as 'folder/subfolder/subfolder'.     
    Example:
        mystor = sci.store.azure('the-bucket', 'virtual/dir', 'profile') 
        my_objects = mystor.bucket_list()
    """

    def __init__(self, bucket, prefix=None, profile='default'):
        """ 
        Examples:
        mystor = s3('the-bucket', 'virtual/sub/directory')
        """
        self.s3conn = "s3"
        self.bucket = bucket
        self.prefix = prefix

    def bucket_list(self, filter=None):
        """
        return a list of objects from the previously initialized bucket. 
        This list can be a filtered list by passing in a dictionary of
        key / value pairs as filter. 

        Example:
        my_filtered_objects = mystor.bucket_list({'proj': 'ABC', 'tag': 'new'})
        """

        if self.bucket == None:
            return []

        try:
            listing = self.s3conn.get_container(self.bucket,
                                           prefix=self.prefix,
                                           full_listing=True)
        except:
            return []

        newlist=[]
        for item in listing[1]:
            if item['content_type'] != 'application/directory' and \
                not item['name'].startswith('.') and not '/.' in item['name']:
                    if not filter:
                        newlist.append(item["name"])
                    else:
                        meta = self.object_meta_get(item['name'])
                        # is filter a full subset of meta
                        if filter.items() <= meta.items():
                            newlist.append(item["name"])

        #return [item["name"] for item in listing[1]]
        return newlist

    def object_get(self, objname):
        """ load the object into memory """
        if self.bucket == None:
            return None
        raw = self.swiftconn.get_object(self.bucket, objname)[1]
        return raw 
        #raw = lzma.decompress(compressed)
        #data = json.loads(raw.decode("utf-8"))
        # do something with the data ...

    def object_get_json(self, objname):
        """ load object into memory and de-serialize json """
        raw = self.object_get(objname)
        return json.loads(raw.decode()) #json.loads(raw.decode("utf-8"))

    def object_get_csv(self, objname):
        """ load object into memory and convert to csv """
        raw = self.object_get(objname)
        return json.loads(raw.decode()) #json.loads(raw.decode("utf-8"))

    def object_meta_get(self, objname):
        """ retrieve custom metadata from object as a dictionary """
        if self.bucket == None:
            return None
        head = self.s3conn.head_object(self.bucket,objname)
 
    def object_meta_set(self, objname, metadict):
        """ set custom metadata on existing object using a dictionary """

        if self.bucket == None:
            return None

        ret = self.s3conn.post_object(self.bucket, "%s/%s" % (self.prefix,objname))        

    def object_put(self, objname, content, metadict=None):
        """ save object to bucket and optionally set metadata using a dict """

        if self.bucket == None:
            return None

        ret = self.s3conn.put_object(self.bucket, "%s/%s" % (self.prefix,objname), content, \
            response_dict=resp, headers=metadict)

    def _object_ext(self, name):
        """ get the object extention (like content type) """
        ext = name.split(".")[-1]
        if ext == name:
            return ""
        return ext


 