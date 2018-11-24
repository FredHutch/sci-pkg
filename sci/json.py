# ### sci-pkg ###
# json: functions that handle json files
#

import json

def jcompare(jsonfile_or_list,newlist):
    """ eg: addedlist, removedlist = jcompare('/tmp/cache.json',['a', 'b'])
    compares previously saved (json) list with a new list and returns
    a list of newly added items and a list of removed items.
    The old list can be a Python list or a json list stored in the file system
    """
    oldlist = []
    addedlist, removedlist = newlist, []
    if isinstance(jsonfile_or_list,list):
        oldlist=jsonfile_or_list
    if os.path.exists(jsonfile_or_list):
        with open(jsonfile_or_list, 'r') as f:
            oldlist=json.load(f)

    addedlist = [item for item in newlist if item not in oldlist]
    removedlist = [item for item in oldlist if item not in newlist]
    return addedlist, removedlist

def jsearch(json,sfld,search,rfld):
    """ return a list of values from a column based on a search """
    lst=[]
    for j in json:
        if j[sfld]==search or search == '*':
            lst.append(j[rfld].strip())
    return lst

def jgetonerow(json,sfld,search):
    """ return a row based  on a search """
    for row in json:
        if row[sfld]==search or search == '*':
            return row

def jsearchone(json,sfld,search,rfld):
    """ return the first search result of a column based search """
    for j in json:
        if j[sfld]==search:
            return j[rfld].strip()

def jget(json,rfld):
    """ return all values in one column """
    lst=[]
    for j in json:
        if j[rfld].strip() != "":
            lst.append(j[rfld].strip())
    return lst
