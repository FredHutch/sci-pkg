# ### sci-pkg ###
# sciauth: functions that handle authentication and authorization
#

import pwd

def uid2user(uid):
    """ eg: user = uid2user(54321)
        get username based on uidnumber, return uidNumber if fails
    """ 
    try:
        return pwd.getpwuid(uid).pw_name
    except:
        return str(uid)