# ### sci-pkg ###
# fh: Fred Hutch specific Helper functions 
#     (e.g. toolbox and pubmed stuff) 

def getToolbox(jsonfile):
    """
    loads a jsonfile from Toolbox into a list of dictionaries 
    or a simple dictionary.
    """
    if not jsonfile:
        print('You need to pass JSON file name from toolbox')
        return ""
    try:
        r = requests.get('https://toolbox.fhcrc.org/json/%s' % jsonfile)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as err:
        print ("Error: {}".format(err))
        return ""
