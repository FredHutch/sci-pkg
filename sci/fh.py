import json, requests


# ### sci-pkg ###
# fh: Fred Hutch specific helper functions
#     (e.g. toolbox and pubmed stuff)


def getToolbox(jsonfile):
    """
    loads a jsonfile from Toolbox into a list of dictionaries 
    or a simple dictionary.
    """
    if not jsonfile:
        print("You need to pass JSON file name from toolbox")
        return ""
    try:
        r = requests.get("https://toolbox.fhcrc.org/json/%s" % jsonfile)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as err:
        print("Error: {}".format(err))
        return ""


###### interaction with Pubmed ############################


def getPapers(j, papers, pi, lastname, forename, initial, sinceyear, prn=True):
    if not papers:
        return []
    k = 0
    rank = 0
    month = ""
    year = ""
    aid = ""
    ISSN = ""
    rows = []
    for i, paper in enumerate(papers["PubmedArticle"]):
        article = paper["MedlineCitation"]["Article"]
        journal = article["Journal"]
        authors = article["AuthorList"]
        aids = article["ELocationID"]
        journalinfo = paper["MedlineCitation"]["MedlineJournalInfo"]

        retlst = []
        if len(authors) > 0:
            rank, author = authorRank(authors, lastname, forename, initial)

        # if rank == 0 or (rank > 1 and rank < len(authors)-3):
        # only show pubs with first and last author
        #    continue
        # if len(aids)==0:
        #    continue
        if "Month" in journal["JournalIssue"]["PubDate"]:
            month = _month2num(journal["JournalIssue"]["PubDate"]["Month"])
            year = journal["JournalIssue"]["PubDate"]["Year"]

        if year == "" or year < sinceyear:
            continue

        k += 1
        if prn:
            print("%d) %s" % (k, article["ArticleTitle"]))

        if "ISSNLinking" in journalinfo:
            ISSN = journalinfo["ISSNLinking"]
        if len(aids) > 0:
            aid = aids[0]
            if prn:
                print("    ID: %s" % aids[0])

        first_or_last = 0
        if rank == 1:
            first_or_last = 1
        elif rank == len(authors):
            first_or_last = 1

        if prn:
            print("    Journal: %s, ISSN: %s" % (journal["Title"], ISSN))
            print("    Year: %s, Month: %s" % (year, month))
            print("    # of Authors: ", len(authors))
            print("    %s rank: %s" % (author, rank))
            print("    First or Last: %s" % first_or_last)

        peers = getPeers(j, authors, lastname, forename, initial)
        if peers:
            if prn:
                print("    PEERS ***:", ", ".join(peers))

        # if 'GrantList' in article:
        # ret = getGrants(article['GrantList'])

        # returns pi, author, aid, title, year, month, authorcount, authorpos, first_or_last, journal, issn
        first_or_last = 0
        if rank == 1:
            first_or_last = 1
        elif rank == len(authors):
            first_or_last = 1

        rows.append(
            [
                pi,
                author,
                aid,
                article["ArticleTitle"].encode("ascii", "ignore").decode(),
                year,
                month,
                len(authors),
                rank,
                first_or_last,
                len(peers),
                ";".join(peers),
                journal["Title"],
                ISSN,
            ]
        )

    return rows


def getGrants(grants):
    ### this is just a placeholder for now """
    for g in grants:
        if "GrantID" in g:
            print("   ", g["GrantID"], "(", g["Agency"], ")")


def authorRank(authors, lastname, forename, initials):
    """ get the position of a person in an author list, first do first name
        matching, then in a second loop try matching of initials.
    """
    i = 0
    retlist = []
    for a in authors:
        i += 1
        # if a['AffiliationInfo']:
        #    print ("Affiliation: ", a['AffiliationInfo'][0]['Affiliation'])
        if "LastName" in a and lastname:
            if "ForeName" in a:
                # print ("FORENAME", a['ForeName'], forename)
                if a["LastName"].lower() == lastname.lower() and (
                    forename.lower() in a["ForeName"].lower()
                    or a["ForeName"].lower() in forename.lower()
                ):
                    # retlist = [i, a['LastName'] + ' ' + a['ForeName'] + ' (' + a['Initials'] + ')']
                    retlist = [i, a["LastName"] + " " + a["ForeName"]]

    if len(retlist) > 0:
        return retlist
    i = 0
    for a in authors:
        i += 1
        if "LastName" in a:
            if "Initials" in a:
                # print("LAST", a['LastName'], lastname)
                if (
                    a["LastName"].lower() == lastname.lower()
                    and initials.lower() in a["Initials"].lower()
                ):
                    retlist = [i, a["LastName"] + " " + a["Initials"]]

    if len(retlist) > 0:
        return retlist
    else:
        return 0, ""


def getPeers(j, authors, lastname, forename, initial, institute="Fred Hutch"):
    """ get peer faculty from same institution, affiliation info in publication needs
        to include string institute, e.g. Fred Hutch'
    """
    import sci

    peers = []
    if not j:
        return peers
    HutchPIs = sci.tools.uniq(sci.json.jget(j, "displayName"))
    for a in authors:
        if not "LastName" in a:
            continue
        if not "ForeName" in a:
            continue
        if not "Initials" in a:
            continue
        if not "AffiliationInfo" in a:
            continue
        affinfo = a["AffiliationInfo"]
        aff = ""
        if len(affinfo) > 0:
            aff = affinfo[0]["Affiliation"]
        if not institute in aff:
            continue
        for hutchpi in HutchPIs:
            hlast, hfore, hinitial, hinitials = splitName(hutchpi)
            # skip current authorn
            if hlast.lower() == lastname.lower():
                # print ("***hlast, hfore, hinit:", hlast, hfore, hinit)
                # print ("***lastname, forename, initials:", lastname, forename, initials)
                if (
                    hfore.lower() in forename.lower()
                    or forename.lower() in hfore.lower()
                ):
                    continue
                if hinitial.lower() == initial.lower():
                    continue

            if a["LastName"].lower() == hlast.lower():
                if a["Initials"][0].lower() == hinitial.lower():
                    peers.append(hlast + " " + hfore)
    return peers


def splitName(fullname):
    fullname = fullname.replace(",", " ")
    fullname = fullname.replace(".", "")
    name = fullname.split()
    lastname = name[0]
    forename = ""
    initials = ""
    initial = ""
    if len(name) > 1:
        forename = name[1]
        initials = forename[0]
        initial = initials
        if len(name) == 3:
            forename = name[1]
            initials = name[1][0] + name[2][0]
            initial = name[1][0]
    # print ('** lastname, forename, initial, initials', lastname, forename, initial, initials)
    return lastname, forename, initial, initials


def searchPapers(author, institute="Fred Hutch"):
    if institute:
        query = "%s*[Affiliation] AND %s[Author]" % (institute, author)
    else:
        query = "%s[Author]" % author
    results = _EntrezSearch(query)
    id_list = results["IdList"]
    papers = _EntrezDetails(id_list)
    if not papers:
        print("no papers found for %s %s" % (author, institute))
        return []
    return papers


def _EntrezSearch(query):
    from Bio import Entrez

    Entrez.email = "your.email@example.com"
    handle = Entrez.esearch(
        db="pubmed", sort="relevance", retmax="2000", retmode="xml", term=query
    )
    results = Entrez.read(handle)
    return results


def _EntrezDetails(id_list):
    from Bio import Entrez

    ids = ",".join(id_list)
    try:
        Entrez.email = "your.email@example.com"
        handle = Entrez.efetch(db="pubmed", retmode="xml", id=ids)
        results = Entrez.read(handle)
        return results
    except:
        return []


def _month2num(month):
    d = {
        "January": "01",
        "February": "02",
        "March": "03",
        "April": "04",
        "May": "05",
        "June": "06",
        "July": "07",
        "August": "08",
        "September": "09",
        "October": "10",
        "November": "11",
        "December": "12",
        "Jan": "01",
        "Feb": "02",
        "Mar": "03",
        "Apr": "04",
        "May": "05",
        "Jun": "06",
        "Jul": "07",
        "Aug": "08",
        "Sep": "09",
        "Oct": "10",
        "Nov": "11",
        "Dec": "12",
    }
    if month in d:
        return d[month]
    else:
        return month

