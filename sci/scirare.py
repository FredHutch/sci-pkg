# ### sci-pkg ###
# scirare: not frequently used functions, may end up in sciold.py
#

import itertools

def most_common(seq):
    """ eg: most = most_common(['A','B',B','C'])
        Which is the most commonly occuring element in a sequence (e.g. list)
        In this case: 'B'
    """
    # get an iterable of (item, iterable) pairs
    SL = sorted((x, i) for i, x in enumerate(seq))
    # print 'SL:', SL
    groups = itertools.groupby(SL, key=operator.itemgetter(0))
    # auxiliary function to get "quality" for an item
    def _auxfun(g):
        item, iterable = g
        count = 0
        min_index = len(seq)
        for _, where in iterable:
            count += 1
            min_index = min(min_index, where)
            # print 'item %r, count %r, minind %r' % (item, count, min_index)
        return count, -min_index
        # pick the highest-count/earliest item
    return max(groups, key=_auxfun)[0]