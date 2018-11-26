sci Package
===========

A Python 3 package or simply a collection of convenience and wrapper functions supporting tasks
frequently needed by scientists.

install
-------

| ``pip3 install sci`` or
| ``pip3 install --user --upgrade sci``

use the ``--user`` flag on shared computers where you do not have root access. It will install the
package in a place like ``~/.local/lib/python3.x/site-packages/sci`` . You can delete that folder
later to remove the package.

design goals
------------

1. Simplicity: wrap frequently used workflows into simple functions that make life easier for
   scientists. Most import statements should move into functions (except for frequently used ones
   like os, sys) to avoid confusion during autocomplete with VS Code and other IDEs.
2. Verbose but easily consumable docstrings: Docstrings are accessible via code autocomplete in IDEs
   such as VS Code or Atom and through automated documentation environments such as sphinx. Each
   docsting should start with an example use case for the function.
3. functional programming paradigm: While using classes is permitted we encourage writing fewer
   classes and more functions (perhaps with decorators)
4. Cross-platform: The code should work on Linux (RHEL and Debian based), Windows and Mac OS X.
5. Python 3 only compatibility, Python 2.x is legacy and we do not want to invest any extra time in
   it.

How to contribute your own code
-------------------------------

1. ``git clone git@github.com:FredHutch/sci-pkg.git``
2. create a new branch (eg : sci-yourname)
3. paste your function into module sci/new.py.
4. Make sure you add an example call in the first line of the doc string.
5. Add your test case to sci-pkg/sci/tests
