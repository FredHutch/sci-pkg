# ### sci-pkg ###
# db: functions for DBMS (postgres, mysql, sqlite, mssql etc)
#

import os

class postgres:
    """
    Examples:
    mydb = mssql('the-bucket', 'virtual/sub/directory')
    
    the password should be stored in ~/.pgpass
    """
    def __init__(self, host, dbname, user, password, port=5432):
        """ 
        just pass in a bucket and an optional prefix. The prefix can be a 
        single virtual folder such as 'subfolder' or a virtual folder 
        such as 'folder/subfolder/subfolder'. Example:
        mystor = SwiftStorage('the-bucket', 'virtual/sub/directory') 
        """
        import psycopg2
    def execute(self, sqlstr):
        """
        execute a sql command on the database
        """
    def fetch(self, sqlstr):
        """
        return the result of a SQL query as a dictionary
        """

class mysql:
    """
    Examples:
    mystor = mssql('the-bucket', 'virtual/sub/directory')
    """
    def __init__(self, host, dbname, user, password, port=3306):
        """ 
        just pass in a bucket and an optional prefix. The prefix can be a 
        single virtual folder such as 'subfolder' or a virtual folder 
        such as 'folder/subfolder/subfolder'. Example:
        mystor = SwiftStorage('the-bucket', 'virtual/sub/directory') 
        """
        import PyMySQL
    def execute(self, sqlstr):
        """
        execute a sql command on the database
        """
    def fetch(self, sqlstr):
        """
        return the result of a SQL query as a dictionary
        """

class mssql:
    """
    Examples:
    mystor = mssql('the-bucket', 'virtual/sub/directory')
    """
    def __init__(self, host, dbname, user, password, port=1433):
        """ 
        just pass in a bucket and an optional prefix. The prefix can be a 
        single virtual folder such as 'subfolder' or a virtual folder 
        such as 'folder/subfolder/subfolder'. Example:
        mystor = SwiftStorage('the-bucket', 'virtual/sub/directory') 
        """
        import pymssql
    def execute(self, sqlstr):
        """
        execute a sql command on the database
        """
    def fetch(self, sqlstr):
        """
        return the result of a SQL query as a dictionary
        """

class sqlite:
    """
    Examples:
    mystor = mssql('the-bucket', 'virtual/sub/directory')
    """
    def __init__(self, dbname):
        """ 
        dbname is a file name of a local database. 
        use :memory: as filename to create the database in RAM 
        """
        import sqlite3
    def execute(self, sqlstr):
        """
        execute a sql command on the database
        """
    def fetch(self, sqlstr):
        """
        return the result of a SQL query as a dictionary
        """

def _getdbpasswd(host, port, dbname, user):
    passwd = os.path.join(os.path.expanduser("~"),'.sci','.dbpasswd')
    if not os.path.exists(passwd):
        return None 
    with open(passwd, 'r') as f:
        host1,port1,dbname1,user1,password1 = f.readline().strip().split(':')

