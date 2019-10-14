from sqlalchemy import create_engine, select
from sqlalchemy import MetaData, Table, Column, String, Integer

class Connector:
  """Class for connecting to a database and it modification"""

  def __init__(self, username, password, servername, dbname, driver, port=1433):
    """
    Initialization of parameters for connecting to a database

    :param username:    server admin login
    :type username:     str
    :param password:    password to access the database
    :type password:     str
    :param servername:  server to connect to the database
    :type servername:   str
    :param dbname:      database name
    :type dbname:       str
    :param driver:      the name of the driver that is required to connect to the database
    :type driver:       str
    :param port:        port to connect to the database
    :type port:         int
    """
    self.username = username
    self.password = password
    self.servername = servername
    self.dbname = dbname
    self.driver = '+'.join(driver.split())
    self.port = port

  def connect(self):
    """
    Database connection

    :parameters: none
    """
    self.engine = create_engine("mssql+pyodbc://{0}:{1}@{2}:{3}/{4}"
                                "?driver={5}".format(self.username,
                                                     self.password,
                                                     self.servername,
                                                     self.port,
                                                     self.dbname,
                                                     self.driver))
    self.conn = self.engine.connect()

  def selectAllFromTable(self, table):
    """
    Selects rows from a table

    :param table:  the table from which you want to get data
    :type table:   Table
    :return:       list with data rows from the table
    :rtype:        list
    """
    select_stmt = select([table])
    return self.conn.execute(select_stmt).fetchall()

  def getTable(self, tablename):
    """
    Gets an instance of a table from a database

    :param tablename:  the name of the table to get it from the database
    :type tablename:   str
    :return:           table instance
    :rtype:            Table
    """
    metadata = MetaData()
    return Table(tablename, metadata, autoload=True,
                           autoload_with=self.engine)

  def insertIntoTable(self, table, strings):
    """
    Inserts rows into a table

    :param table:    the table to insert the rows
    :type table:     Table
    :param strings:  rows to insert into the table
    :type strings:   list
    :return status:  0 if no new keys added; 1 if some new keys added
    :rtype:          int
    """
    length = len(strings[0])
    for s in strings:
      if len(s) != length:
        raise ValueError('Incorrect rows format for insert into table')
    max_length = 255
    types = {str: String(max_length), int: Integer(), unicode: String(max_length)}
    status = 0
    for s in strings:
      for i in range(len(s)):
        if s.keys()[i] not in table.columns and s.values()[i] is not None:
          status = 1
          stype = types.get(type(s.values()[i]))
          self.addColumn(table, s.keys()[i], stype)
    self.conn.execute(table.insert(), strings)
    return status

  def deleteColumn(self, table, columnname):
    """
    Removes a column from a table

    :param table:       the table from which you want to remove the column
    :type table:        Table
    :param columnname:  the name of the column to be deleted
    :type columnname:   str
    """
    table_meta = table.metadata.tables[table.fullname]
    table_meta._columns.remove(table_meta._columns[columnname])
    self.conn.execute('alter table {0} drop column {1}'.
                      format(table.fullname, columnname))

  def addColumn(self, table, columnname, type):
    """
    Adds a column to a table

    :param table:       the table to which you want to add the column
    :type table:        Table
    :param columnname:  column name to add
    :type columnname:   str
    :param type:        the type of data to be stored in the added column
    :type type:         sqlalchemy.sql.sqltypes.{data type}
    """
    table.append_column(Column(columnname, type))
    self.conn.execute('alter table {0} add "{1}" {2}'.
                      format(table.fullname, columnname, type))

  def doQuery(self, query):
    """
    Performs queries in sql language
    To select rows from a table, you need to apply
    the fetchone() or fetchall() methods to this function

    :param query:  query in sql language
    :type query:   str
    :return:       query result
    :type:         sqlalchemy.engine.result.ResultProxy
    """
    return self.conn.execute(query)
