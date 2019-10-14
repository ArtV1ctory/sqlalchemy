from DBConnector import Connector

if __name__ == "__main__":
  connector = Connector('adminroot', 'admin_1111',
                          'testserverpoly.database.windows.net',
                          'Test', 'ODBC Driver 17 for SQL Server')
  connector.connect()
  python = connector.getTable('python')
  connector.doQuery('delete from python')
  connector.deleteColumn(python, 'date')
  connector.insertIntoTable(python, [
      {'id': 2, 'name': "greenpython", 'date': '2019-05-06 14:14:15'},
      {'id': 3, 'name': "rosepython", 'date': '2019-05-06 12:55:10'}
    ])
  print python.columns
  print connector.selectAllFromTable(python)
