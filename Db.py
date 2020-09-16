import pymysql
import re

class Db:
    def __init__(self,host,user,password):
        self.host = host
        self.user = user
        self.password = password
        self.databaseStr = ''
        self.query = ''
        self.tableStr = ''
        self.link = None
        self.cursor = None
        self.connect()
    def database(self,databaseStr):
        self.databaseStr = databaseStr
        self.query = ''
        return self
    def removeLastComma(self,queryStr):
        return re.sub(",([^,]*)$","",queryStr)
    def escapeQuotes(self,queryStr):
        return re.sub("'","''",queryStr)
    def connect(self):
        try:
            self.link = pymysql.connect(self.host,self.user,self.password)
            self.cursor = self.link.cursor()
        except Exception as e:
            raise e
        return self
    def createDatabase(self,dbName):
        self.query = "CREATE database " + dbName
        return self
    def drop(self,name,table=True):
        if table:
            self.query = "drop table " + name
        else:
            self.query = "drop database " + name
        return self
    def truncate(self):
        self.query = "trunace table " + self.tableStr
        return self
    def select(self,select):
        self.query = "SELECT " + select + " FROM " + self.tableStr + "\n"
        return self
    def table(self,tableStr):
        self.tableStr = self.databaseStr + '.' + tableStr
        return self
    def where(self,where,conditional,condition):
        self.query += "WHERE " + where + " " + conditional + " " + condition + "\n"
        return self
    def andWhere(self,where,conditional,condition):
        self.query += "AND " + where + " " + conditional + " " + condition + "\n"
        return self
    def orWhere(self,where,conditional,condition):
        self.query += "OR " + where + " " + conditional + " " + condition + "\n"
        return self
    def orderBy(self,condition):
        self.query += "ORDER BY " + condition + "\n"
        return self
    def groupBy(self,condition):
        self.query += "GROUP BY " + condition + "\n"
        return self
    def leftJoin(self,table,condition1,conditional,condition2):
        self.query += "JOIN " + table + " ON " + condition1 + " " + conditional + " " + condition2 + "\n"
        return self
    def having(self,where,conditional,condition):
        self.query += "HAVING " + where + " " + conditional + " " + condition + "\n"
        return self
    def delete(self):
        self.query += "DELETE FROM " + self.tableStr + "\n"
        return self
    def insert(self,dataDict):
        queryStr = "INSERT INTO " + self.tableStr + "("
        for key in dataDict.keys():
            queryStr += key + ","
        queryStr = self.removeLastComma(queryStr)
        queryStr += ") "
        queryStr += "VALUES ("
        for value in dataDict.values():
            queryStr += "'" + self.escapeQuotes(format(value)) + "',"
        queryStr = self.removeLastComma(queryStr)
        queryStr += ")"
        self.query = queryStr
        return self
    def update(self,dataDict):
        queryStr = "UPDATE " + self.tableStr + " SET \n"
        for key,value in dataDict.items():
            queryStr += key + " = '" + value + "', "
        self.query = self.removeLastComma(queryStr)
        self.query = self.escapeQuotes(queryStr)
        return self
    def put(self):
        try:
            self.cursor.execute(self.query)
            self.link.commit()
        except Exception as e:
            self.link.rollback()
            raise e
        return self
    def get(self):
        try:
            self.cursor.execute(self.query)
        except Exception as e:
            raise e
        return self.cursor.fetchall()
