import mysql.connector
import json
import logging

HOST = 'mysqldb'
USER = 'root'
PASSWORD = '1qaz@WSX'
DATABASE = 'Managment'
RESOURCES_TABLE = 'Resources'
REQUESTS_TABLE = 'Requests'


class DBHandler:

    def __init__(self, is_init=False):
        self.db = None
        self.cursor = None
        self.is_init = is_init
        self.logger = logging.getLogger('dbh')

    def __enter__(self):
        if self.is_init:
            self.db = mysql.connector.connect(host=HOST, user=USER, password=PASSWORD)
        else:
            self.db = mysql.connector.connect(host=HOST, user=USER, password=PASSWORD, database=DATABASE)
        self.cursor = self.db.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.db:
            self.db.close()

    def execute_command(self, command: str, use_commit=False):
        result = self.cursor.execute(command)
        if use_commit:
            self.db.commit()
            return ''
        self.logger.info(result)
        if not self.cursor.description:
            return None
        else:
            row_headers = [x[0] for x in self.cursor.description]
            self.logger.info(row_headers)
            fetch = self.cursor.fetchall()
            self.logger.info(fetch)
            json_data = []
            for f in fetch:
                self.logger.info(f)
                json_data.append(dict(zip(row_headers, f)))
            return json.dumps(json_data)

