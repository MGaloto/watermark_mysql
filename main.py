import mysql.connector
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import Tuple, Any
import os

load_dotenv(override=True)
user = os.getenv("USER")
password = os.getenv("PASS")
host = os.getenv("HOST")
port= os.getenv("PORT")
table = 'FactTrips'
schema = 'sales'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class WaterMarkProcess():

    def __init__(self, 
                 host: str, 
                 user: str, 
                 password: str, 
                 port: str, 
                 table: str, 
                 schema: str) -> None:
        
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.table = table
        self.schema = schema
        self.procedure = "uspUpdateWatermark"
        self.watermark = "WatermarkTable"
        self.conn = None
        self.cursor = None
        self.dir = os.getcwd()


    class bcolors:
        GREEN = '\033[92m' 
        YELLOW = '\033[93m'  
        RED = '\033[91m'  
        RESET = '\033[0m' 

    def create_schema(self):
        schema_name = self.schema
        self.cursor.execute(f"""DROP SCHEMA IF EXISTS {self.schema};""")
        self.infoLogger(message=f"Creating schema: '{schema_name}'.")
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
        self.conn.commit()
        self.successLogger(message=f"Schema '{schema_name}' created OK.")



    def create_table(self):
        table_name = self.table
        self.cursor.execute(f"""DROP TABLE IF EXISTS {self.schema}.{self.table};""")
        self.infoLogger(message=f"Creando table '{table_name}'.")
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                TripID INT,
                customerID INT,
                LastModifiedTime DATETIME
            )
        """)
        self.conn.commit()
        self.successLogger(message=f"Table '{table_name}' created.")



    def insert_data(self, triptid: int, customerid: int, datetime: datetime):
        schema_name = self.schema
        table_name = self.table
        self.infoLogger(message=f"Insert data in '{table_name}'.")
        query = f"INSERT INTO {schema_name}.{table_name} values ( %s, %s ,%s )"
        self.cursor.execute(query, (triptid, customerid, datetime))
        self.conn.commit()
        self.successLogger(message=f"Insert data in '{table_name}' OK.")



    def insert_watermark(self, datetime, commit=True):
        table_name = self.table
        schema_name = self.schema
        self.infoLogger(message=f"Initial insert in '{self.watermark}'.")
        query = f"INSERT INTO {schema_name}.{self.watermark} values ( %s, %s )"
        self.cursor.execute(query, (table_name, datetime))
        if commit:
            self.conn.commit()
        self.successLogger(message=f"Initial insert in '{self.watermark}' OK.")


    def update_watermark(self, datetime, commit=True):
        table = self.table
        self.infoLogger(message=f"Update Watermark '{self.procedure}' using datetime: {datetime} in table: {table}.")
        call_query = f"CALL {self.procedure}( %s, %s )"
        self.cursor.execute(call_query, (datetime, table))
        self.successLogger(message=f"Update Watermark  '{self.procedure}' OK.")
        if commit:
            self.conn.commit()


    def create_watermark_table(self):
        watermark_name = self.watermark
        self.cursor.execute(f"""DROP TABLE IF EXISTS {self.schema}.{self.watermark};""")
        self.infoLogger(message=f"Creando Watermark '{watermark_name}'.")
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{watermark_name} (
                TableName VARCHAR(100),
                WatermarkValue DATETIME
            )
        """)
        self.conn.commit()
        self.successLogger(message=f"Watermark '{watermark_name}' created.")



    def create_stored_procedure(self):
        schema_name = self.schema
        procedure_name = self.procedure
        watermark_name = self.watermark
        self.infoLogger(message=f"Stored Procedure '{procedure_name}'.")
        self.cursor.execute(f"DROP PROCEDURE IF EXISTS {self.schema}.{self.procedure}")
        self.cursor.execute(f"""
            CREATE PROCEDURE {schema_name}.{procedure_name} (
                IN LastModifiedtime DATETIME, 
                IN TableName VARCHAR(100)
            )
            BEGIN
                UPDATE {schema_name}.{watermark_name} 
                SET WatermarkValue = LastModifiedtime 
                WHERE TableName = TableName;
            END
        """)
        self.conn.commit()
        self.successLogger(message=f"Stored Procedure '{procedure_name}' created.")


    def query(self, table ,rows):
        self.infoLogger(message=f"Show '{table}'.")
        self.cursor.execute(f"SELECT * FROM {table} LIMIT {rows}")
        rows = self.cursor.fetchall()
        for row in rows:
            print(row)

    
    def get_folder(self, date_process):
        try:
            datetime_file = date_process.strftime('%H-%M-%S')
            date_file = date_process.strftime('%Y-%m-%d')
            folder = self.dir + f'/incremental_load/{self.schema}/{self.table}/{self.table}_{date_file}/'
            print(f'Folder: {folder}')
            os.makedirs(folder, exist_ok=True)
            return folder + f'{datetime_file}.csv'

        except Exception  as e:
            print(f"Error: {logger.error(self.bcolors.RED+ str(e)  +self.bcolors.RESET)}")

    
    def infoLogger(self, message):
        logger.info(self.bcolors.YELLOW+ message  +self.bcolors.RESET)
    def successLogger(self, message):
        logger.info(self.bcolors.GREEN+ message  +self.bcolors.RESET)



    def connect(self):
        try:
            self.conn = mysql.connector.connect(
                    user=self.user, 
                    password=self.password, 
                    host=self.host, 
                    port=str(self.port)
                )
            self.cursor = self.conn.cursor()
        except Exception  as e:
            print(f"Error: {logger.error(self.bcolors.RED+ str(e)  +self.bcolors.RESET)}")


    def get_atomic_operation(self, operation_watermark: str, query: str, values: Tuple[Any], date: datetime, close_conn:bool=False):
        logger.info(self.bcolors.YELLOW+ f'Atomic Operation with {operation_watermark} in WatermarkTable'+self.bcolors.RESET)
        self.cursor.execute(query.format(table=self.table), values)
        data = self.cursor.fetchall()
        column_names = [desc[0] for desc in self.cursor.description]
        try:
            # Atomic Operation
            df = pd.DataFrame(data, columns=column_names)
            folder = self.get_folder(date)
            df.to_csv(folder, index=False)
            if operation_watermark == 'update':
                self.update_watermark(datetime=date, commit=False)
            if operation_watermark == 'insert':
                self.insert_watermark(datetime=date, commit=False)
        except Exception as e:
            self.conn.rollback()
            print(f'Error: {logger.error(self.bcolors.RED+ str(e)  +self.bcolors.RESET)}')
        finally:
            self.conn.commit()
            self.cursor.execute(f'SELECT * FROM {self.watermark}')
            watermark_data = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]
            print(F'Columns: {column_names}')
            print('Final row in WatermarkTable:')
            for row in watermark_data:
                print(row)
            if close_conn:
                self.conn.close()


    def run(self):
        self.infoLogger(message="Iniciando el proceso..")
        self.connect()
        self.create_schema()
        self.cursor.execute(f"USE {self.schema}")
        self.create_table()
        self.create_watermark_table()
        self.create_stored_procedure()

        self.insert_data(
            triptid=100,
            customerid=200,
            datetime = '2024-09-08 12:54:22'
        )
        self.insert_data(
            triptid=101,
            customerid=201,
            datetime = '2024-09-08 12:54:22'
        )

        self.cursor.execute(f"SELECT MAX(LastModifiedTime) FROM {self.table}")
        max_datetime_first_insert = self.cursor.fetchone()[0]
        print(f'Max datetime from Table to update WatermarkTable with first insert: {max_datetime_first_insert}')

        self.get_atomic_operation(
            operation_watermark='insert', 
            query="""SELECT * FROM {table} WHERE LastModifiedTime <= %s""", 
            values=(max_datetime_first_insert,), 
            date=max_datetime_first_insert,
            close_conn=False
        )


        self.insert_data(
            triptid=102,
            customerid=202,
            datetime='2024-09-08 18:54:22'
        )
        self.insert_data(
            triptid=103,
            customerid=203,
            datetime='2024-09-08 18:54:22'
        )

        self.cursor.execute(f"SELECT MAX(LastModifiedTime) FROM {self.table}")
        max_datetime_second_insert = self.cursor.fetchone()[0]
        print(f'Max datetime from Table to update WatermarkTable with second insert: {max_datetime_second_insert}')

        # Obtenemos la fecha del ultimo watermark actualizado y lo comparamos con 
        # el LastModifiedTime de la tabla principal:
        self.cursor.execute(f"SELECT WatermarkValue FROM {self.watermark} WHERE TableName = '{self.table}'")
        last_wateremarkvalue = self.cursor.fetchone()[0]
        print(f"Watermark: {last_wateremarkvalue}, LastModified: {max_datetime_second_insert}")

        if max_datetime_second_insert > last_wateremarkvalue:
            print('New rows...')
            self.get_atomic_operation(
                operation_watermark='update', 
                query="""
                    SELECT * FROM {table}
                    WHERE LastModifiedTime > %s
                    AND LastModifiedTime <= %s
                """, 
                values=(last_wateremarkvalue, max_datetime_second_insert), 
                date=max_datetime_second_insert,
                close_conn=True
            )
            


if __name__ == "__main__":
    obj = WaterMarkProcess(
        host=host, user=user, password=password, port=port, table=table, schema=schema
        )
    obj.run()
    print("Finish WaterMarkProcess")


