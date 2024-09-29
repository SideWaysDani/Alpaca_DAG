from datetime import datetime, timedelta
import math
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta, date
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.subprocess import SubprocessHook
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context
from configparser import ConfigParser
from pandas.tseries.offsets import Day, BDay
import configparser
import psycopg2
from polygon import RESTClient
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import savgol_filter, find_peaks, argrelextrema
import requests
import json

schema_name_global = "war_iter_4_2"

# config = configparser.ConfigParser()
# config.read('trade_configuration.ini')
# # stop_loss
# stop_loss_perc_for_unassigning_global = int(config['TradeConfig']['stop_loss'])
# # set_limit
# threshold_perc_for_setting_limit_global = int(
#     config['TradeConfig']['set_limit'])

PreviousNumberOfDaysToIncludeForFetchingLeads = 5

# # stop loss
# threshold_perc_for_unassigning_global = 4
# # setting limit percentage
# threshold_perc_for_setting_limit_global = 3


class PostgresConnection:
    def __init__(self):
        self.host = 'sthub.c3uguk04fjqb.ap-southeast-2.rds.amazonaws.com'
        self.database = 'postgres'
        self.user = 'stpostgres'
        self.password = 'stocktrader'
        self.connection = None

    def connect(self):
        print('Connecting to the PostgreSQL database...')
        try:
            params = {
                'host': self.host,
                'database': self.database,
                'user': self.user,
                'password': self.password
            }
            self.connection = psycopg2.connect(**params)
            return self.connection
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            return None

    def close_connection(self):
        """ Close the PostgreSQL database connection """
        if self.connection is not None:
            self.connection.close()
            print('Database connection closed.')

    def test_connection(self):
        """ Test the connection by fetching the PostgreSQL version """
        if self.connection is None:
            print('Connection not established.')
            return

        try:
            # create a cursor
            cur = self.connection.cursor()
            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')
            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)
            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
        finally:
            self.close_connection()

    def _config(self):
        """ Read connection parameters from a config file """
        parser = ConfigParser()
        parser.read(self.config_file)
        print(parser.has_section(self.section))
        db = {}
        if parser.has_section(self.section):
            params = parser.items(self.section)
            for param in params:
                db[param[0]] = param[1]
             # Print the configuration details after reading
            print(f"Configuration Details (section: {self.section}):")
            for key, value in db.items():
                print(f"\t{key}: {value}")
        else:
            raise Exception(
                f'Section {self.section} not found in the {self.config_file} file')
        return db


class GenericDBHelper:
    def __init__(self, conn):

        self.connection = PostgresConnection()
        self.conn = conn

    def select_all(self, table_name, schema_name=schema_name_global, columns='*', where_clause=None, where_values=None):
        """
        Select all records from the specified table with optional filtering.

        :param table_name: Name of the table to select from
        :param columns: Comma-separated list of column names to select (default is all columns)
        :param where_clause: Optional WHERE clause for filtering records
        :param where_values: Optional tuple of values to substitute in the WHERE clause
        :return: List of tuples containing the selected records
        """
        sql = f"SELECT {columns} FROM {schema_name}.{table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"

        result = []
        print(sql)
        print(where_values)
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, where_values)
                result = cur.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Database error: {error}")
            self.conn.rollback()
            raise
        return result

    def insert(self, table_name, columns: list, values_list: list):
        """
        Insert multiple records into the table.

        :param columns: List of column names
        :param values_list: List of tuples, each tuple representing a row of values to be inserted
        """
        columns_str = ', '.join(columns)
        # Generate the placeholder string for multiple rows
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO  {schema_name_global}.{table_name} ({columns_str}) VALUES ({placeholders});"
        try:

            with self.conn.cursor() as cur:
                # Execute multiple inserts using `executemany`
                cur.executemany(sql, values_list)
                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Insertion error: {error}")
            raise

    def update(self, table_name, set_columns, set_values, where_clause):
        conn = self.conn
        """ Update records in the table """
        print("set_columns---", set_columns)
        print("set_values-----", set_values)
        set_clause = ', '.join([f"{col} = %s" for col in set_columns])
        print("set_clause----", set_clause)
        sql = f"UPDATE  {schema_name_global}.{table_name} SET {set_clause} WHERE {where_clause};"
        print(sql)
        print(where_clause)
        try:

            with conn.cursor() as cur:
                cur.execute(sql, set_values)
                conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Update error: {error}")
            raise

    def delete(self, table_name, where_clause):
        conn = self.conn
        """ Delete records from the table """
        sql = f"DELETE FROM  {schema_name_global}.{table_name} WHERE {where_clause};"
        try:

            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Delete error: {error}")
            raise


'''added class for fetching the buy and sell signal for stocks from azure api'''


class StockAnalyzerUsingAzureAPI:
    # Class variables (shared across all instances)
    fetched_data = {}
    api_code = "TryM8ecL_3NA8n8CtLwgowLvm08BAHpC3Xp4_QwxtqTKAzFugvz0LQ=="

    @staticmethod
    def load_data_from_api(st_name, start_date="2020-6-01", end_date="2024-01-30"):
        url = "https://stapi02.azurewebsites.net/api/httpstsignals"
        params = {
            "code": StockAnalyzerUsingAzureAPI.api_code,
            "name": st_name,
            "start_date": start_date,
            "end_date": end_date
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx and 5xx)
            data = response.json()
            data = pd.DataFrame(data)
            data['Date'] = pd.to_datetime(data['Date'])
            data.set_index('Date', inplace=True)
            return data
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred: {req_err}")
        except Exception as e:
            print(f"An error occurred: {e}")
        return None

    @staticmethod
    def load_data(file_path):
        try:
            return pd.read_excel(file_path, parse_dates=["Date"], index_col="Date")
        except Exception as e:
            raise ValueError(f"Error loading data: {e}")

    @staticmethod
    def round_data(data, column_name):
        return data[column_name].round(3)

    @staticmethod
    def smooth_data(y_values, window_size=11, polyorder=3):
        if window_size % 2 == 0:
            window_size += 1  # window_size must be odd
        return savgol_filter(y_values, window_size, polyorder)

    @staticmethod
    def compute_derivative(smoothed_data, x_numerical):
        return np.gradient(smoothed_data, x_numerical).round(3)

    @staticmethod
    def find_extrema(smoothed_data, height=0.02, distance=10):
        peaks, _ = find_peaks(smoothed_data, height=height, distance=distance)
        troughs = argrelextrema(
            smoothed_data, np.less_equal, order=distance)[0]
        return peaks, troughs

    @staticmethod
    def collect_green_red_dots(smoothed_data, x_dates):
        peaks, troughs = StockAnalyzerUsingAzureAPI.find_extrema(smoothed_data)

        green_dots_df = pd.DataFrame({
            'Date': x_dates[peaks],
            'Color': 'green'
        })

        red_dots_df = pd.DataFrame({
            'Date': x_dates[troughs],
            'Color': 'red'
        })

        dots_df = pd.concat([green_dots_df, red_dots_df]).sort_values(
            by='Date').reset_index(drop=True)

        return dots_df

    @staticmethod
    def check_sell(start_date, end_date, data):
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        check_start = end_date - pd.Timedelta(days=2)
        check_start = max(check_start, start_date)

        mask = (data['Date'] >= check_start) & (data['Date'] <= end_date)
        relevant_data = data[mask]

        green_dates = relevant_data[relevant_data['Color'] == 'green']

        if not green_dates.empty:
            print(
                f"Sell opportunity detected on: {green_dates['Date'].dt.date.tolist()}")
            return True
        else:
            print("No sell opportunity found.")
            return False

    @staticmethod
    def check_buy(start_date, end_date, data):

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        check_start = end_date - pd.Timedelta(days=2)
        check_start = max(check_start, start_date)

        mask = (data['Date'] >= check_start) & (data['Date'] <= end_date)
        relevant_data = data[mask]

        red_dates = relevant_data[relevant_data['Color'] == 'red']

        if not red_dates.empty:
            print(
                f"Buy opportunity detected on: {red_dates['Date'].dt.date.tolist()}")
            return True
        else:
            print("No buy opportunity found.")
            return False

    @staticmethod
    def save_df_to_json(df, filename):
        """Append DataFrame to a JSON file. Creates the file if it doesn't exist."""
        file_path = f'{filename}.json'

        # Convert the DataFrame to JSON
        df_json = df.to_json(orient='records', date_format='iso')

        # Check if file exists
        try:
            # Read existing data
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            # File does not exist, start with an empty list
            existing_data = []

        # Convert the new data to a list of dictionaries
        new_data = json.loads(df_json)

        # Append new data
        existing_data.extend(new_data)

        # Write updated data to file
        with open(file_path, 'w') as file:
            json.dump(existing_data, file, indent=4)

        print(f"Data appended to {file_path}")

    @classmethod
    def analyze_stock(cls, sym_name, start_date, end_date):
        if sym_name in cls.fetched_data:
            print("Using cached data from already fetched data from api for", sym_name)
            dots_df = cls.fetched_data[sym_name]
        else:
            print("Fetching data from API. for stock", sym_name)
            data = cls.load_data_from_api(sym_name)
            if data is not None:
                data['H9'] = cls.round_data(data, 'H9')
                data['H14'] = cls.round_data(data, 'H14')

                x_dates = data.index
                smoothed_H9 = cls.smooth_data(data['H9'])
                smoothed_H14 = cls.smooth_data(data['H14'])

                cls.compute_derivative(smoothed_H9, np.arange(len(data)))
                cls.compute_derivative(smoothed_H14, np.arange(len(data)))

                dots_df = cls.collect_green_red_dots(
                    smoothed_data=smoothed_H9, x_dates=x_dates)
                cls.fetched_data[sym_name] = dots_df

                # Save the fetched data to a JSON file
                # cls.save_df_to_json(dots_df, sym_name)
            else:
                print("Failed to load data.")
                return {"sell_status": False, "buy_status": False}

        sell_status = cls.check_sell(start_date, end_date, data=dots_df)
        buy_status = cls.check_buy(start_date, end_date, data=dots_df)

        return {"sell_status": sell_status, "buy_status": buy_status}


# Define your default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'params': {
        "start_date": (date.today() - timedelta(days=35)).strftime("%Y-%m-%d"),
        "end_date": (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    }

}


@dag(dag_id='war_dag_test', schedule_interval=None, tags=['war_dag_test'], render_template_as_native_obj=True, default_args=default_args)
def war_dag_test():
    '''New functions added below'''
    ''''-----------------------------'''


    def update_assignment_status(conn, assignment_status, unit_ass_id):  # working

        db_helper = GenericDBHelper(conn)

        if not conn:
            print("Failed to connect to the database.")
            return

        try:
            db_helper = GenericDBHelper(conn)

            where_clause = f"unit_assignment_id = {unit_ass_id}"
            print(where_clause)
            result = db_helper.update(table_name='unit_assignment',
                                      set_columns=['assignment_status'],
                                      set_values=(assignment_status,),
                                      where_clause=where_clause)

            print("Assignment status from 'unit_assignment' table:")

        finally:
            conn.commit()

    # working
    def insert_into_allocation_history(conn, values_list: list[list]):
        db_helper = GenericDBHelper(conn)

        db_helper.insert(
            table_name='allocation_history',
            columns=['allocated_strength',	'opening_price',
                     'lead_id', 'closing_price',	'stock_quantity',	'p_and_l',
                     'valid_from_start_date',	'valid_to_end_date',	'unit_assignment_id', 'allocation_id', 'status', 'battle_date','deployment_id'
                     ],
            values_list=values_list
        )
        print(f"Allocation history inserted successfully.")

    def get_lead_name_id_from_allocation(db_helper: GenericDBHelper, depl_id):
        result = db_helper.select_all(columns='leads.stock_name,leads.id', table_name=f'''allocation JOIN  {schema_name_global}.deployment ON allocation.deployment_id = deployment.deployment_id
                            JOIN  stocktrader.leads ON deployment.lead_id = leads.id''', where_clause='allocation.deployment_id = %s', where_values=(depl_id,))

        leads_name = result[0][0]
        leads_id = result[0][1]
        return leads_id, leads_name

    """
    Inserts into allocation table and also update the deployment status or associated deployment to accepted
    """




    def insert_into_allocation(conn, values_list, battle_date_for_allocation_history):  # working

        db_helper = GenericDBHelper(conn)

        deployment_id = values_list[0][1]

        try:
            # Insert allocation record
            db_helper.insert(
                table_name='allocation',
                columns=[
                    'profit_and_loss',
                    'deployment_id',
                    'opening_price',
                    'closing_price',
                    'allocated_strength',
                    'stock_quantity',
                    'status'],
                values_list=values_list
            )
            print("Allocation record inserted successfully.")

            # Update deployment status (assuming a separate function exists)
            print(f'UPDATE Deployment id = {deployment_id} ')
            # Replace 'accepted' with desired status
            update_deployment_status(conn, deployment_id, status="accepted")
            print("Deployment status updated successfully!!!!!!!!!!!")

            # fetching allocation_id from allocation table based on deployment id
            allocation_id = fetch_allocation_id(
                conn=conn, deployment_id=deployment_id)
            sql = f"deployment_id = {deployment_id}"
            account = db_helper.select_all(
                table_name='deployment', where_clause=sql)

            unit_assignment_id = account[0][1]
            start_date_depl = account[0][5]
            end_date_depl = account[0][6]

            allocated_strength = values_list[0][4]
            opening_price = values_list[0][2]
            lead_id, lead_name = get_lead_name_id_from_allocation(
                db_helper=db_helper, depl_id=deployment_id)
            closing_price = values_list[0][3]
            stock_quantity = values_list[0][5]
            p_l = values_list[0][0]
            valid_from_start_date = start_date_depl
            valid_to_end_date = end_date_depl
            status = values_list[0][6]

            print("Inserting allocation history table")
            value_list = [[allocated_strength, opening_price, lead_id, closing_price,
                           stock_quantity, p_l, valid_from_start_date, valid_to_end_date, unit_assignment_id, allocation_id, status, battle_date_for_allocation_history,deployment_id]]
            print(f'Values received for allocation history{value_list}')
            insert_into_allocation_history(conn, value_list)

        except (Exception, psycopg2.DatabaseError) as error:

            print(
                f'values received in insert into allocation table{values_list[0]}')
            print(
                f"Error inserting allocation or updating deployment status: {error}")
            raise

    def insert_into_deployment(conn, values_list):  # working

        db_helper = GenericDBHelper(conn)

        db_helper.insert(
            table_name='deployment',
            columns=[
                'unit_assignment_id',
                'lead_id',
                'strength',
                'status',
                'start_date',
                'end_date',
                'account_id'],
            values_list=values_list
        )

        # deployment_id = values_list[0][0]

        # lead_id = values_list[0][2]
        # status = values_list[0][4]
        # valid_from_date = values_list[0][5]
        # valid_to_date = values_list[0][6]
        # unit_assignment_id = values_list[0][1]
        # strength = values_list[0][3]
        unit_assignment_id, lead_id, strength, status, valid_from_date, valid_to_date, account_id = values_list[
            0]
        lead_name = db_helper.select_all(
            table_name='leads', schema_name='stocktrader', columns='stock_name', where_clause='id = %s', where_values=(lead_id,))
        deployment_id = fetch_deployment_data(
            conn=conn, unit_assignment_id=unit_assignment_id)[0][0]
        values_list = [(lead_id, status,
                        valid_from_date, valid_to_date, unit_assignment_id, strength, deployment_id)]

        print(f"deployment {values_list[0]}added successfully.")
        insert_into_deployment_history(conn, values_list)

    def update_deployment(conn, deployment_id, columns_to_be_updated: list, new_values: list):  # working

        db_helper = GenericDBHelper(conn)

        try:
            db_helper.update(
                table_name='deployment',
                set_columns=columns_to_be_updated,
                set_values=new_values,
                where_clause=f"deployment_id = {deployment_id}"
            )
            print(
                f"Successfully updated deployment data for input columns {columns_to_be_updated} with values {new_values} for deployment {deployment_id}.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating deployment status: {error}")
            raise

        result = db_helper.select_all(
            table_name='deployment', where_clause=f"deployment_id = {deployment_id}")

        for row in result:
            print(row)

        lead_id = result[0][2]
        status = result[0][4]
        valid_from_date = result[0][5]
        valid_to_date = result[0][6]
        unit_assignment_id = result[0][1]
        strength = result[0][3]

        values_list = [[lead_id, status, valid_from_date,
                        valid_to_date, unit_assignment_id, strength, deployment_id]]
        print(
            f'Inserting deployment history with deplyoment status update{values_list}')
        insert_into_deployment_history(conn, values_list)

    def update_deployment_status(conn, deployment_id, status):  # working

        db_helper = GenericDBHelper(conn)

        result = db_helper.select_all(
            table_name='deployment', where_clause=f"deployment_id = {deployment_id}")

        for row in result:
            print(row)

        lead_id = result[0][2]
        prev_status = result[0][4]
        valid_from_date = result[0][5]
        valid_to_date = result[0][6]
        unit_assignment_id = result[0][1]
        strength = result[0][3]

        try:
            db_helper.update(
                table_name='deployment',
                set_columns=['status'],
                set_values=[status],
                where_clause=f"deployment_id = {deployment_id}"
            )
            print(
                f"Successfully updated deployment status for deployment {deployment_id}.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating deployment status: {error}")
            raise

        values_list = [[lead_id, status, valid_from_date,
                        valid_to_date, unit_assignment_id, strength, deployment_id]]
        print(
            f'Inserting deployment history with deplyoment status update{values_list}')
        insert_into_deployment_history(conn, values_list)

    def update_allocation(conn, allocation_id, columns_to_be_updated: list, new_values: list, start_date_for_allocation_history: date, end_date_for_allocation_history: date, current_battle_date):  # working

        db_helper = GenericDBHelper(conn)

        try:
            db_helper.update(
                table_name='allocation',
                set_columns=columns_to_be_updated,
                set_values=new_values,
                where_clause=f"allocation_id = {allocation_id}"
            )
            print(
                f"Successfully updated allocation for allocation {allocation_id}.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating deployment status: {error}")
            raise

        result = db_helper.select_all(
            table_name='allocation', where_clause=f"allocation_id = {allocation_id}")

        for row in result:
            print(row)

        allocated_strength = result[0][5]
        opening_price = result[0][3]
        closing_price = result[0][4]
        stock_quantity = result[0][6]
        p_l = result[0][1]
        deployment_id = result[0][2]
        status = result[0][7]
        lead_id, lead_name = get_lead_name_id_from_allocation(
            db_helper=db_helper, depl_id=deployment_id)
        valid_from_start_date = start_date_for_allocation_history
        valid_to_end_date = end_date_for_allocation_history

        sql = f"deployment_id = {deployment_id}"
        account = db_helper.select_all(
            table_name='deployment', where_clause=sql)
        unit_assignment_id = account[0][1]

        values_list = [[allocated_strength, opening_price, lead_id, closing_price,
                        stock_quantity, p_l, valid_from_start_date, valid_to_end_date, unit_assignment_id, allocation_id, status, current_battle_date,deployment_id]]
        print(
            f'Inserting into allocation history update')
        insert_into_allocation_history(conn, values_list)

    def insert_into_deployment_history(conn, values_list):  # working

        db_helper = GenericDBHelper(conn)

        db_helper.insert(
            table_name='deployment_history',
            columns=[
                'lead_id',
                'status',
                'valid_from_date',
                'valid_to_date',
                'unit_assignment_id',
                'strength',
                'deployment_id'],
            values_list=values_list
        )
        print(f"deployment history inserted successfully.")

    def remove_poor_allocations(db_helper: GenericDBHelper, allocation_ids: list):
        """
        Remove poor allocations from the allocation table based on their IDs.
        """
        if allocation_ids:
            ids_placeholder = ', '.join(['%s'] * len(allocation_ids))
            where_clause = f"allocation_id IN ({ids_placeholder})"
            try:
                db_helper.delete(table_name='allocation',
                                 where_clause=where_clause % tuple(allocation_ids))
                print(f"Removed allocations with IDs: {allocation_ids}")
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"Error removing poor allocations: {error}")
                raise

    def remove_poor_deployments(db_helper: GenericDBHelper, deployment_ids: list):
        """
        Remove poor deployments from the deployment table based on their IDs.
        """
        if deployment_ids:
            ids_placeholder = ', '.join(['%s'] * len(deployment_ids))
            where_clause = f"deployment_id IN ({ids_placeholder})"
            try:
                db_helper.delete(table_name='deployment',
                                 where_clause=where_clause % tuple(deployment_ids))
                print(f"Removed deployments with IDs: {deployment_ids}")
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"Error removing poor deployments: {error}")
                raise

    def insert_into_account_history(conn, values_list: list):
        db_helper = GenericDBHelper(conn)

        db_helper.insert(
            table_name='account_history',
            columns=[
                'account_name',
                'active_strength',
                'total_strength',
                'remaining_strength',
                'account_id',
                'battle_date'
            ],
            values_list=values_list
        )
        print(f"account history inserted successfully.")

    def update_account_table(conn, account_id, columns_to_be_updated: list, new_values: list, battle_date: datetime):
        db_helper = GenericDBHelper(conn)

        try:
            db_helper.update(
                table_name='account',
                set_columns=columns_to_be_updated,
                set_values=new_values,
                where_clause=f"account_id = {account_id}"
            )
            print(
                f"Successfully updated account with values {new_values}.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating deployment status: {error}")
            raise

        account_table_info = db_helper.select_all(
            table_name='account', columns='*', where_clause=f'account_id = {account_id}')
        account_id,	active_strength, user_id,	total_strength, remaining_strength = account_table_info[
            0]

        history_values_list = [[user_id, active_strength,
                                total_strength, remaining_strength, account_id, battle_date]]

        insert_into_account_history(conn=conn, values_list=history_values_list)

    def process_allocations_for_removing_them(allocations_to_be_removed: list, allocation_history_status: str, battle_date):
        """
        retieves their respective information for the deployment 
        adds the allocation detail to allocation history
        then removes the poor allocation
        adds poor deployment to deployment history
        removes the poo deployment from deployment table
        make the status of the unit assignment  unassigned
        account table update 
        insert into accoent history table
        """
        for poor_performer in allocations_to_be_removed:
            # code for getting start date and end date from deployment table
            allocation_id, profit_and_loss, deployment_id, opening_price, closing_price, allocated_strength, stock_quantity, allocation_status = poor_performer
            print("hello 3")

            deployment_result = db_helper.select_all(
                table_name='deployment',
                columns='*',
                where_clause='deployment_id = %s',
                where_values=(deployment_id,))

            print("hello 4")
            print(deployment_result)
            deployment_id, unit_assignment_id, lead_id, strength, depl_status, start_date, end_date, account_id = deployment_result[
                0]
            print("hello 5")
            # Check if a result was returned
            if deployment_result:
                print(
                    f"Start Date: {start_date}, End Date: {end_date}, Unit Assignment ID: {unit_assignment_id}")

                # deployment_ids.add(deployment_id)
                # unit_assignment_ids.add(unit_assignment_id)

                print("hello 6")
                allocation_history_status = allocation_history_status
                allocation_history_values_list = [[allocated_strength, opening_price, lead_id, closing_price,
                                                   stock_quantity, profit_and_loss, start_date, end_date, unit_assignment_id, allocation_id, allocation_history_status, battle_date,deployment_id]]
                print("allocation history being inserted in process allocation")
                print(allocation_history_values_list)
                insert_into_allocation_history(
                    conn=conn, values_list=allocation_history_values_list)

                # removing poor allocation from allocations
                remove_poor_allocations(
                    db_helper=db_helper, allocation_ids=[allocation_id])
                print("hello 7")

                deployment_history_vlaues = [
                    [lead_id, depl_status, start_date, end_date, unit_assignment_id, strength, deployment_id]]

                insert_into_deployment_history(
                    conn=conn, values_list=deployment_history_vlaues)

                remove_poor_deployments(
                    db_helper=db_helper, deployment_ids=[deployment_id])

                account_id, active_strength, user_id, total_strength, remaining_strength = db_helper.select_all(
                    table_name='account', columns='*', where_clause=f'account_id = {account_id}')[0]

                print("active_strength before----", active_strength)
                print("remaining_strength before----", remaining_strength)
                print("total_strength before---------", total_strength)
                # cummulative strength after profit and loss being added to itt
                old_allocated_strength = allocated_strength
                # new allocated strength
                allocated_strength = allocated_strength + profit_and_loss

                active_strength = active_strength - old_allocated_strength
                remaining_strength = remaining_strength + \
                    allocated_strength  # new allocated strength
                total_strength += profit_and_loss

                print("active_strength after----", active_strength)
                print("remaining_strength after----", remaining_strength)
                print("total_strength---------", total_strength)

                account_update_values_list = [
                    active_strength, total_strength, remaining_strength]
                update_account_table(conn=conn, account_id=account_id, columns_to_be_updated=[
                    'active_strength', 'total_strength', 'remaining_strength'], new_values=account_update_values_list, battle_date=battle_date)
                # unasssgining the unit
                update_assignment_status(
                    conn=conn, assignment_status='unassigned', unit_ass_id=unit_assignment_id)
                print(
                    f'successfuly un assigned the unit asssignment id {unit_assignment_id}')

            else:
                print(
                    f"No deployment found for deployment_id: {deployment_id}")

    # set limit removal

    def check_setting_limit_remove_allocation(conn, battle_date, threshold_perc_for_setting_limit):
        """ 
        This function retrieves all allocations whose limits have reached the desired limit set (where closing price is greater than 
        threshold_perc_for_setting_limit (i.e 3% for now) than buying price),
        it saves the allocations that removed with allocation history status profit_limit removing
        calls the function process_allocations_for_removing_them which does this
        retieves their respective information for the deployment 
        adds the allocation detail to allocation history
        then removes the poor allocation
        adds poor deployment to deployment history
        removes the poo deployment from deployment table
        make the status of the unit assignment  unassigned
        """

        if not conn:
            print("Failed to connect to the database.")
            return

        db_helper = GenericDBHelper(conn)
        threshold_perc_for_unassigning = threshold_perc_for_setting_limit/100
        # if closing price drops 4 percent from buying price then in assign the lead from that unit
        # so the algo is if closing price < opening price - (threshold_perc_for_unassigning(i.e 4 for now) /100)

        # Fetch all allocations with negative profit and loss

        good_performer_allocations = db_helper.select_all(
            table_name='allocation',
            columns='*',
            where_clause=f"closing_price > opening_price+(opening_price*{threshold_perc_for_unassigning})"
        )
        print("hello 2")
        print("good_permorfer_allocations whose set limit has reached------",
              good_performer_allocations)
        # if nothing was returned then get out of the function
        if not good_performer_allocations:
            print("no allocations found whose set limit has reached ")
            return

        # Fetch all unit assignments with status 'unassigned'

        print("Profit and loss from 'allocation' table:")
        for row in good_performer_allocations:
            print(row)
        print("hello 2")

        print('sending to function to process allocations whose set limit reached')

        process_allocations_for_removing_them(
            good_performer_allocations, allocation_history_status='set_limit removing', battle_date=battle_date)

        print("hello8")
        return good_performer_allocations

    # stop loss removal

    def check_performance_remove_allocations_deployments(conn, battle_date, threshold_perc_for_unassigning: int = 4):
        """ 
        This function retrieves all allocations with low performance (where closing price is dropped less than 
        threshold_perc_for_unassigning (i.e 4% for now) then buying price),
        calls the function process_allocations_for_removing_them which does this
        retieves their respective information for the deployment 
        adds the allocation detail to allocation history
        then removes the poor allocation
        adds poor deployment to deployment history
        removes the poo deployment from deployment table
        make the status of the unit assignment  unassigned
        """

        if not conn:
            print("Failed to connect to the database.")
            return

        db_helper = GenericDBHelper(conn)
        threshold_perc_for_unassigning = threshold_perc_for_unassigning/100
        # if closing price drops 4 percent from buying price then in assign the lead from that unit
        # so the algo is if closing price < opening price - (threshold_perc_for_unassigning(i.e 4 for now) /100)

        # Fetch all allocations with negative profit and loss

        poor_performer_allocations = db_helper.select_all(
            table_name='allocation',
            columns='*',
            where_clause=f"closing_price < opening_price-(opening_price*{threshold_perc_for_unassigning})"
        )
        print("hello 2")
        print("poor_performer_allocations------", poor_performer_allocations)
        # if nothing was returned then get out of the function
        if not poor_performer_allocations:
            print("no poor performer allocations found")
            return

        # Fetch all unit assignments with status 'unassigned'

        print("Profit and loss from 'allocation' table:")
        for row in poor_performer_allocations:
            print(row)
        print("hello 2")

        print('sending to function to process poor performer allocations')

        process_allocations_for_removing_them(
            poor_performer_allocations, allocation_history_status='stop_loss removing', battle_date=battle_date)

        print("hello8")
        return poor_performer_allocations
        # finally:
        #     # Optionally, close the connection if it's no longer needed
        #     conn.close()

    # def get_lead_name_mapping_id_from_allocation(db_helper: GenericDBHelper):
    #     result = db_helper.select_all(columns='allocation.*,leads.stock_name,leads.leads_id', table_name=f'''allocation JOIN  {schema_name_global}.deployment ON allocation.deployment_id = deployment.deployment_id
    #                           JOIN  {schema_name_global}.leads ON deployment.lead_id = leads.leads_id''')
    #     if result:
    #         # print("result", result)
    #         # leads_names = [item[-2] for item in result]
    #         # leads_ids = [item[-1] for item in result]
    #         # allocations = [item[:-3] for item in result]
    #         # leads_name_mapping = dict(zip(depl_ids, leads_names))
    #         return result
    #     else:
    #         print("no allocation found in allocation table")
    #         return

    def remove_allocation_to_sell_using_StockAnalyzerUsingAzureAPI(dbhelper: GenericDBHelper, conn, battle_date):

        lead_and_allocations_data = get_lead_name_mapping_id_from_allocation(
            conn=conn)

        allocations_to_sell = []
        if lead_and_allocations_data:
            for result in lead_and_allocations_data:
                stock_name = result[-2]
                buy_sell_status = StockAnalyzerUsingAzureAPI.analyze_stock(
                    sym_name=stock_name, start_date=battle_date, end_date=battle_date)
                sell_status = buy_sell_status["sell_status"]
                if sell_status:
                    # appending only
                    allocation_part = result[:-2]
                    allocation_id = result[0]
                    print("signal to sell this stock its stock name is ",
                          result[-2], "lead id is", result[-1], "allocation id is", allocation_id)
                    allocations_to_sell.append(allocation_part)
                else:
                    print("DIDn't recieve selling signal")
            process_allocations_for_removing_them(
                allocations_to_be_removed=allocations_to_sell, allocation_history_status='api_signal sell', battle_date=battle_date)
        else:
            print("no allocation found in allocation table")
            return

    def get_lead_name_mapping_id_from_allocation(conn):
        query = f'''select allocation.*,leads.stock_name,leads.id from {schema_name_global}.allocation JOIN  {schema_name_global}.deployment ON allocation.deployment_id = deployment.deployment_id
                            JOIN  stocktrader.leads ON deployment.lead_id = leads.id'''
        cur = conn.cursor()
        # Execute a simple SQL query
        # query = "SELECT * FROM stocktrader.stocks_leads WHERE start_date = %s"

        # query = "SELECT * FROM stocktrader.stocks_leads"
        cur.execute(query)

        # Fetch the result
        result = cur.fetchall()
        print(result)

        if result:
            # print("result", result)
            # leads_names = [item[-2] for item in result]
            # leads_ids = [item[-1] for item in result]
            # allocations = [item[:-2] for item in result]
            # print("allocations are ....", allocations)
            # leads_name_mapping = dict(zip(depl_ids, leads_names))
            return result
        else:
            print("no result found")
            return

    # result  = get_lead_name_mapping_id_from_allocation(conn)

    def filter_leads_for1_to_1_correspondance(conn, dbhelper, leads):
        lead_allocation_mapping = get_lead_name_mapping_id_from_allocation(
            conn=conn)
        if lead_allocation_mapping:
            current_allocated_leads_names = [item[-2]
                                             for item in lead_allocation_mapping]

            print("current_allocated_leads_names----",
                  current_allocated_leads_names)

            # leads = [lead[0] for lead in leads]
            print(leads)

            # Create a new list of leads that are not in current_allocated_leads_names
            filtered_leads = [
                lead for lead in leads if lead[1] not in current_allocated_leads_names]

            print("filtered leads--", filtered_leads)

            #  convert each element to a tuple to match the input format like [('A',),('B',),('C',)]
            unique_leads_tuples = [(lead,) for lead in set(filtered_leads)]

            print('unique_tuples---0', unique_leads_tuples)

            return filtered_leads
        # if no allocation os available to compare and filter then filter simply return the
        else:
            return leads

    def get_trending_leads(conn, current_date, start_date, end_date, **kwargs):
        '''returns list of tuples [(id,lead_name)]'''
        # Simulate fetching trending leads data

        cur = conn.cursor()
        # Execute a simple SQL query
        # query = "SELECT * FROM stocktrader.stocks_leads WHERE start_date = %s"

        # query = "SELECT * FROM stocktrader.stocks_leads WHERE %s BETWEEN stocks_leads.start_date AND stocks_leads.end_date"
        query = "SELECT id,stock_name FROM stocktrader.leads WHERE leads.lead_date BETWEEN %s AND %s and leads.endorsement = 'Yes'"

        # query = "SELECT * FROM stocktrader.stocks_leads"
        cur.execute(query, (start_date, end_date,))

        # Fetch the result
        result = cur.fetchall()
        print("fetched leads")
        print(result)
        # print("result--------------------------end")

        db_helper = GenericDBHelper(conn=conn)
        filtered_result = result
        filtered_result = filter_leads_for1_to_1_correspondance(
            dbhelper=db_helper, leads=result, conn=conn)

        print("filtered_result---", filtered_result)

        if not filtered_result:
            return None

        return filtered_result

    def get_units(conn, fetch: str):

        db_helper = GenericDBHelper(conn)
        sql = ''
        if fetch == 'unassigned':
            sql = "assignment_status = 'unassigned'"
        elif fetch == 'asssigned':
            sql = "assignment_status = 'assigned'"
        elif not fetch == '':
            sql = ''

        try:
            units = db_helper.select_all(
                table_name='unit_assignment', where_clause=sql)
            print(units)
            print(f"number of {fetch} units---", len(units))
            return units
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error fetching unassigned units: {error}")
            raise
            return None

    #  function to check total remaining strength

    def checking_total_remaining_strength(conn):
        db_helper = GenericDBHelper(conn)
        try:

            total_remaining_strength = 0
            accouts_info = db_helper.select_all(table_name='account')
            for account in accouts_info:
                total_remaining_strength += account[4]
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error fetching top leads----: {error}")
            raise

        return total_remaining_strength

    def get_polygon_data(battle_date, leads, unassigned_units, origin: str):
        print("battle date----", battle_date)
        print("leads-----------", leads)
        stock_names = []
        if origin == 'trends':
            leads_data = [(l[0], l[1]) for l in leads]
            sorted_data = leads_data

            print("sorted data 0", sorted_data)
            print("unassigned_units------------------", unassigned_units)
            length_of_unassigned_units = len(unassigned_units)
            print("length_of_unassigned_units-----------",
                  length_of_unassigned_units)

            # sorted_data = list(dict.fromkeys(leads))

            if len(sorted_data) > length_of_unassigned_units:
                sorted_data = sorted_data[:length_of_unassigned_units]
                print("sorted data--------------", sorted_data)

            # for stock in sorted_data:
            #     stock_names.append(stock[0])

            values_list_leads = []
            if isinstance(battle_date, str):
                battle_date = datetime.strptime(battle_date, "%Y-%m-%d")

            from_date = (battle_date - timedelta(days=1)).strftime("%Y-%m-%d")
            to_date = (battle_date).strftime("%Y-%m-%d")
            print("from_date--------", from_date)
            print("to_date-----------", to_date)

            # sorting the data

            for stock_id, stock_name in sorted_data:

                # api_key is used
                print('stock_name----------', stock_name)
                print('stock_id----------', stock_id)
                client = RESTClient("x2WHlSdeMaaSJLsYgck_sVSdMFSAaNpu")
                dataRequest = client.get_aggs(ticker=stock_name,
                                              multiplier=1,
                                              timespan='day',
                                              from_=from_date,
                                              to=to_date)
                priceData = pd.DataFrame(dataRequest)
                if not priceData.empty:
                    priceData["lead id"] = stock_id
                    priceData['Stock name'] = stock_name

                    priceData['Date'] = priceData['timestamp'].apply(
                        lambda x: pd.to_datetime(x*1000000))

                    priceData['Date'] = pd.to_datetime(
                        priceData['timestamp'] * 1000000).dt.strftime('%Y-%m-%d')

                    print("price_data_list before filter---", priceData)
                    # extracted data of 2 dates but taking 1st one
                    # Filter priceData based on the battle_date

                    filtered_data = priceData[priceData['Date']
                                              == battle_date.strftime("%Y-%m-%d")]

                    price_data_list = filtered_data.values.tolist()
                    if price_data_list:
                        price_data_list = price_data_list[0]
                        print(price_data_list)

                        print("price data list---", price_data_list)
                        # filling with name, date ,open price , close price
                        values_list_leads.append(
                            (price_data_list[9], price_data_list[10], price_data_list[11], price_data_list[0], price_data_list[3]))

                # print((stock_name[1],'\n',priceData['Date'],'\n',priceData['open'],'\n',priceData['close']))

                # values_list_leads.append((stock_name[1],priceData['Date'],priceData['open'],priceData['close']))

                print(
                    "polygon data for filling the leads table ------------------------")
                print(values_list_leads)
            # Simulate fetching polygon data

        elif origin == 'assigned units':
            print("assigned units")
            stock_names = leads

            values_list_leads = []
            if isinstance(battle_date, str):
                battle_date = datetime.strptime(battle_date, "%Y-%m-%d")

            from_date = (battle_date - timedelta(days=1)).strftime("%Y-%m-%d")
            to_date = (battle_date).strftime("%Y-%m-%d")
            print("from_date--------", from_date)
            print("to_date-----------", to_date)

            print("stock_names---", stock_names)
            # sorting the data

            for stock_name in stock_names:

                # api_key is used
                print('stock_name----------', stock_name)
                client = RESTClient("x2WHlSdeMaaSJLsYgck_sVSdMFSAaNpu")
                dataRequest = client.get_aggs(ticker=stock_name,
                                              multiplier=1,
                                              timespan='day',
                                              from_=from_date,
                                              to=to_date)
                priceData = pd.DataFrame(dataRequest)
                if not priceData.empty:
                    priceData['Stock name'] = stock_name
                    priceData['Date'] = priceData['timestamp'].apply(
                        lambda x: pd.to_datetime(x*1000000))

                    priceData['Date'] = pd.to_datetime(
                        priceData['timestamp'] * 1000000).dt.strftime('%Y-%m-%d')

                    print("price_data_list before filter---", priceData)
                    # extracted data of 2 dates but taking 1st one
                    # Filter priceData based on the battle_date

                    filtered_data = priceData[priceData['Date']
                                              == battle_date.strftime("%Y-%m-%d")]

                    price_data_list = filtered_data.values.tolist()
                    if price_data_list:
                        price_data_list = price_data_list[0]
                        print(price_data_list)

                        print("price data list---", price_data_list)
                        # filling with name, date ,open price , close price
                        values_list_leads.append(
                            (price_data_list[9], datetime.strptime(price_data_list[10], "%Y-%m-%d"), float(price_data_list[0]), float(price_data_list[3])))

                # print((stock_name[1],'\n',priceData['Date'],'\n',priceData['open'],'\n',priceData['close']))

                # values_list_leads.append((stock_name[1],priceData['Date'],priceData['open'],priceData['close']))

                print(
                    "polygon data for filling the leads table ------------------------")
                print(values_list_leads)
            # Simulate fetching polygon data

        return {"values_list_leads": values_list_leads}

    # def fill_leads_data_with_polygon(conn, list_leads_data):
    #     # Simulate filling leads data with polygon data
    #     print("list_leads_data----------", list_leads_data)
    #     list_leads_data = list_leads_data["values_list_leads"]
    #     table_name = f' {schema_name_global}.leads'
    #     columns = ['stock_name', 'lead_date', 'opening_price', 'closing_price']
    #     db_helper = GenericDBHelper(conn)

    #     # Execute the query with the list of tuples
    #     db_helper.insert(table_name='leads', columns=columns,
    #                      values_list=list_leads_data)

    #     print("values inserted in leads table")

    def analysing_units_to_assign_leads(conn, unassigned_units, battle_date):
        # Simulate assigning unassigned units to leads and filling deployment table

        db_helper = GenericDBHelper(conn)
        hardcoded_account_id = 1

        if not unassigned_units:
            print("No unassigned units found. Skipping assignment.")
            return

        # fetching account strength from account table

        total_remaining_strength = checking_total_remaining_strength(conn)

        num_unassigned_units = len(unassigned_units)
        strength_to_allocate_each_unit = total_remaining_strength/num_unassigned_units

        # ----------------------------------------------------------

        return strength_to_allocate_each_unit, total_remaining_strength

    # def fetch_closing_prices(conn, lead_id, battle_date):
    #     db_helper = GenericDBHelper(conn)
    #     closing_price = db_helper.select_all(
    #         table_name='leads', columns='closing_price', where_clause='lead_date = %s and leads_id = %s', where_values=(battle_date, lead_id))
    #     return closing_price

    def fetch_deployment_data(conn, unit_assignment_id):
        db_helper = GenericDBHelper(conn)
        depl_id = db_helper.select_all(
            table_name='deployment', where_clause='unit_assignment_id = %s', where_values=(unit_assignment_id,))

        return depl_id

    def fetch_allocation_id(conn, deployment_id):
        db_helper = GenericDBHelper(conn)
        alloc = db_helper.select_all(
            table_name='allocation', where_clause='deployment_id = %s', where_values=(deployment_id,))
        alloc_id = alloc[0][0]
        print(f'Allocation id fetched = {alloc_id}')

        return alloc_id

    def isBusinessDay(date):
        bday = BDay()
        is_business_day = bday.is_on_offset(date)
        print(is_business_day)

        print(date)
        is_business_day = bday.is_on_offset(date)

        print(is_business_day)
        return is_business_day

    def nextBusinessDay(date):
        bday = BDay()
        return date + 1*bday

    def calculate_profit_and_loss(closing_price, opening_price, stock_quantity):

        print(type(opening_price))
        print(type(closing_price))

        # cumulative profit and loss
        p_and_l = (closing_price*stock_quantity)-(opening_price*stock_quantity)

        print("prift adn loss*------", p_and_l)
        return p_and_l

    def calculate_profit_and_loss_percent(closing_price, opening_price, stock_quantity):

        print(type(opening_price))
        print(type(closing_price))
        total_initial_stock_value = opening_price*stock_quantity
        p_and_l = (closing_price*stock_quantity)-(opening_price*stock_quantity)
        p_and_l_percent = (p_and_l/total_initial_stock_value)*100
        print("prift adn loss*------", p_and_l)
        print("opening price * stock quantity %:*------",
              total_initial_stock_value)
        print("profit and loss %:*------", p_and_l_percent)
        return p_and_l_percent

    def filling_summary_table(conn, battle_date):
        db_helper = GenericDBHelper(conn)

        total_allocated_strength = db_helper.select_all(
            table_name='allocation', columns='SUM(allocated_strength)')
        total_allocated_strength = total_allocated_strength[0][0]

        # Fetch daily profit and loss
        profit_and_losses = db_helper.select_all(
            table_name='performance', columns='SUM(profit_and_loss)', where_clause='battle_date = %s', where_values=(battle_date,))
        cumulative_pandl = profit_and_losses[0][0]

        print('Total Allocated Strength', total_allocated_strength)
        cumulative_percentage_profit_and_loss = (
            cumulative_pandl*100)/total_allocated_strength

        columns = ['battle_date', 'cumulative_profit_and_loss',
                   'cumulative_percentageprofitandloss']

        db_helper.insert(table_name='summary', columns=columns,
                         values_list=[(battle_date, cumulative_pandl, cumulative_percentage_profit_and_loss)])

        print('summary table filled !!!!!<----')

    def inserting_into_performace(conn, values_list: list):
        db_helper = GenericDBHelper(conn)
        columns = ['unit_assignment_id', 'battle_date',
                   'profit_and_loss', 'lead_id', 'valid_to', 'valid_from', 'allocation_id', 'percentageprofitandloss']
        db_helper.insert(table_name='performance',
                         columns=columns, values_list=values_list)
        print("inserted into performace table")


    def get_sectors_from_trending_leads(conn, trending_leads):
        """
        Fetches sectors for trending leads from the stocktrader.fortune_1000 table.

        Args:
            conn: A database connection object.
            trending_leads: A list of trending leads.

        Returns:
            A list of tuples containing stock names and their corresponding sectors.
        """

        sector_list = []
        for lead_id, stock_name in trending_leads:
            cursor = conn.cursor()
            cursor.execute("SELECT sector FROM stocktrader.fortune_1000 WHERE ticker = %s", (stock_name,))
            sector = cursor.fetchone()
            if sector:
                sector_list.append((lead_id, stock_name, sector[0]))
            else:
            # Handle the case where the ticker is not found in stocktrader.fortune_100
                print(f"Stock {stock_name} not found in stocktrader.fortune_100")
            cursor.close()

        return sector_list


    def get_active_sectors(battle_date):
        """
        Fetches active sectors for a given battle date.

        Args:
            conn: A database connection object.
            battle_date: The current battle date.

        Returns:
            A list of active sector names.
        """

        active_sectors = db_helper.select_all(
            table_name="active_sectors",
            columns="sector_name",
            where_clause = "active_from <= %s AND active_to >= %s AND (status = 'yes' OR status = 'Automated')",
            where_values=(battle_date, battle_date),
        )

        return active_sectors


    def filter_active_sectors(sector_list, active_sectors):
        """
        Filters the sector list to retain only sectors present in active_sectors.

        Args:
            sector_list: A list of tuples containing stock names and sectors.
            active_sectors: A list of tuples containing sector names.

        Returns:
            A list of tuples containing stock names and sectors that are active.
        """

        active_sector_names = {sector[0] for sector in active_sectors}
        filtered_sector_list = [(lead_id, stock_name, sector) for lead_id, stock_name, sector in sector_list if sector in active_sector_names]
        return filtered_sector_list    

    def check_for_account_strength_update(conn,db_helper,account_id,current_battle_date):
        latest_account_history = db_helper.select_all(
        table_name='account_history',
        columns='*',
        where_clause=f'account_id = {account_id}'
        )

        if latest_account_history:
            # print(f'Account History', latest_account_history[-1])
            account_history_id, account_name, total_strength,account_id, remaining_strength,active_strength, battle_date = latest_account_history[-1]
            # print(total_strength)
            # print(remaining_strength)
            # print(battle_date)

            account_id, current_active_strength, user_id, current_total_strength, current_remaining_strength = db_helper.select_all(
                    table_name='account', columns='*', where_clause=f'account_id = {account_id}')[0]
            # print(current_total_strength)
            # print(current_remaining_strength)


            if (current_total_strength != total_strength):
                user_id = "Manual Strength Update"
                print("Manual Account Strength Updated , adding new entry to account_history")
                history_values_list = [[user_id, current_active_strength,
                                current_total_strength, current_remaining_strength, account_id, current_battle_date]]

                insert_into_account_history(conn=conn, values_list=history_values_list)        
        
    @task()
    def process_dates(conn, dates: dict):

        if not conn:
            print("Failed to connect to the database.")
            return

        dates_list = []
        hardcoded_account_id = 1
        dates_list = dates["dates"]

        print("dates_list---------------", dates_list)
        db_helper = GenericDBHelper(conn)

        # threshold_perc_for_unassigning = 4
        for date in dates_list:
            print("current---------------dates----------", date)
            current_date = date
            check_for_account_strength_update(conn=conn, db_helper=db_helper,account_id=hardcoded_account_id,current_battle_date=current_date)

            if not isBusinessDay(current_date):
                print(f"this is not a businessday so skipping")
                continue
            battle_date = current_date.strftime("%Y-%m-%d")
            start_date_for_trend = (
                current_date - timedelta(days=PreviousNumberOfDaysToIncludeForFetchingLeads)).strftime("%Y-%m-%d")

            # this function calls the analyzerstock class to check and sell the stock received from azure API
            remove_allocation_to_sell_using_StockAnalyzerUsingAzureAPI(
                dbhelper=db_helper, conn=conn, battle_date=battle_date)

            # stop loss
            # check_poor_performers_results = check_performance_remove_allocations_deployments(
            #     conn=conn, threshold_perc_for_unassigning=stop_loss_perc_for_unassigning_global, battle_date=battle_date)

            # set limit sell
            # check_set_limit_profit_results = check_setting_limit_remove_allocation(
            #     conn=conn, battle_date=battle_date, threshold_perc_for_setting_limit=threshold_perc_for_setting_limit_global)
            # current_date = datetime.strptime(date_str, "%Y-%m-%d")

            unassigned_units = get_units(conn=conn, fetch='unassigned')
            num_unassigned_units = len(unassigned_units)

            total_units = get_units(conn=conn, fetch='')
            num_total_units = len(total_units)

            if unassigned_units:
                trending_leads = get_trending_leads(
                    conn=conn, current_date=battle_date, start_date=start_date_for_trend, end_date=battle_date)
            # # Check if trending_leads is empty and skip if so
            # if not trending_leads:
            #     print(f"No trending leads found for date: {battle_date}. Skipping...")
            #     continue
                if not trending_leads and num_unassigned_units == num_total_units:
                    print(
                        f'skipping this day because there are no leads and total units are {num_total_units} and total number of unassigned units are {num_unassigned_units}')
                    continue

                if not trending_leads :
                    print(
                        f'skipping this day because there are no leads')
                    continue
                
                print(f'trending list',trending_leads)

       
                # Get sectors from trending leads
                sector_list = get_sectors_from_trending_leads(conn, trending_leads)
                print(f"sector_list = ",sector_list)

                # Get active sectors for the current battle date
                active_sectors = get_active_sectors( battle_date)
                print(f"active_sectors = ", active_sectors)

                # Filter sector list to keep only active sectors
                filtered_sector_list = filter_active_sectors(sector_list, active_sectors)
                print(f"filtered_sector_list = ", filtered_sector_list)

                if len(filtered_sector_list) < 1:
                    print('No Leads remaining after active sector fitering so skipping assignment for the current battle')
                    continue
                # Create a new list containing only the stock names from filtered sectors
                trending_leads = [(lead_id, stock_name) for lead_id, stock_name, _ in filtered_sector_list]
                print(f'Active Sector Leads', trending_leads)

                if len(trending_leads) > 0:
                    polygon_data = get_polygon_data(
                        battle_date=current_date, unassigned_units=unassigned_units, leads=trending_leads, origin='trends')

                    # filled_leads_data = fill_leads_data_with_polygon(
                    #     conn=conn, list_leads_data=polygon_data)

                

                leads_data_from_table = polygon_data["values_list_leads"]

                
                print("length of leads_data_from_table",
                      len(leads_data_from_table))
                print(leads_data_from_table)

            # if no unassigned units then no need to fetch leads so leads_data_from_table variable is empty
            if not unassigned_units:
                print("No unassigned units found. Skipping fetching leads.")
                leads_data_from_table = []

            num_unassigned_units = len(unassigned_units)
            num_assigned_units = len(total_units) - num_unassigned_units

            

            if num_assigned_units == 0 and not leads_data_from_table:
                print(
                    'skipping this day because no assigned unit and no lead datafound from table')
                continue
            
            

            # #handle scenario where leads_data_from_table length is less than 1
            # if len(leads_data_from_table)==0 or num_unassigned_units==0:
            #     print("assigning same leads as in previous battle date")

            # Handle scenario where leads are less than unassigned units

            lead_index_to_fetch = 0
            leads_fetched_already = []
            for unit in total_units:
                print("we are in the loop")
                print("unit starting is--------", unit)
                unit_assignment_status = unit[3]
                print("unit_assignment_status--", unit_assignment_status)
                unit_assignment_id = unit[0]

                if unit_assignment_status == 'assigned':

                    print("already assigned unit ")
                    print("unit_assignment_id---", unit_assignment_id)

                    deployment_data_assigned_unit = fetch_deployment_data(
                        conn=conn, unit_assignment_id=unit_assignment_id)

                    print("deployment_data_assigned_unit----",
                          deployment_data_assigned_unit)
                    deployment_id, unit_assignment_id, lead_id, strength, depl_status, start_date_depl, end_date_depl, account_id = deployment_data_assigned_unit[
                        0]

                    # update old end_date to current battle date
                    update_deployment(conn=conn, deployment_id=deployment_id, columns_to_be_updated=[
                        'end_date'], new_values=[battle_date])
                    print("current battle date--- ", battle_date)

                    lead_name = db_helper.select_all(
                        table_name='leads', schema_name='stocktrader', columns='stock_name', where_clause="id = %s", where_values=(lead_id,))[0][0]

                    print("lead name---", lead_name)
                    print("leads_fetched_already---", leads_fetched_already)
                    # if lead_name not in leads_fetched_already:
                    print("current_date type", type(current_date))
                    polygon_data = get_polygon_data(
                        battle_date=current_date, unassigned_units=unassigned_units, leads=[lead_name], origin='assigned units')
                    if polygon_data['values_list_leads']:
                        leads_fetched_already.append(lead_name)
                        print("leads_fetched_already---",
                              leads_fetched_already)
                        # fill the lead data with new date
                        # filled_leads_data = fill_leads_data_with_polygon(
                        #     conn=conn, list_leads_data=polygon_data)
                        lead_name, lead_date, new_lead_opening_price, new_lead_closing_price = polygon_data[
                            "values_list_leads"][0]

                    # elif lead_name in leads_fetched_already:
                    #     print(
                    #         f'lead name{lead_name} already in already fetched leads{leads_fetched_already} ')
                    #     fetched_lead = db_helper.select_all(
                    #         table_name='leads', columns='*', where_clause='stock_name = %s and lead_date = %s', where_values=(lead_name, battle_date))[0]
                    #     leads_id, lead_name, lead_date, new_lead_opening_price, new_lead_closing_price = fetched_lead

                    # fetch allocation id based on deployment id
                    allocation_id_for_update = fetch_allocation_id(
                        conn=conn, deployment_id=deployment_id)

                    # fetching previous allocation id's data
                    allocaion_data = db_helper.select_all(
                        table_name='allocation', columns='*', where_clause='allocation_id = %s', where_values=(allocation_id_for_update,))[0]
                    prev_allocation_opening_price = allocaion_data[3]

                    print("prev_allocation_opening_price",
                          prev_allocation_opening_price)

                    prev_allocation_closing_price = allocaion_data[4]
                    print("prev_allocation_closing_price",
                          prev_allocation_closing_price)
                    prev_stock_quantity = allocaion_data[6]
                    prev_stock_quantity = float(prev_stock_quantity)
                    allocation_strength = allocaion_data[5]

                    # checks if polygon has fetched the data of the lead on business day
                    if polygon_data["values_list_leads"]:

                        # new_lead_id = db_helper.select_all(
                        #     table_name='leads', columns='leads_id', where_clause="stock_name = %s and lead_date = %s", where_values=(lead_name, lead_date))[0]

                        # update_deployment(conn=conn, deployment_id=deployment_id, columns_to_be_updated=[
                        #     'lead_id'], new_values=[new_lead_id])

                        closing_price_for_current_date = float(
                            new_lead_closing_price)
                        print("in condition", "closing_price_for_current_date",
                              closing_price_for_current_date)
                        opening_price_for_current_date = float(
                            prev_allocation_opening_price)
                        print("in condition opening_price_for_current_date",
                              opening_price_for_current_date)

                    else:
                        # business day and polygon data not available
                        print(
                            f"polygon has not fetched data for {lead_name} for date {battle_date}")

                        closing_price_for_current_date = float(
                            prev_allocation_closing_price)
                        opening_price_for_current_date = float(
                            prev_allocation_opening_price)

                    # update_allocation_value(conn, allocation_id=allocation_id_for_update, column_name='opening_price',
                    #                         value=lead_opening_price, battle_date_for_allocation_history=battle_date)
                    profit_and_loss = calculate_profit_and_loss(
                        closing_price_for_current_date, opening_price_for_current_date, prev_stock_quantity)

                    profit_and_loss_percent = calculate_profit_and_loss_percent(
                        closing_price_for_current_date, opening_price_for_current_date, prev_stock_quantity)

                    print(
                        f"profit_and_loss for this date{battle_date}", profit_and_loss)
                    # allocation_strength = float(
                    #     allocation_strength)+float(profit_and_loss)

                    update_allocation(conn, allocation_id=allocation_id_for_update, columns_to_be_updated=['profit_and_loss', 'opening_price', 'closing_price'],
                                      new_values=[profit_and_loss, opening_price_for_current_date, closing_price_for_current_date], start_date_for_allocation_history=start_date_depl, end_date_for_allocation_history=end_date_depl, current_battle_date=battle_date)

                    performance_values_list = [
                        (unit_assignment_id, battle_date, profit_and_loss, lead_id, battle_date, battle_date, allocation_id_for_update, profit_and_loss_percent)]
                    inserting_into_performace(
                        conn=conn, values_list=performance_values_list)

                elif unit_assignment_status == 'unassigned' and leads_data_from_table:
                    print('leads_data_from_table', leads_data_from_table)
                    print("this unit is un assigned")
                    print('lead_index_to_fetch---------', lead_index_to_fetch)

                    current_lead = leads_data_from_table[lead_index_to_fetch]
                    lead_id = current_lead[0]
                    print('lead id ------', lead_id)

                    # fetch the lead data
                    # updating respective deployment data

                    strength_to_allocate_each_unit, total_remaining_strength = analysing_units_to_assign_leads(
                    unassigned_units=unassigned_units, battle_date=battle_date, conn=conn)
                    print("strength_to_allocate_each_unit---------",
                        strength_to_allocate_each_unit)

                    strength = math.floor(strength_to_allocate_each_unit)
                    current_date = battle_date
                    account_id = hardcoded_account_id

                    value_list = [(unit_assignment_id, lead_id, strength,
                                   'requested', current_date, current_date, account_id)]

                    insert_into_deployment(conn, value_list)

                    check_for_account_strength_update(conn=conn, db_helper=db_helper,account_id=hardcoded_account_id,current_battle_date=current_date)

                    # ---------------------------------------------------------
                    # updating account table
                    account_id, active_strength, user_id, total_strength, remaining_strength = db_helper.select_all(
                        table_name='account', columns='*', where_clause=f'account_id = {account_id}')[0]
                    print("active_strength before----", active_strength)
                    print("remaining_strength before----", remaining_strength)
                    active_strength = float(active_strength) + float(strength)
                    remaining_strength = float(
                        remaining_strength) - float(strength)
                    print("active_strength after----", active_strength)
                    print("remaining_strength after----", remaining_strength)
                    # total_strength += profit_and_loss
                    account_update_values_list = [
                        active_strength, remaining_strength]

                    update_account_table(conn=conn, account_id=account_id, columns_to_be_updated=[
                        'active_strength', 'remaining_strength'], new_values=account_update_values_list, battle_date=battle_date)

                    try:
                        # updating the status of deployment
                        print("unit status update krne tak tou agya")
                        update_assignment_status(
                            conn=conn, assignment_status='assigned', unit_ass_id=unit_assignment_id)

                        print(
                            f"Assigned unit {unit_assignment_id} to lead {lead_id}.")

                        # taking one lead from leads and assigning it to unit until units are assigned
                        leads_data_from_table.pop(lead_index_to_fetch)

                        # lead_index_to_fetch = (
                        #     lead_index_to_fetch+1) % (len(leads_data_from_table))

                    except (Exception, psycopg2.DatabaseError) as error:
                        print(
                            f"Error updating assignment status for unit {unit_assignment_id}: {error}")
                        raise

                    # fetching deployment id
                    deployment = fetch_deployment_data(conn=conn,
                                                       unit_assignment_id=unit_assignment_id)
                    deployment_id = deployment[0][0]

                    print(f'Deployment Id fetched {deployment_id}')
                    # update the deployment status based on deployment id
                    # deployment status is automatically updated when new allocation is created for the respective deployment_id

                    # save it in deployment history
                    # change is deployment status automatically adds new entry to deployment history to reflect the updated deployment status

                    # inserting to allocations hereeeeeeee

                    opening_price = current_lead[3]
                    closing_price = 0
                    print("opening_price-----", opening_price)
                    print("closing_price-----", closing_price)

                    stock_quantity = strength/opening_price
                    profit_and_loss = 0

                    values_list = [[profit_and_loss, deployment_id, opening_price,
                                    closing_price, strength, stock_quantity, 'materlized']]
                    insert_into_allocation(conn, values_list, battle_date)

                    

                    # inseritng to allocation history
                    # Insterting into allocation automatically adds corresponding allocation history entry

                    # fettching closing price

                    # closing_price = fetch_closing_prices(conn=conn,
                    #                                      lead_id=lead_id, battle_date=battle_date)
                    # # calculate profit and loss
                    # opening_price = db_helper.select_all(table_name='leads',
                    #                                      columns='opening_price', where_clause='leads_id = %s', where_values=(lead_id,))

                    # opening_price = opening_price[0][0]
                    closing_price = current_lead[4]
                    print("opening_price-----", opening_price)
                    print("closing_price-----", closing_price)

                    profit_and_loss = calculate_profit_and_loss(
                        closing_price, opening_price, stock_quantity)

                    profit_and_loss_percent = calculate_profit_and_loss_percent(
                        closing_price, opening_price, stock_quantity)

                    allocation_id = fetch_allocation_id(conn, deployment_id)

                    # day end strength of unit
                    # strength = float(strength)+float(profit_and_loss)

                    update_allocation(conn=conn, allocation_id=allocation_id, columns_to_be_updated=[
                        'closing_price', 'profit_and_loss'], new_values=[closing_price, profit_and_loss],
                        start_date_for_allocation_history=current_date, end_date_for_allocation_history=current_date, current_battle_date=battle_date)

                    performance_values_list = [
                        (unit_assignment_id, battle_date, profit_and_loss, lead_id, battle_date, battle_date, allocation_id, profit_and_loss_percent)]
                    inserting_into_performace(
                        conn=conn, values_list=performance_values_list)

                elif unit in unassigned_units and not leads_data_from_table:
                    print(
                        "we do not have leads and so we are not assigning any leads to un assigned unit---")
                    continue

                elif  not leads_data_from_table:
                    print(
                        "we do not have leads and so we are not assigning---")
                    continue

            # # fetching perfirmance details
            # profit_and_losses = db_helper.select_all(
            #     table_name='performance', columns='SUM(profit_and_loss)', where_clause='battle_date = %s', where_values=(battle_date,))
            # profit_and_losses = profit_and_losses[0][0]
            # print(profit_and_losses)

            # account_id = 1
            # # updating account table
            # account_id, active_strength, user_id, total_strength, remaining_strength = db_helper.select_all(
            #     table_name='account', columns='*', where_clause=f'account_id = {account_id}')[0]

            # whole_day_processed_active_strength = float(
            #     active_strength) + float(profit_and_losses)

            # # total_strength += profit_and_loss
            # account_update_values_list = [whole_day_processed_active_strength]
            # update_account_table(conn=conn, account_id=account_id, columns_to_be_updated=[
            #                      'active_strength'], new_values=account_update_values_list,battle_date=battle_date)

            # the summary table filling
            filling_summary_table(conn=conn, battle_date=battle_date)

            print(f"Processed leads for date: {battle_date}")

    @task()
    def access_params(**kwargs):
        # Retrieve parameters from the DAG run configuration
        params = get_current_context()

        parameters = params["params"]
        start_date_str = parameters["start_date"]
        print(type(start_date_str))
        end_date_str = parameters["end_date"]
        print(type(end_date_str))

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        current_date = start_date
        dates = []
        while current_date < end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)

        dates_dic = {"dates": dates}

        # Return the parameters
        return dates_dic

    # DAG workflow
    connection = PostgresConnection()
    conn = connection.connect()
    if not conn:
        print("Failed to connect to the database.")
        return
    db_helper = GenericDBHelper(conn)

    # parameters = access_params()

    # print("dates", parameters["start_date"],
    #       parameters["end_date"])
    dates = access_params()
    process_dates_task = process_dates(conn=conn, dates=dates)


War_Dag = war_dag_test()