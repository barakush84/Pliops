from datetime import datetime
from db_handler import DBHandler, DATABASE, RESOURCES_TABLE, REQUESTS_TABLE


def add_resource(name: str, type: str):
    with DBHandler() as db:
        not_exist = db.execute_command(f"SELECT * FROM {RESOURCES_TABLE} WHERE name='{name}' AND type='{type}'")
        if not not_exist:
            return f'Resource already exists'
        db.execute_command(f'INSERT INTO {RESOURCES_TABLE} (name, type) VALUES ("{name}", "{type}")', use_commit=True)
    return f'New resource {name} :: {type} added to resources list'


def get_all_resources():
    with DBHandler() as db:
        return db.execute_command(f'SELECT * FROM {RESOURCES_TABLE}')


def get_all_free_resources():
    with DBHandler() as db:
        return db.execute_command(f'SELECT * FROM {RESOURCES_TABLE} WHERE running_request IS NULL')


def assign_request_to_resource(resource, request):
    resource_name = resource['name']
    resource_type = resource['type']
    queued = request['queued']

    with DBHandler() as db:
        db.execute_command(f'UPDATE {RESOURCES_TABLE} WHERE name="{resource_name}" and type="{resource_type}" SET queued="{queued}"', use_commit=True)
        db.execute_command(f'DELETE FROM {REQUESTS_TABLE} WHERE queued="{queued}"', use_commit=True)

    return f'Request {queued} assigned to {resource_name} :: {resource_type}'


def add_request(job: str, run_type: str, priority: int):
    current_time = datetime.now().strftime("%Y-%m-%d %X")
    with DBHandler() as db:
        db.execute_command(f'INSERT INTO {REQUESTS_TABLE} '
                           f'(queued, job, run_type, priority) VALUES ("{current_time}", "{job}", "{run_type}", {priority})',
                           use_commit=True)
    return f'New request {job} :: {run_type} :: {current_time} added to requests list'


def get_all_requests():
    with DBHandler() as db:
        return db.execute_command(f'SELECT * FROM {REQUESTS_TABLE}')


def db_init():
    with DBHandler(is_init=True) as db:
        db.execute_command(f'DROP DATABASE IF EXISTS {DATABASE}')
        db.execute_command(f'CREATE DATABASE {DATABASE}')

    with DBHandler() as db:
        db.execute_command(f'DROP TABLE IF EXISTS {RESOURCES_TABLE}')
        db.execute_command(f'DROP TABLE IF EXISTS {REQUESTS_TABLE}')
        db.execute_command(f'CREATE TABLE {RESOURCES_TABLE} '
                           f'(name VARCHAR(255) NOT NULL, type VARCHAR(10) NOT NULL, running_request DATETIME)')
        db.execute_command(f'CREATE TABLE {REQUESTS_TABLE} (queued DATETIME, job VARCHAR(255), run_type VARCHAR(10), priority BIT)')
    return 'Database initialized'
