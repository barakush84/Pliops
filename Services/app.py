import logging

from flask import Flask, request
from flask.logging import default_handler
import db_connect
import validation
import time
from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)


@app.route('/')
def welcome():
    return 'Welcome to Pliops Lab Resources Manager!'


@app.route('/resources/add')
def add_resource():
    name = request.args.get('name', default=None, type=str)
    type = request.args.get('type', default=None, type=str)
    invalid = validation.resource_validation(name=name, type=type)
    if invalid:
        return invalid
    return db_connect.add_resource(name=name.lower(), type=type.lower())


@app.route('/resources/get_all')
def get_all_resources():
    return db_connect.get_all_resources()


@app.route('/requests/add')
def add_request():
    job = request.args.get('job', default=None, type=str)
    run_type = request.args.get('run_type', default=None, type=str)
    priority = request.args.get('priority', default=1, type=int)
    invalid = validation.request_validation(job=job, run_type=run_type, priority=priority)
    if invalid:
        return invalid
    return db_connect.add_request(job=job, run_type=run_type.lower(), priority=priority)


@app.route('/init')
def db_init():
    return db_connect.db_init()


executor = ThreadPoolExecutor(1)


def main():
    logger = logging.getLogger()
    logger.addHandler(default_handler)
    while True:
        all_request = db_connect.get_all_requests()
        all_free_resources = db_connect.get_all_free_resources()
        logger.info(all_request)
        logger.info(all_free_resources)
        for req in all_request:
            logger.info(req)
            has_available_resource = [x for x in all_free_resources if req['run_type'] == 'any' or req['run_type'] == x['type']]
            logger.info(has_available_resource)
            if has_available_resource:
                db_connect.assign_request_to_resource(req, has_available_resource[0])
                all_free_resources.pop(has_available_resource[0])

        time.sleep(2)


if __name__ == "__main__":
    db_connect.db_init()
    executor.submit(main)
    app.run(host='0.0.0.0')
