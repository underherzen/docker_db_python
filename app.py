import time

import redis
from flask import Flask

import sqlite3

conn = sqlite3.connect('db.db', check_same_thread=False)
c = conn.cursor()

# get the count of tables with the name

app = Flask(__name__)
cache = redis.Redis(host='redis', port=6379)


def create_table():
    # get the count of tables with the name
    c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='counts' ''')
    # if the count is 1, then table exists
    if c.fetchone()[0] == 0:
        c.execute('''CREATE TABLE counts (
                        id INTEGER PRIMARY KEY,
                        count INTEGER NOT NULL DEFAULT 0
        );''')
        sql = '''INSERT INTO counts (count) VALUES (?)'''
        c.execute(sql, (0,))


def add_row():
    c.execute('''SELECT count(*) FROM counts''')
    if c.fetchone()[0] == 0:
        sql = '''INSERT INTO counts (count) VALUES (?)'''
        c.execute(sql, (0,))


def get_count_db():
    res = c.execute('''SELECT * FROM counts''')
    return res


def update_count_db(count):
    sql = '''UPDATE counts SET count=? WHERE id=1'''
    c.execute(sql, (count,))


def get_hit_count():
    retries = 5
    while True:
        try:
            return cache.incr('hits')
        except redis.exceptions.ConnectionError as exc:
            if retries == 0:
                raise exc
            retries -= 1
            time.sleep(0.5)


def decrement_count():
    retries = 5
    while True:
        try:
            return cache.decr('hits')
        except redis.exceptions.ConnectionError as exc:
            if retries == 0:
                raise exc
            retries -= 1
            time.sleep(0.5)


def hits_to_zero():
    retries = 5
    while True:
        try:
            cache.set('hits', 0)
            return cache.get('hits')
        except redis.exceptions.ConnectionError as exc:
            if retries == 0:
                raise exc
            retries -= 1
            time.sleep(0.5)


@app.route('/')
def hello():
    create_table()
    add_row()
    count = get_count_db().fetchone()[1]
    update_count_db(count + 1)
    result = 'Hello from Docker! I have been seen ' + str(count) + ' times (DB value).\nAnd' + str(
        get_hit_count()) + ' from cache'
    return result


@app.route('/decrement_count')
def decr():
    create_table()
    add_row()
    count = get_count_db().fetchone()[1]
    if count > 0:
        update_count_db(count - 1)
    else:
        update_count_db(0)
    result = 'Hello from Docker! I have been seen ' + str(count) + ' times (DB value).\nAnd' + str(
        decrement_count()) + ' from cache'
    return result


@app.route('/hits_to_zero')
def to_zero():
    create_table()
    add_row()
    update_count_db(0)
    result = 'Hello from Docker! I have been seen ' + str(0) + ' times (DB value).\nAnd ' + str(
        int(hits_to_zero())) + ' from cache'
    return result
