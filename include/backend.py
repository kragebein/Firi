#!/usr/bin/env python
''' Backend '''

import greenstalk
import sqlite3
import json
import time
from include.bean import Bean


DATABASE = 'messages.db'

TUBE_NAME = 'backend'
BEAN_HOST = '127.0.0.1'
BEAN_PORT = 8880


class Store():

    def __init__(self):
        ''' Initialize and sets up database for inserts.'''

        self.connection = sqlite3.connect(DATABASE)
        self.cursor = self.connection.cursor()
        query = "CREATE table IF NOT EXISTS \
            incoming(id integer primary key, name text)"
        self.cursor.execute(query)

    def put(self, value: str) -> int:
        ''' We put :value into database, and returns its
            location in the database using lastrow'''

        query = "INSERT INTO incoming (name) VALUES(:value)"
        insert = self.cursor.execute(query, (value,))
        self.connection.commit()

        return insert.lastrowid


class Backend():

    def __init__(self):
        self.r_queue = None  # Incoming response_queue
        self.sql = Store()
        self.wait_timer = 1  # Connection timer
        self.incoming = []  # Keep trakc of incoming messages
        self.outgoing = []  # Keep track of outgoing jobs

    def run(self):
        ''' Get and return data'''
        while True:
            bean = Bean()
            incoming = bean.consumer(
                BEAN_HOST, BEAN_PORT,
                TUBE_NAME)

            if isinstance(incoming, greenstalk.Job):
                # we received a job!
                self.wait_timer = 0
                message = json.loads(incoming.body)

                if message.get('response_queue', False):
                    # Checking if we have a response_queue in this dict
                    bean.client.delete(incoming)
                    incomingId = incoming.id
                    self.r_queue = message.get('response_queue', False)
                    name = message.get('field', False)
                    value = message.get(name, False)
                    insertid = self.sql.put(value)
                    BODY = {
                            'responseTo': incomingId,
                            'id': insertid
                            }

                    # Produce a new job, return data back to response_queue
                    sendt = bean.producer(
                        BEAN_HOST, BEAN_PORT,
                        self.r_queue,
                        json.dumps(BODY)
                        )

                    if isinstance(sendt, int):
                        self.outgoing.append(sendt)

                    elif sendt.get('error', False):
                        print(f'Got error while trying to send \
                                reply on tube {self.r_queue}')

                else:
                    # Remove invalid jobs.
                    bean.client.delete(incoming)

            elif isinstance(incoming, dict):
                # we received an error :(
                if incoming.get('error'):
                    print(
                        'Error:',
                        incoming.get('error'),
                        f' Retrying in {self.wait_timer} seconds'
                        )

                    time.sleep(self.wait_timer)
                    self.wait_timer += 2
