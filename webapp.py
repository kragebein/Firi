#!/usr/bin/env python


import json
import timeout_decorator
import greenstalk
from flask import Flask
from waitress import serve
from include.bean import Bean

app = Flask(__name__)

WEB_HOST = '127.0.0.1'
WEB_PORT = 8881
RESPONSE = 'webapp'
TUBE_NAME = 'backend'
BEAN_HOST = '127.0.0.1'
BEAN_PORT = 8880


class Messages():
    def __init__(self):
        self.pending = None
        self.bean = Bean()
        self.job = None

    # We have 1 seconds to complete the Job
    @timeout_decorator.timeout(1, use_signals=False)
    def sendAndWait(self, name: str) -> str:
        ''' Sends the reply and awaits the response response'''
        BODY = json.dumps(
            {
                'response_queue': RESPONSE,
                'field': 'name',
                'value': name
            }
        )
        # Sends the message,
        sendt = self.bean.producer(BEAN_HOST, BEAN_PORT, TUBE_NAME, BODY)
        # Add Job ID to self.pending, this is the exact return value we want
        self.pending = sendt
        while True:
            response = self.bean.consumer(BEAN_HOST, BEAN_PORT, RESPONSE)
            if isinstance(response, greenstalk.Job):
                message = json.loads(response.body)
                if message.get('responseTo', False) != self.pending:
                    # This is not the message we are looking for
                    # So we bury theese unil other instances
                    # can claim their rightful Job.
                    self.bean.client.bury(response)
                elif message.get('responseTo', False) == self.pending:
                    # THIS is the message we are looking for, return it.
                    self.bean.client.delete(response)
                    return json.dumps(message)


@app.route('/add/<name>', methods=(['GET', 'POST']))
def addThis(name: str) -> str:
    ''' Sends the message through the tube, returns rowID'''
    message = Messages()
    try:
        data = message.sendAndWait(name)
    except timeout_decorator.TimeoutError:
        return json.dumps(
            {
                'error':
                'Transaction timed out: No response from backend'
                }
            )

    return data


@app.errorhandler(404)
def error_404(e):
    return json.dumps({'error': 'not a valid route'}), 404


@app.errorhandler(500)
def error_500(e):
    return json.dumps({'error': 'Internal Server error'}), 500


# app.run(host='127.0.0.1', port='8889', debug=True)
print(f'Webapp running at {WEB_HOST}:{WEB_PORT} ')
serve(app, host=WEB_HOST, port=WEB_PORT)
