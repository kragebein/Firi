import traceback
import greenstalk


class Bean():

    def __init__(self):
        '''Greenstalk Client context'''
        self.client = None

    def producer(self, host, port, tube, body):
        try:
            client = greenstalk.Client((host, port), use=tube)

        except ConnectionRefusedError:
            return self.error('Connection Refused')

        id = client.put(body)
        return id  # Returns ID of inserted job.

    def consumer(self, host, port, tube):
        try:
            self.client = greenstalk.Client((host, port), watch=[tube])

        except ConnectionRefusedError:
            return self.error('Connection Refused')
        try:
            incoming = self.client.reserve()
        except ConnectionError:
            return self.error('Connection to beanstalkd has been lost')

        except ConnectionResetError:
            return self.error('Connection to backend has been reset by peer')

        except ConnectionRefusedError:
            return self.error('Unable to connect to beanstalkd.')

        except Exception:
            traceback.print_exc()
            return self.error('Generalized Exception has occured.')
        return incoming

    def consume_job(self, host, port, tube, id):
        try:
            self.client = greenstalk.Client((host, port), watch=[tube])

        except ConnectionRefusedError:
            return self.error('Connection Refused')
        try:
            incoming = self.client.reserve_job(id)
        except ConnectionError:
            return self.error('Connection to beanstalkd has been lost')

        except ConnectionResetError:
            return self.error('Connection to backend has been reset by peer')

        except ConnectionRefusedError:
            return self.error('Unable to connect to beanstalkd.')

        except Exception:
            traceback.print_exc()
            return self.error('Generalized Exception has occured.')
        return incoming

    def error(self, e):
        '''Returns error message as dict'''
        return {
            'job_ok': False,
            'error': e
        }
