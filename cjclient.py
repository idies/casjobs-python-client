__author__ = 'dmedv'

import keystone_v2
import http.client
import configparser
import argparse
import urllib.parse
import json
import time


token = None
config = None


class AuthorizationError(Exception):
    pass


def main():
    global config
    parser = argparse.ArgumentParser()
    parser.add_argument('command')
    parser.add_argument('-q', dest='query', metavar='QUERY')
    parser.add_argument('-x', dest='context', metavar='CONTEXT')
    parser.add_argument('-t', dest='table', metavar='TABLE')
    parser.add_argument('-fo', dest='output', metavar='FILENAME')
    parser.add_argument('-fi', dest='input', metavar='FILENAME')
    parser.add_argument('-j', dest='job_id', metavar='JOB_ID')
    parser.add_argument('-c', dest='create_table', action="store_true", help="create a new table")
    config = configparser.ConfigParser()
    config.read('cjclient.cfg')

    try:
        update_token_from_file()
    except FileNotFoundError:
        update_token_from_keystone()

    args = parser.parse_args()
    if args.command == 'execute':
        do_execute(args)
    elif args.command == 'upload':
        do_upload(args)
    elif args.command == 'status':
        do_status(args)
    elif args.command == 'cancel':
        do_cancel(args)
    elif args.command == 'job_async':
        do_job_async(args)
    elif args.command == 'job_sync':
        do_job_sync(args)
    else:
        print('Invalid command:', args.command)


def auth_retry(func):
    def wrapper(args):
        url = urllib.parse.urlparse(config.get('CasJobs', 'url'))
        conn = http.client.HTTPConnection(url.netloc)
        try:
            return func(args, url, conn)
        except AuthorizationError:
            update_token_from_keystone()
            return func(args, url, conn)
    return wrapper


@auth_retry
def do_execute(args, url, conn):
    global token
    request_body = json.dumps({'Query': args.query})
    conn.request('POST', url.path+'/RestApi/contexts/'+args.context+'/query', request_body,
                 {'Content-Type': 'application/json',
                  'X-Auth-Token': token})
    response = conn.getresponse()
    if response.code == 401:
        raise AuthorizationError
    elif response.code != 200:
        print(response.read().decode())
    else:
        result = response.read().decode()
        if args.output is not None:
            with open(args.output, 'wt') as f:
                f.write(result)
        else:
            print(result)


@auth_retry
def do_upload(args, url, conn):
    with open(args.input, 'rt') as f:
        request_body = f.read()
    conn.request('POST', url.path+'/RestApi/contexts/'+args.context+'/tables/'+args.table, request_body,
                 {'Content-Type': 'application/json',
                  'X-Auth-Token': token})
    response = conn.getresponse()
    if response.code == 401:
        raise AuthorizationError
    elif response.code != 200:
        print(response.read().decode())


@auth_retry
def get_job_status(job_id, url, conn):
    conn.request('GET', url.path+'/RestApi/jobs/'+job_id,
                 headers={'Content-Type': 'application/json',
                          'Content-Length': '0',
                          'X-Auth-Token': token})
    response = conn.getresponse()
    if response.code == 401:
        raise AuthorizationError
    else:
        return response.read().decode()


def do_status(args):
    print(get_job_status(args.job_id))


@auth_retry
def submit_job(args, url, conn):
    request_body = json.dumps({'Query': args.query, 'CreateTable': args.create_table, 'TableName': args.table})
    conn.request('PUT', url.path+'/RestApi/contexts/'+args.context+'/jobs', request_body,
                 {'Content-Type': 'application/json',
                  'X-Auth-Token': token})
    response = conn.getresponse()
    if response.code == 401:
        raise AuthorizationError
    else:
        return response.read().decode()


def do_job_async(args):
    print('Job {0} created'.format(submit_job(args)))


def do_job_sync(args):
    job_id = submit_job(args)
    print('Job {0} created\n...'.format(job_id))
    while True:
        status = json.loads(get_job_status(job_id))
        if status['Status'] >= 3:
            print('Job {0} finished with code {1}'.format(job_id, status['Status']))
            break
        time.sleep(5)


@auth_retry
def do_cancel(args, url, conn):
    conn.request('DELETE', url.path+'/RestApi/jobs/'+args.job_id,
                 headers={'Content-Type': 'application/json',
                          'Content-Length': '0',
                          'X-Auth-Token': token})
    response = conn.getresponse()
    if response.code == 401:
        raise AuthorizationError
    else:
        print(response.read().decode())


def update_token_from_file():
    global token
    with open('token', 'rt') as f:
        token = f.read()


def update_token_from_keystone():
    global token
    token = keystone_v2.get_token(config.get('Keystone', 'host'),
                                  config.get('Keystone', 'tenantname'),
                                  config.get('Keystone', 'username'),
                                  config.get('Keystone', 'password'))
    with open('token', 'wt') as f:
        f.write(token)


if __name__ == '__main__':
    main()