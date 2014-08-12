__author__ = 'dmedv'

import keystone_v2
import http.client
import configparser
import argparse
import urllib.parse
import json


class AuthorizationError(Exception):
    pass


def main():
    global config
    global token
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
        execute(args)
    elif args.command == 'upload':
        upload(args)
    elif args.command == 'status':
        status(args)
    elif args.command == 'cancel':
        cancel(args)
    elif args.command == 'submit':
        submit(args)
    else:
        print('Invalid command:', args.command)


def auth_retry(func):
    def wrapper(args):
        try:
            func(args)
        except AuthorizationError:
            update_token_from_keystone()
            func(args)
    return wrapper


@auth_retry
def execute(args):
    global config
    global token
    url = urllib.parse.urlparse(config.get('CasJobs', 'url'))
    conn = http.client.HTTPConnection(url.netloc)
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
def upload(args):
    global config
    global token
    url = urllib.parse.urlparse(config.get('CasJobs', 'url'))
    conn = http.client.HTTPConnection(url.netloc)
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
def status(args):
    global config
    global token
    url = urllib.parse.urlparse(config.get('CasJobs', 'url'))
    conn = http.client.HTTPConnection(url.netloc)
    conn.request('GET', url.path+'/RestApi/jobs/'+args.job_id,
                 headers={'Content-Type': 'application/json',
                          'Content-Length': '0',
                          'X-Auth-Token': token})
    response = conn.getresponse()
    if response.code == 401:
        raise AuthorizationError
    else:
        print(response.read().decode())


@auth_retry
def submit(args):
    global config
    global token
    url = urllib.parse.urlparse(config.get('CasJobs', 'url'))
    conn = http.client.HTTPConnection(url.netloc)
    request_body = json.dumps({'Query': args.query, 'CreateTable': args.create_table, 'TableName': args.table})
    conn.request('PUT', url.path+'/RestApi/contexts/'+args.context+'/jobs', request_body,
                 {'Content-Type': 'application/json',
                  'X-Auth-Token': token})
    response = conn.getresponse()
    if response.code == 401:
        raise AuthorizationError
    else:
        print(response.read().decode())


@auth_retry
def cancel(args):
    global config
    global token
    url = urllib.parse.urlparse(config.get('CasJobs', 'url'))
    conn = http.client.HTTPConnection(url.netloc)
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
    global config
    global token
    token = keystone_v2.get_token(config.get('Keystone', 'host'),
                                  config.get('Keystone', 'tenantname'),
                                  config.get('Keystone', 'username'),
                                  config.get('Keystone', 'password'))
    with open('token', 'wt') as f:
        f.write(token)


if __name__ == '__main__':
    main()