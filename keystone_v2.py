__author__ = 'dmedv'

import http.client
import json


def get_token(host, tenantname, username, password):
    conn = http.client.HTTPConnection(host)
    auth_request_body = json.dumps(
        {'auth': {'tenantName': tenantname,
                  'passwordCredentials': {'username': username,
                                          'password': password}}},
        indent=4)

    conn.request('POST', '/v2.0/tokens', auth_request_body, {'Content-Type': 'application/json'})
    return json.loads(conn.getresponse().read().decode())['access']['token']['id']