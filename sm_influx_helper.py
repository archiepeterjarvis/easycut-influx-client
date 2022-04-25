"""
@author Archie Jarvis

Created to work with InfluxDB 2+ and Python 2.7

Allows EC to create an InfluxDB bucket on a remote server securely for data transaction
"""

import datetime
import json
import time
import sqlite3
import requests

BASE_URL = 'http://127.0.0.1:8000/api/buckets?hostname='

TEST_BUCKET_ID = '96ae188fadfcfe7d'
TEST_AUTH_TOKEN = 'HougHckqOTib7ImQ0H7fJO08afdxK37tUSN44n5sFQhXWeGXxIDLtpzu1yCEsKmPb4O6sWdu73-IxVhIzj6gUw=='

"""
SECTION FOR CREATING AND STORING BUCKET DETAILS
"""


def create_local_database(db_name):
    conn = sqlite3.connect(db_name)


def create_local_bucket_database(db_name):
    conn = sqlite3.connect(db_name)

    query = "CREATE TABLE IF NOT EXISTS Buckets (id integer PRIMARY KEY, Name text NOT NULL, BucketId text NOT NULL, " \
            "AuthToken text NOT NULL);"

    conn.execute(query)
    conn.commit()


def store_bucket_details(db_name, bucket_name, bucket_id, auth_token):
    """
    STORE BUCKET DETAILS LOCALLY
    :param bucket_name:
    :param db_name:
    :param bucket_id: this machine's influx bucket ID
    :param auth_token: this machine's influx auth key for bucket
    :return: None
    """

    create_local_bucket_database(db_name)

    conn = sqlite3.connect(db_name)

    query = "INSERT INTO Buckets (Name, BucketId, AuthToken) VALUES ('%s', '%s', '%s')" % (bucket_name, bucket_id,
                                                                                           auth_token)

    conn.execute(query)
    conn.commit()


def get_new_bucket_details(hostname, bucket_type):
    """
    SENDS GET REQUEST FOR BUCKET DETAILS AND STORES RESULT LOCALLY ON MACHINE

    To be run once when Wi-Fi first available.

    :param bucket_type: brief description of bucket usage (event/status/job/etc)
    :param hostname: machine's local hostname
    :return: id of created bucket and relative auth token
    """

    url = BASE_URL + hostname + "&type=" + bucket_type
    data = send_bucket_get_request(url)
    data = json.loads(data)

    bucket_id = data['bucket_id']
    auth_token = data['auth_token']

    store_bucket_details(bucket_id, auth_token)

    return bucket_id, auth_token


def send_bucket_get_request(url):
    """
    SENDS GET REQUEST URL AND RETURNS RESULT IF STATUS OK
    :param url: url to send request to
    :return: json response (see example below)

    {
        u'auth_token': u'*',
        u'bucket_id': u'*'
    }
    """
    r = requests.get(url)

    if r.status_code == 200:
        return r.content


"""
EPOCH TIME FORMATTING
"""


def get_influx_time():
    return str(time.mktime(datetime.datetime.now().timetuple())).split('.')[0]


"""
SECTION FOR ENTERING INTO BUCKETS
"""

BASE_WRITE_URL = 'http://localhost:8086/api/v2/write?bucket=%s&org=org&precision=s'


def post_request(url, headers, data):
    """
    SEND POST REQUEST
    :param url: url to send request to
    :param headers: request headers
    :param data: request data
    :return: result of request
    """
    return requests.post(url=url, headers=headers, data=data)


def generate_line_protocol_from_kwargs(measurement, hostname, args):
    """
    GENERATES INFLUX LINE PROTOCOL FORMAT FROM ARGUMENTS
    :param hostname: machine's hostname
    :param measurement: measurement key
    :param args: ... (change to **kwargs for testing)
    :return: str of line protocol
    """

    # todo: make this cleaner

    line_protocol = measurement + ','

    line_protocol += 'hostname=%s' % hostname

    for key, value in args.iteritems():
        line_protocol += " %s=%s" % (key, value)

    line_protocol += " " + get_influx_time()

    return line_protocol


def post_machine_data(measurement, hostname, **kwargs):
    """
    SEND MACHINE DATA TO INFLUX
    :param hostname: machine's hostname
    :param measurement: measurement key
    :param kwargs: ...
    :return: status code
    """

    headers = {
        'Authorization': 'Token ' + TEST_AUTH_TOKEN,
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    url = BASE_WRITE_URL % TEST_BUCKET_ID

    data = generate_line_protocol_from_kwargs(measurement, hostname, args=kwargs)

    response = post_request(url, headers, data)

    if response.status_code != 204:
        print(response.content)

    return response.status_code
