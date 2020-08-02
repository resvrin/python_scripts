''' This script is for  usercreation to generate unix uid for new user creation in
inventory . The usage has been explained in the parser '''
import argparse
import datetime
import csv
import sys
import os
from contextlib import contextmanager

headers = ['user', 'gecos', 'rpm', 'ticket', 'uid']
filename = 'csvfile.csv'

def header_write(f_name, header):
    ''' Checking file empty if yes write header only onetime, then finally executed to count the uid'''
    try:
        with open(f_name, 'a', newline='') as f:
            if os.stat(f_name).st_size == 0:
                fieldnames = header
                writer = csv.DictWriter(f, fieldnames)
                writer.writeheader()
                writer.writerow({'user': 'last_user', 'gecos': 'last_email', 'rpm': 'last_team', 'ticket': 'last_ticket', 'uid': 56789})
    finally:
        data_num = get_last_line(f_name)
        yield next(data_num)



#Get the last UID

def get_last_line(f_name):
    filesize = os.path.getsize(f_name)
    blocksize = 1024
    with open(f_name, 'rb') as infile:
        if filesize > blocksize:
            max_seek_point = filesize // blocksize
            infile.seek(max_seek_point * blocksize)
        elif filesize:
            max_seek_point = blocksize % filesize
            infile.seek(max_seek_point)
        lines = infile.readlines()
        if lines:
            last_line = lines[-1].strip()
            data_uid = last_line.decode("utf-8")
            yield int(data_uid.split(',')[-1]) + 1

#Decorator to prime the function
def coroutine(fn):
    def inner(*args, **kwargs):
        f = fn(*args, **kwargs)
        next(f)
        return f
    return inner

@coroutine
def saved_data(f_name, header):
    with open(f_name, 'a' ) as f:
        fieldnames = header
        writer  = csv.DictWriter(f, fieldnames=fieldnames)
        while True:
            data_row = yield
            writer.writerow({'user': data_row[0], 'gecos': data_row[1], 'rpm': data_row[2],
                             'ticket': data_row[3], 'uid': data_row[4]})
#pull data from pipeline
@coroutine
def broadcaster(func):
    while True:
        data_row = yield
        func.send(data_row)

#Data processed then saving to file
@coroutine
def process_data():
    out_file = saved_data(filename, headers)
    broadcast = broadcaster(out_file)
    while True:
        data = yield
        broadcast.send(data)

#Using to close the file in exit of each function.
@contextmanager
def pipeline():
    p = process_data()
    try:
        yield p
    finally:
        p.close()

def final_send():
    with pipeline() as pipe:
        pipe.send(rows)


def func_list(all_user):
    with open(filename) as f:
        fieldnames = headers
        reader = csv.DictReader(f, fieldnames=fieldnames)
        for row in reader:
            print(row['user'], row['gecos'], row['rpm'], row['ticket'], row['uid'])

def func_search(usr_name):
    with open(filename) as f:
        fieldnames = headers
        reader = csv.DictReader(f, fieldnames=fieldnames)
        for row in reader:
            if usr_name in row['user']:
                print(row['user'], row['gecos'], row['rpm'], row['ticket'], row['uid'])



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Usercreation substitute for generating unix UID')
    sub_parser = parser.add_subparsers(help='UserCreation substitute to generate Unix UID')

    create_subparser = sub_parser.add_parser('create', help='Usage Eg: test5.py create -u myuser -g myuser@myuser.com -r , -t TASK84848')
    create_subparser.add_argument('-u', '--user', type=str,  help='Provide unix user name as per C standard', required=True)
    create_subparser.add_argument('-g', '--gecos', type=str, help='Provide email address', required=True)
    create_subparser.add_argument('-r', '--rpm', type=str, help="Provide the group Eg: teamname", required=True)
    create_subparser.add_argument('-t', '--ticket', type=str, help="Provide the Service Request Eg: TASK773737", required=True)

    list_subparser = sub_parser.add_parser('listuser', help='List all the users')
    list_subparser.add_argument('-l', type=func_list, help='Usage: UserCreation.py listuser -l all')

    search_subparser = sub_parser.add_parser('search', help='Search a user')
    search_subparser.add_argument('-s', type=func_search, help='Usage: UserCreation.py search -s username')
    args = parser.parse_args()

    try:
        if (args.user and args.gecos and args.rpm and args.ticket):
            rows = [args.user, args.gecos, args.rpm, args.ticket, next(header_write(filename, headers))]
            print(rows)
            final_send()
    except AttributeError as ex:
        print(ex)
        
