import time
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import redis
import os
from elasticsearch import Elasticsearch
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
import requests

DIRECTORY_TO_WATCH = "/home/firstuser/allpics/"
HAPROXY_URL = 'http://localhost:80/'
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379


class Watcher:

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        # why recursive? what is the best option?
        self.observer.schedule(event_handler, DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()


def get_file_name_with_suffix(event):
    full_file_basename = os.path.basename(str(event))
    splited_file_name_with_suffix = full_file_basename.split('.')
    return splited_file_name_with_suffix[0]


def get_file_basename_and_delete_both_parts_from_directory(file_base_name):
    for file in os.listdir(DIRECTORY_TO_WATCH):
        file_full_path = os.path.join(DIRECTORY_TO_WATCH, file)
        file_name_without_suffix = get_file_name_with_suffix(file_full_path)[:-2]
        if file_name_without_suffix == file_base_name:
            os.remove(file_full_path)


def run_over_directory_files():
    for img in os.listdir(DIRECTORY_TO_WATCH):
        img_full_path = os.path.join(DIRECTORY_TO_WATCH, img)
        get_full_path_and_do_everything(img_full_path)


def get_full_path_and_do_everything(img_full_path):
    redis_connection = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    file_name_without_suffix = get_file_name_with_suffix(img_full_path)[:-2]
    # return 0 if not exist and 1 if exist
    if redis_connection.exists(file_name_without_suffix) == 0:
        redis_connection.setex(file_name_without_suffix,60, str(img_full_path).split('/')[-1])
    else:
        second_file_name = redis_connection.get(file_name_without_suffix)
        is_first_file_or_second = get_file_name_with_suffix(img_full_path)[-1]
        second_file_path = os.path.join(DIRECTORY_TO_WATCH, second_file_name.decode('ascii'))
        if is_first_file_or_second == 'a':
            file_list = [('file', open(str(img_full_path), 'rb')), ('file', open(second_file_path, 'rb'))]
        else:
            file_list = [('file', open(second_file_path, 'rb')), ('file', open(str(img_full_path), 'rb'))]
        response = requests.post(url=HAPROXY_URL, files=file_list)
        if response.status_code == 200:
            send_log_to_elastic("the files sent successfully")
        else:
            send_log_to_elastic("the files didn't sent successfully")
        get_file_basename_and_delete_both_parts_from_directory(file_name_without_suffix)


def send_log_to_elastic(message):
    es = Elasticsearch(hosts=['http://20.54.249.197:9200'])
    doc = {
        'message': message,
        'timestamp': datetime.now(),
    }
    res = es.index(index="http-logs", body=doc)


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_created(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            print("Received created event - %s." % event.src_path)
            send_log_to_elastic('new file got into pure-ftpd server folder')
            get_full_path_and_do_everything(event.src_path)


if __name__ == '__main__':
    w = Watcher()
    executor = ProcessPoolExecutor(5)
    future = executor.submit(w.run(), ("Completed"))

