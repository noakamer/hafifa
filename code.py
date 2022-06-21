import time
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import redis
import os
from elasticsearch import Elasticsearch
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
from requests import get
import requests


class Watcher:
    DIRECTORY_TO_WATCH = "/home/firstuser/allpics/"

    def __init__(self):
        self.observer = Observer()

    def run(self):
        before_running_client(self.DIRECTORY_TO_WATCH)
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()


def full_file_name(event):
    full_path_of_the_file = str(event)
    file_name = os.path.basename(full_path_of_the_file)
    splited_name = file_name.split('.')
    return splited_name[0]


def before_running_client(path):
    url = 'http://localhost:80/'
    r = redis.Redis(host='127.0.0.1', port=6379)
    for img in os.listdir(path):
        full_path = os.path.join(path, img)
        file_name = full_file_name(full_path)[:-2]
        is_first_file_or_second = full_file_name(full_path)[-1]
        if is_first_file_or_second == 'a':
            r.set(file_name, str(full_path).split('/')[-1])
        elif is_first_file_or_second == 'b':
            url = 'http://localhost:80/'
            second_file_name = r.get(file_name)
            re = os.path.basename(str(full_path))
            basic_path = str(full_path)[:-len(re)]
            second_file_path = os.path.join(basic_path, second_file_name.decode('ascii'))
            file_list = [('file', open(str(full_path), 'rb')), ('file', open((second_file_path), 'rb'))]
            response = requests.post(url=url, files=file_list)
            print(response.status_code)
            print(response)
            os.remove(str(full_path))
            os.remove(second_file_path)


def send_log_to_elastic(message):
    es = Elasticsearch(hosts=['http://20.54.249.197:9200'])
    doc = {
        'message': message,
        'timestamp': datetime.now(),
    }
    res = es.index(index="http-logs", body=doc)


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            print("Received created event - %s." % event.src_path)
            send_log_to_elastic('new file got into pure-ftpd server folder')
            file_name = full_file_name(event.src_path)[:-2]
            is_first_file_or_second = full_file_name(event.src_path)[-1]
            r = redis.Redis(host='127.0.0.1', port=6379)
            if is_first_file_or_second == 'a':
                r.setex(file_name, 60, str(event.src_path).split('/')[-1])
            elif is_first_file_or_second == 'b':
                url = 'http://localhost:80/'
                second_file_name = r.get(file_name)
                re = str(event.src_path).split('/')[-1]
                basic_path = str(event.src_path)[:-len(re)]
                second_file_path = os.path.join(basic_path, second_file_name.decode('ascii'))
                file_list = [('file', open(second_file_path, 'rb')), ('file', open(str(event.src_path), 'rb'))]
                response = requests.post(url=url, files=file_list)
                print(response.status_code)
                if response.status_code == 200:
                    send_log_to_elastic("the files sent successfully")
                else:
                    send_log_to_elastic("the files didn't sent successfully")
                os.remove(str(event.src_path))
                os.remove(second_file_path)


if __name__ == '__main__':
    w = Watcher()
    executor = ProcessPoolExecutor(5)
    future = executor.submit(w.run(), ("Completed"))

