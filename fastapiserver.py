import hashlib

from fastapi import FastAPI, UploadFile, File
from datetime import datetime
from typing import List
import uvicorn
import os
from os import urandom
from elasticsearch import Elasticsearch
from requests import get
from Crypto.Cipher import AES

app = FastAPI()


def send_log_to_elastic(message):
    es = Elasticsearch(hosts=['http://20.54.249.197:9200'])
    doc = {
        'message': message,
        'timestamp': datetime.now(),
    }
    res = es.index(index="http-logs", body=doc)


@app.post("/")
def index(file: List[UploadFile] = File(...)):
    path = "/home/noa/unifiedfiles"
    file_name = file[0].filename
    splited_name = file_name.split('.')
    base_name = splited_name[0][:-2]
    fullpath = os.path.join(path, base_name + '.jpg')
    file1 = open(fullpath, "wb")
    file1.write(file[0].file.read())
    file1.write(file[1].file.read())
    with open(fullpath, "rb") as f:
        bytes = f.read()  # read entire file as bytes
        readable_hash = hashlib.sha512(bytes).hexdigest()
    file_for_secret_key = open('/home/noa/tornado.key', "rb")
    secret_key = file_for_secret_key.read()
    iv = urandom(16)
    obj = AES.new(secret_key, AES.MODE_CFB, iv)
    encd = obj.encrypt(readable_hash.encode("utf-8"))
    file1.write(iv)
    file1.write(encd)
    file1.close()
    send_log_to_elastic("wrote unified file")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

