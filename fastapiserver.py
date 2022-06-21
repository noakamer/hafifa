import hashlib

from fastapi import FastAPI, UploadFile, File
from typing import List
import uvicorn
import os
from elasticsearch import Elasticsearch
from requests import get
from Crypto.Cipher import AES
import inotifiScript

DIRECTORY_PATH = "/home/noa/unifiedfiles"

app = FastAPI()


@app.post("/")
def index(file: List[UploadFile] = File(...)):
    base_file_name = file[0].filename
    file_name_without_suffix = base_file_name.split('.')[0][:-2]
    fullpath = os.path.join(DIRECTORY_PATH, file_name_without_suffix + '.jpg')
    unified_file = open(fullpath, "wb")
    unified_file.write(file[0].file.read())
    unified_file.write(file[1].file.read())
    with open(fullpath, "rb") as f:
        bytes = f.read()
        readable_hash = hashlib.sha512(bytes).hexdigest()
    secret_key = open('/home/noa/tornado.key', "rb").read()
    iv = urandom(16)
    obj = AES.new(secret_key, AES.MODE_CFB, iv)
    encoded_hash = obj.encrypt(readable_hash)
    unified_file.write(iv)
    unified_file.write(encoded_hash)
    unified_file.close()
    inotifiScript.send_log_to_elastic("wrote unified file")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

