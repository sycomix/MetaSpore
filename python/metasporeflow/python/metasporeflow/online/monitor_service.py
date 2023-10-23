#
# Copyright 2022 DMetaSoul
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import requests
import time
import traceback
import hashlib
import base64
import hmac
import argparse
import tornado
from apscheduler.schedulers.tornado import TornadoScheduler

webhook_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/067fedf7-b7da-4d02-98b7-ea2709c83bd6'
webhook_secret = 'ZHNjODODPEa7kWD7ekoMBf'

def gen_sign(timestamp, secret):
  # 拼接timestamp和secret
  string_to_sign = f'{timestamp}\n{secret}'
  hmac_code = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
  return base64.b64encode(hmac_code).decode('utf-8')

def gen_content(msg_title, msg_body, msg_href, msg_at, msg_type):
  content = {}
  if msg_type == 'post':
    data = [{'tag': 'text', 'text': msg_body}]
    if msg_href:
        data.append({'tag': 'a', 'text': '点击链接', 'href': msg_href})
    if msg_at:
        data.append({'tag': 'at', 'user_id': msg_at})
    content[msg_type] = {
      'zh-cn': {
        'title': msg_title,
        'content': [data]
      }
    }
  elif msg_type == 'text':
    content[msg_type] = f'【{msg_title}】{msg_body}'
  return content

def push(url, secret, msg_title, msg_body, msg_href='', msg_at=''):
  msg_type = 'post' if msg_href or msg_at else 'text'
  if not msg_title and not msg_body:
      return None
  ts = str(int(time.time()))
  content = gen_content(msg_title, msg_body, msg_href, msg_at, msg_type)
  params = {
    'msg_type': msg_type,
    'content': content,
    'timestamp': ts,
    'sign': gen_sign(ts, secret)
  }
  r = requests.post(url, json=params, timeout=(7, 3))
  return r.json()

def restart_service(path):
  from metasporeflow.resources.resource_manager import ResourceManager
  from metasporeflow.resources.resource_loader import ResourceLoader
  from metasporeflow.online.online_executor import OnlineLocalExecutor
  print(f"restart service load config from {path}")
  resources = ResourceLoader().load(path)
  online_executor = OnlineLocalExecutor(resources)
  online_executor.down()
  online_executor.up()

def check_service_status(host, port):
  health_url = f"http://{host}:{port}/actuator/health"
  print(f"check service request {health_url}")
  resp = requests.get(health_url)
  if resp.status_code != 200:
    push(
        webhook_url,
        webhook_secret,
        '警报',
        f"recommend service[{host}:{port}] health request fail!",
    )
    return False
  try:
    data = resp.json()
    print(data)
    if data.get("status") != "UP":
      push(
          webhook_url,
          webhook_secret,
          '警报',
          f"recommend service[{host}:{port}] status is not UP!",
      )
      return False
    if data.get("components", {}).get("dataSource", {}).get("status") != "UP":
        push(webhook_url, webhook_secret, '警报', "recommend service[$s:%s] datasource status is not UP!" % (host, port))
        return False
  except Exception as ex:
    push(
        webhook_url,
        webhook_secret,
        '警报',
        f"recommend service[{host}:{port}] health resp parser fail! ex:{ex}",
    )
    return False
  return True

def status_service():
  host = "127.0.0.1"
  port = 13013
  path = ""
  if check_service_status(host, port):
    push(
        webhook_url,
        webhook_secret,
        '警报',
        f"recommend service[{host}:{port}] health is ok!",
    )
    print("test feishu")

def monitor_service():
    host = "127.0.0.1"
    port = 13013
    path = ""
    check_service_status(host, port)

if __name__ == "__main__":
    sched = TornadoScheduler()
    sched.add_job(status_service, 'interval', seconds=4*3600, id="1")
    sched.add_job(monitor_service, 'interval', seconds=5, id="2")
    sched.start()
    tornado.ioloop.IOLoop.instance().start()
