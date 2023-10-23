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
import json

import requests
import time
import traceback

def notifyRecommendService(host, port):
    print(f"notify recommend service {host}:{port}")
    max_wait = 300
    num = max_wait
    last_exception = None
    while num > 0:
        try:
            resp = requests.post(f'http://{host}:{port}/actuator/refresh')
        except Exception as ex:
            resp = None
            last_exception = ex
        if resp is not None and resp.status_code == 200:
            try:
                data = resp.json()
            except Exception as ex:
                data = None
                last_exception = ex
            if data is not None:
                # succeed: print and return
                print(data)
                return
        print(f"retry refresh recommend service! {host}:{port}")
        time.sleep(1)
        num -= 1
    if last_exception is not None:
        traceback.print_exception(last_exception)
    message = "fail to notify recommend service %s:%s after waiting %d seconds" % (host, port, max_wait)
    raise RuntimeError(message)


def healthRecommendService(host, port):
    try:
        resp = requests.get(f'http://{host}:{port}/actuator/health')
    except Exception as ex:
        return {
            "status": "WAIT",
            "resp": None,
            "msg": f"wait recommend service up ok, request ex:{ex.args}",
        }
    if resp is not None:
        if resp.status_code != 200:
            return {
                "status": "WAIT",
                "resp": resp,
                "msg": f"health check wait service start!, ret_code:{resp.status_code}",
            }
        try:
            data = resp.json()
        except Exception as ex:
            return {
                "status": "DOWN",
                "resp": resp,
                "msg": f"health check request resp parser fail, ex:{ex.args}",
            }
        if data is not None:
            status = data.setdefault("status", "DOWN")
            if status == "DOWN":
                return {"status": "DOWN", "resp": data, "msg": "health check fail!"}
            elif status == "OUT_OF_SERVICE":
                return {"status": "WAIT", "resp": data, "msg": "recommend has empty config! wait config"}
            return {"status": status, "resp": data, "msg": "health check successfully"}
    return {"status": "DOWN", "resp": None, "msg": "health check request fail, unknown"}


def tryRecommendService(host, port, scene, param=None):
    if param is None:
        param = {"user_id": "test"}
    try:
        header = {
            'Content-Type': 'application/json'
        }
        resp = requests.post(
            f'http://{host}:{port}/service/recommend/{scene}',
            headers=header,
            data=json.dumps(param),
        )
    except Exception as ex:
        return {
            "status": "DOWN",
            "resp": None,
            "msg": f"request scene:{scene} fail, ex:{ex.args}",
        }
    if resp is not None:
        if resp.status_code != 200:
            return {
                "status": "DOWN",
                "resp": resp,
                "msg": f"request scene:{scene}, ret_code:{resp.status_code}",
            }
        try:
            data = resp.json()
        except Exception as ex:
            return {
                "status": "DOWN",
                "resp": resp,
                "msg": f"request scene:{scene} request resp parser fail, ex:{ex.args}",
            }
        if data is not None:
            data["timeRecords"] = None
            code = data.setdefault("code", "UNKNOWN")
            if code != "SUCCESS":
                return {
                    "status": "DOWN",
                    "resp": data,
                    "msg": f"recommend request scene: {scene} fail",
                }
            result = data.get("data", [])
            if len(result) == 0:
                return {
                    "status": 'WAIT',
                    "resp": data,
                    "msg": f"recommend request scene: {scene} return empty result, wait data load",
                }
            data["data"] = None
            return {
                "status": 'UP',
                "resp": data,
                "msg": f"recommend request scene: {scene} successfully",
            }
    return {
        "status": "DOWN",
        "resp": None,
        "msg": f"recommend request scene: {scene} fail, unknown",
    }


if __name__ == "__main__":
    print("test")
    data = tryRecommendService("recommend-k8s-service-saas-demo.huawei.dmetasoul.com", 80, "guess-you-like")
    print(data)
