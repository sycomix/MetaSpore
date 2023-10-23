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

TXT2TXT_SEARCH_SERVICE_URL = 'http://localhost:8080/qa/user/10'
TXT2IMG_SEARCH_SERVICE_URL = 'http://localhost:8080/t2i/user/10'

IMG2IMG_SEARCH_SERVICE_URL = 'http://172.31.0.197:8083/search'
IMG2TXT_SEARCH_SERVICE_URL = 'http://172.31.0.197:8083/classify'


def txt2txt_search_service(query, top_k=10):
    params = {'query': query}
    res = requests.post(TXT2TXT_SEARCH_SERVICE_URL, json=params).json()
    items = []
    if not res.get('searchItemModels'):
        return items
    items.extend(
        {
            'title': item['summary']['question'],
            'content': item['summary']['answer'],
            'url': '',
            'score': item['score'],
        }
        for item in res['searchItemModels'][0]
    )
    return items

def txt2img_search_service(query, top_k=10):
    params = {'query': query}
    res = requests.post(TXT2IMG_SEARCH_SERVICE_URL, json=params).json()
    items = []
    if not res.get('searchItemModels'):
        return items
    items.extend(
        {
            'title': item['summary']['name'],
            'content': f"""<img width="100" height="auto" src="{item['summary']['url']}" />""",
            'url': item['summary']['url'],
            'score': item['score'],
        }
        for item in res['searchItemModels'][0]
    )
    return items

def img2img_search_service(img, top_k=10):
    # not impl
    return []

def img2txt_search_service(img, top_k=10):
    # not impl
    return []

if __name__ == '__main__':
    print(txt2txt_search_service("拉肚子怎么办？"))
    #print(txt2img_search_service("猫"))
    #print(txt2img_search_service("大象"))
