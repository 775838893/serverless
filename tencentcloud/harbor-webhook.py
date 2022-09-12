# -*- coding:utf-8 -*-

import os
import time
import json
import base64
import requests

"""
harbor webhook，当harbor产生操作仓库的日志，触发api请求然后发送到graylog
"""


def format_time(timestamp):
    """
    时间戳转本地日期
    @param timestamp:
    @return: date
    """
    if not timestamp:
        return None
    time_stame_array = time.localtime(timestamp)
    local_time = time.strftime("%Y-%m-%d %H:%M:%S", time_stame_array)
    return local_time



def handler(event, context):
    graylog_url = os.environ.get("graylog_url")
    body = json.loads(json.dumps(event)).get("body")
    body_decode = base64.b64decode(body).decode("utf-8")  # 对body解码
    response = json.loads(body_decode)
    types = response.get("type").split("_")
    action = types[0]
    resource_type = types[1]
    occur_at = format_time(response.get("occur_at"))
    operator = response.get("operator") if response.get("operator") else "匿名"
    event_data = response.get("event_data")
    resource_url = event_data.get("resources")[0].get("resource_url")
    repo_type = event_data.get("repository").get("repo_type")

    short_message = {"username": operator, "occur_at": occur_at, "action": action, "resource_type": resource_type,
                     "resource_url": resource_url, "repo_type": repo_type}

    msg = {"version": "1.1", "host": "harbor-webhook",
           "short_message": json.dumps(short_message, ensure_ascii=False)}
    try:
        headers = {'Content-Type': 'application/json;charset=utf-8'}
        data_dump = json.dumps(msg, ensure_ascii=False).encode("utf-8")
        x = requests.post(url=graylog_url, data=data_dump, headers=headers)
    except Exception as e:
        print(e)
