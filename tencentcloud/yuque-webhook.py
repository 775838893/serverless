# -*- coding:utf-8 -*-

import os
import json
import base64
import requests

"""
语雀webhook，当语雀产生日志，触发api请求然后发送到graylog
"""


def handler(event, context):
    graylog_url = os.environ.get("GRAYLOG_ADDRESS")
    body = json.loads(json.dumps(event)).get("body")
    body_decode = base64.b64decode(body).decode("utf-8")  # 对body解码
    response = json.loads(body_decode)
    headers = {'Content-Type': 'application/json;charset=utf-8'}
    print(response)
    data = response.get("data")
    if data:
        username = data.get("actor").get("name") if data.get("actor") else None
        title = data.get("auditable").get("title") if data.get("auditable") else None
        file_type = data.get("auditable").get("type") if data.get("auditable") else None
        export_format = data.get("auditable").get("format") if data.get("auditable") else None
        description = data.get("group").get("description") if data.get("group") else None
        action = data.get("action")
        auditable_type = data.get("auditable_type")
        created_at = data.get("created_at")
        ip = data.get("ip")
        short_message = {"username": username, "action": action, "file_type": file_type, "title": title,
                         "export_format": export_format, "description": description, "auditable_type": auditable_type,
                         "created_at": created_at, "ip": ip}

        msg = {"version": "1.1", "host": "yuque-webhook",
               "short_message": json.dumps(short_message, ensure_ascii=False)}
    else:
        msg = {"version": "1.1", "host": "yuque-webhook", "short_message": json.dumps(response, ensure_ascii=False)}
    
    try:
        data_dump = json.dumps(msg, ensure_ascii=False).encode("utf-8")
        x = requests.post(url=graylog_url, data=data_dump, headers=headers)
    except Exception as e:
        print(e)

    # return {
    #     "statusCode": 200,
    #     "isBase64Encoded": True,
    #     "body": json.dumps(event),
    #     "headers": {
    #         "Content-Type": "application/json"
    #     }
    # }
