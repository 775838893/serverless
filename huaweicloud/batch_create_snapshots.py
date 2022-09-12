# -*- coding:utf-8 -*-

from huaweicloudsdkcore.auth.credentials import GlobalCredentials, BasicCredentials
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkrms.v1.region.rms_region import RmsRegion
from huaweicloudsdkevs.v2.region.evs_region import EvsRegion
from huaweicloudsdkrms.v1 import *
from huaweicloudsdkevs.v2 import *

import hmac
import hashlib
import base64
import urllib.parse
import json
import time
import requests
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed


class HuaweiCloud:

    def __init__(self, ak, sk, max_workers):
        self.ak = ak
        self.sk = sk
        self.global_credentials = GlobalCredentials(ak, sk)
        self.basic_credentials = BasicCredentials(ak, sk)
        self.errLists = []
        self.max_workers = max_workers

        # 此处目前只能填cn-north-4
        self.rms_client = RmsClient.new_builder() \
            .with_credentials(self.global_credentials) \
            .with_region(RmsRegion.value_of("cn-north-4")) \
            .build()

    def create_snapshot(self, client, volume_id, name):
        """
        创建快照
        @param client: evs client
        @param volume_id: 磁盘id
        @param name: 快照名
        @return:
        """
        try:
            request = CreateSnapshotRequest()
            snapshotCreateSnapshotOption = CreateSnapshotOption(
                volume_id=volume_id,
                force=True,
                name=name
            )
            request.body = CreateSnapshotRequestBody(
                snapshot=snapshotCreateSnapshotOption
            )
            response = client.create_snapshot(request)
            return response
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)
            self.errLists.append({"createErr": e.error_msg + " volume_info name is " + name})

    def delete_snapshot(self, client, snapshot_id):
        """
        删除快照
        @param client: evs client
        @param snapshot_id: 快照id
        @return:
        """
        try:
            request = DeleteSnapshotRequest()
            request.snapshot_id = snapshot_id
            response = client.delete_snapshot(request)
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)
            self.errLists.append({"deleteErr": e.error_msg})

    def get_all_snapshots(self, client):
        """
        获取所有区域的快照信息。
        @param client: evs client
        @return: {volume_id: [snapshot_id]}
        """
        try:
            offset = 0
            snapshots = {}
            request = ListSnapshotsRequest()
            request.limit = 1000
            while True:
                request.offset = offset
                response = client.list_snapshots(request).to_dict().get('snapshots')
                if response:
                    for info in response:
                        volume_id = info.get("volume_id")
                        snapshot_id = info.get("id")
                        if volume_id in snapshots:
                            snapshots.get(volume_id).append(snapshot_id)
                        else:
                            snapshots.update({volume_id: [snapshot_id]})
                    offset += 1000
                else:
                    return snapshots

        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)

    def get_all_volumes(self):
        """
        获取所有的磁盘信息
        @return: {region_id: [{volume_id: volume_name},{volume_id: volume_name}...],}
        """
        data = {}
        marker = None
        request = ListAllResourcesRequest()
        request.limit = 200  # 目前最大200
        request.type = "evs.volumes"
        while True:
            try:
                request.marker = marker
                response = self.rms_client.list_all_resources(request).to_dict()
                if response:
                    page_info = response.get("page_info")
                    next_marker = page_info.get("next_marker")
                    resources_list = response.get("resources")
                    for info in resources_list:
                        properties = info.get("properties")
                        if properties.get("status") == "in-use":  # 只查使用中的
                            id = info.get("id")
                            region_id = info.get("region_id")
                            name = info.get("name")
                            if region_id in data:
                                data[region_id].append({id: name})
                            else:
                                data[region_id] = [{id: name}]
                    if next_marker:
                        marker = next_marker
                    else:
                        return data
                else:
                    return data
            except exceptions.ClientRequestException as e:
                print(e.status_code)
                print(e.request_id)
                print(e.error_code)
                print(e.error_msg)

    def dojob(self, volumes, max_savetime):
        """
        批量创建快照任务
        @param volumes: 总磁盘数dict
        @param max_savetime: 最大保留日期
        @return: 执行结果
        """
        res = []
        times = time.strftime("%Y-%m-%d", time.localtime())

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for region, value in volumes.items():
                evs_client = EvsClient.new_builder() \
                    .with_credentials(BasicCredentials(self.ak, self.sk)) \
                    .with_region(EvsRegion.value_of(region)).build()

                snapshots = self.get_all_snapshots(evs_client)
                for i in value:
                    volume_id = list(i.keys())[0]
                    volume_name = list(i.values())[0]
                    snapshot_lists = snapshots.get(volume_id)
                    if snapshot_lists and len(snapshot_lists) >= int(max_savetime):
                        executor.submit(self.delete_snapshot, client=evs_client, snapshot_id=snapshot_lists[-1])
                    future = executor.submit(self.create_snapshot, client=evs_client, volume_id=volume_id,
                                             name=volume_name + '-' + times)
                    res.append(future)
        return res

    @staticmethod
    def send_dingding(dd_secret, dd_token, text):
        timestamp = str(round(time.time() * 1000))
        secret = dd_secret  # 钉钉secret

        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        # print(timestamp)
        # print(sign)

        url = 'https://oapi.dingtalk.com/robot/send?access_token=' + dd_token + '&timestamp=' + timestamp + '&sign=' + sign
        data = {
            "msgtype": "text",
            "text": {
                "content": "华为云-快照创建信息" + '\n' + '\n' + text
            },
            "at": {
                "atMobiles": [
                ],
                "isAtAll": False
            }
        }
        headers = {'Content-Type': 'application/json'}
        try:
            data_dump = json.dumps(data, ensure_ascii=False).encode("utf-8")
            res = requests.post(url=url, data=data_dump, headers=headers)
        except Exception as e:
            print(e)


def handler(event, context):
    ak = context.getAccessKey()
    sk = context.getSecretKey()
    dd_token = context.getUserData("dd_token")
    dd_secret = context.getUserData("dd_secret")
    max_savetime = context.getUserData("max_savetime")

    max_workers = multiprocessing.cpu_count() * 6  # 线程池任务最大数量
    huaweicloud = HuaweiCloud(ak, sk, max_workers)
    volumes = huaweicloud.get_all_volumes()
    response = huaweicloud.dojob(volumes, max_savetime)
    result = [i.result() for i in response if i.result() is not None]
    total_volumes = [i for lst in volumes.values() for i in lst]
    dingding_msg = "硬盘总数共{}个，成功创建{}个快照！".format(len(total_volumes), len(result))
    print(dingding_msg)
    huaweicloud.send_dingding(dd_secret, dd_token, dingding_msg)
    if huaweicloud.errLists:
        huaweicloud.send_dingding(str(huaweicloud.errLists))

    return {
        "statusCode": 200,
        "isBase64Encoded": False,
        "body": json.dumps(event),
        "headers": {
            "Content-Type": "application/json"
        }
    }
