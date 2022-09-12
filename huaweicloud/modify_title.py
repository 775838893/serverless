# coding: utf-8

from huaweicloudsdkcore.auth.credentials import BasicCredentials, GlobalCredentials
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkecs.v2.region.ecs_region import EcsRegion
from huaweicloudsdkevs.v2.region.evs_region import EvsRegion
from huaweicloudsdkrms.v1.region.rms_region import RmsRegion
from huaweicloudsdkeip.v2.region.eip_region import EipRegion

from huaweicloudsdkecs.v2 import *
from huaweicloudsdkevs.v2 import *
from huaweicloudsdkrms.v1 import *
from huaweicloudsdkeip.v2 import *

import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

"""
统一 ecs volume 带宽名称,如dev_4c16g_1.1.1.1_xxx项目
"""


class HuaWeiCloudTask:
    def __init__(self, ak, sk, max_workers):
        self.ak = ak
        self.sk = sk
        self.max_workers = max_workers
        self.basic_credentials = BasicCredentials(ak, sk)
        self.global_credentials = GlobalCredentials(ak, sk)

        self.rms_client = RmsClient.new_builder() \
            .with_credentials(self.global_credentials) \
            .with_region(RmsRegion.value_of("cn-north-4")) \
            .build()

    def get_all_servers(self):
        """
        获取所有的服务器
        @return: {region_id1: [{id: id, ip: ip, environment: environment, project: project, ecs_config: ecs_config}]}
        """
        marker = None
        data = {}
        request = ListAllResourcesRequest()
        request.limit = 200  # 目前最大200
        request.type = "ecs.cloudservers"
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
                        if properties.get("status") == "ACTIVE":
                            region_id = info.get("region_id")
                            ecs_id = info.get("id")
                            ecs_name = info.get("name")
                            ip = properties.get("addresses")[0].get("addr")

                            cpu = properties.get("flavor").get("vcpus")
                            memory = int(properties.get("flavor").get("ram")) // 1024
                            environment = "dev" if info.get("tags").get("环境") == "非生产" else "pro"
                            project = info.get("tags").get("项目编号").split("-", 1)[-1]
                            mark = "_" + info.get("tags").get("备注") if info.get("tags").get("备注") else ""
                            tmp = {
                                "id": ecs_id,
                                "ecs_name": ecs_name,
                                "ip": ip,
                                "environment": environment,
                                "project": project,
                                "ecs_config": cpu + "c" + str(memory) + "g",
                                "mark": mark
                            }

                            if region_id in data:
                                data[region_id].append(tmp)
                            else:
                                data[region_id] = [tmp]
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

    def get_all_volumes(self):
        """
        获取所有的磁盘
        @return: {"region_id":[{"id":id,"name":name,"size":size,"volume_type":volume_type,"device":device,"server_id":server_id}]}
        """
        marker = None
        data = {}
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
                        if properties.get("status") == "in-use":
                            region_id = info.get("region_id")
                            evs_id = info.get("id")
                            name = info.get("name")
                            size = properties.get("size")
                            volume_type = properties.get("volumeType")
                            device = properties.get("attachments")[0].get("device")
                            server_id = properties.get("attachments")[0].get("serverId")
                            tmp = {
                                "id": evs_id,
                                "name": name,
                                "size": size,
                                "volume_type": volume_type,
                                "device": device,
                                "server_id": server_id
                            }
                            if region_id in data:
                                data[region_id].append(tmp)
                            else:
                                data[region_id] = [tmp]
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

    def get_all_eips(self) -> dict:
        """
        获取所有的公网ip
        @return: {'cn-southwest-2': [{'public_ip': '139.9.242.222', 'instance_type': 'PORT', 'size': '10M', 'inner_ip':
         '1.1.1.1', 'charge_mode': 'traffic', 'bandwidth_id': 'dc46d1fa-a6fe-4085-b2fb-c47b4363a9eb'}]}
        """
        marker = None
        data = {}
        request = ListAllResourcesRequest()
        request.limit = 200  # 目前最大200
        request.type = "vpc.publicips"
        while True:
            try:
                request.marker = marker
                response = self.rms_client.list_all_resources(request).to_dict()
                if response:
                    page_info = response.get("page_info")
                    next_marker = page_info.get("next_marker")
                    resources_list = response.get("resources")
                    for info in resources_list:
                        region_id = info.get("region_id")
                        properties = info.get("properties")
                        status = properties.get("status")
                        if status == "DOWN":  # 跳过未绑定的EIP（绑定的状态ACTIVE）
                            continue
                        public_ip = properties.get("publicIpAddress")
                        inner_ip = properties.get("vnic").get("privateIpAddress")
                        instance_type = properties.get("associateInstanceType")
                        bandwidth = properties.get("bandwidth")
                        size = str(bandwidth.get("size")) + "M"
                        charge_mode = bandwidth.get("chargeMode")  # traffic=按流量，bandwidth=包年包月
                        bandwidth_id = bandwidth.get("id")
                        bandwidth_name = bandwidth.get("name")
                        tmp = {
                            "public_ip": public_ip,
                            "instance_type": instance_type,
                            "size": size,
                            "inner_ip": inner_ip,
                            "charge_mode": charge_mode,
                            "bandwidth_id": bandwidth_id,
                            "bandwidth_name": bandwidth_name
                        }

                        if region_id in data:
                            data[region_id].append(tmp)
                        else:
                            data[region_id] = [tmp]
                    if next_marker:
                        marker = next_marker
                    else:
                        return data
                else:
                    return data

            except exceptions.ClientRequestException as e:
                print(e.status_code)
                print(e.error_code)
                print(e.error_msg)

    def update_ecs_title(self, client: EcsClient, server_id: str, name: str):
        try:
            request = UpdateServerRequest()
            request.server_id = server_id
            serverUpdateServerOption = UpdateServerOption(
                name=name
            )
            request.body = UpdateServerRequestBody(
                server=serverUpdateServerOption
            )
            response = client.update_server(request)
            print(response)
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.error_code)
            print(e.error_msg)

    def update_volume_title(self, client: EvsClient, volume_id: str, description: str, name: str):
        try:
            request = UpdateVolumeRequest()
            request.volume_id = volume_id
            volumeUpdateVolumeOption = UpdateVolumeOption(
                description=description,
                name=name
            )
            request.body = UpdateVolumeRequestBody(
                volume=volumeUpdateVolumeOption
            )
            response = client.update_volume(request)
            print(response)
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)

    def update_bandwidth_title(self, client: EipClient, bandwidth_id: str, name: str):
        try:
            request = UpdateBandwidthRequest()
            request.bandwidth_id = bandwidth_id
            bandwidthUpdateBandwidthOption = UpdateBandwidthOption(
                name=name
            )
            request.body = UpdateBandwidthRequestBody(
                bandwidth=bandwidthUpdateBandwidthOption
            )
            response = client.update_bandwidth(request)
            print(response)
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.error_code)
            print(e.error_msg)

    def do_job(self, servers, volumes, eips):
        res = []

        # update ecs
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for region, value in servers.items():
                ecs_client = EcsClient.new_builder() \
                    .with_credentials(BasicCredentials(self.ak, self.sk)) \
                    .with_region(EcsRegion.value_of(region)).build()
                for ecs in value:
                    ecs_id = ecs.get("id")
                    ecs_name = ecs.get("ecs_name")
                    ip = ecs.get("ip")
                    environment = ecs.get("environment")
                    project = ecs.get("project")
                    ecs_config = ecs.get("ecs_config")
                    mark = ecs.get("mark")

                    if ip in ecs_name and ecs_config in ecs_name and environment in ecs_name \
                            and project in ecs_name and mark in ecs_name:
                        continue
                    else:
                        new_ecs_name = f"{environment}_{ecs_config}_{ip}_{mark}_{project}"  # pro_2c16g_1.1.1.1_k8s-master_xxx项目
                        f = executor.submit(self.update_ecs_title, ecs_client, ecs_id, new_ecs_name)
                        res.append(f)

        # update volume
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for region, value in volumes.items():
                evs_client = EvsClient.new_builder() \
                    .with_credentials(BasicCredentials(self.ak, self.sk)) \
                    .with_region(EvsRegion.value_of(region)).build()
                for volume in value:
                    volume_id = volume.get("id")
                    volume_origin_name = volume.get("name")
                    volume_size = volume.get("size")
                    volume_type = volume.get("volume_type").lower()
                    volume_device = volume.get("device")[5:]  # /dev/vda => vda
                    server_id = volume.get("server_id")
                    ip = "".join([i.get("ip") for i in servers.get(region) if server_id in i.get("id")])
                    if ip in volume_origin_name and str(volume_size) in volume_origin_name:
                        continue
                    else:
                        volume_name = f"volume_{volume_type}_{volume_size}G_{volume_device}_{ip}"  # 名称 volume_sata_100G_vda_1.1.1.1
                        description = ip
                        f = executor.submit(self.update_volume_title, evs_client, volume_id, description, volume_name)
                        res.append(f)

        # update bandwidth
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for region, value in eips.items():
                eip_client = EipClient.new_builder() \
                    .with_credentials(BasicCredentials(self.ak, self.sk)) \
                    .with_region(EipRegion.value_of(region)).build()
                for eip in value:
                    bandwidth_id = eip.get("bandwidth_id")
                    bandwidth_name = eip.get("bandwidth_name")
                    public_ip = eip.get("public_ip")
                    instance_type = eip.get("instance_type")
                    size = eip.get("size")
                    inner_ip = eip.get("inner_ip")
                    charge_mode = eip.get("charge_mode")
                    if size in bandwidth_name and inner_ip in bandwidth_name and charge_mode in bandwidth_name:
                        continue
                    else:
                        # bandwidth_PORT_1.1.1.1_1.1.1.2_10M_traffic
                        new_bandwidth_name = f"bandwidth_{instance_type}_{public_ip}_{inner_ip}_{size}_{charge_mode}"
                        f = executor.submit(self.update_bandwidth_title, eip_client, bandwidth_id, new_bandwidth_name)
                        res.append(f)

        return res


def handler(event, context):
    ak = context.getAccessKey()
    sk = context.getSecretKey()
    max_workers = multiprocessing.cpu_count() * 4  # 线程池任务最大数量
    task = HuaWeiCloudTask(ak, sk, max_workers)
    servers = task.get_all_servers()
    volumes = task.get_all_volumes()
    eips = task.get_all_eips()
    job = task.do_job(servers, volumes, eips)
    m_result = [i.result() for i in as_completed(job) if i.result() is None]
    print(f"修改名称{len(m_result)}个")
