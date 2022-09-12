# -*- coding:utf-8 -*-

from huaweicloudsdkcore.auth.credentials import GlobalCredentials, BasicCredentials
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkeps.v1.region.eps_region import EpsRegion
from huaweicloudsdkevs.v2.region.evs_region import EvsRegion
from huaweicloudsdkeip.v2.region.eip_region import EipRegion
from huaweicloudsdkrms.v1.region.rms_region import RmsRegion
from huaweicloudsdkiam.v3.region.iam_region import IamRegion

from huaweicloudsdkeps.v1 import *
from huaweicloudsdkevs.v2 import *
from huaweicloudsdkeip.v2 import *
from huaweicloudsdkrms.v1 import *
from huaweicloudsdkiam.v3 import *

import multiprocessing
import json
from concurrent.futures import ThreadPoolExecutor, as_completed


class HuaWeiCloudTask:
    def __init__(self, ak: str, sk: str, max_workers: int):
        self.ak = ak
        self.sk = sk
        self.max_workers = max_workers
        self.basic_credentials = BasicCredentials(ak, sk)
        self.global_credentials = GlobalCredentials(ak, sk)

        # 此处目前只能填cn-north-4
        self.eps_client = EpsClient.new_builder() \
            .with_credentials(self.global_credentials) \
            .with_region(EpsRegion.value_of("cn-north-4")) \
            .build()

        # cn-southwest-2 随意填写，全局的
        self.iam_client = IamClient.new_builder() \
            .with_credentials(self.global_credentials) \
            .with_region(IamRegion.value_of("cn-southwest-2")) \
            .build()

        # 此处目前只能填cn-north-4
        self.rms_client = RmsClient.new_builder() \
            .with_credentials(self.global_credentials) \
            .with_region(RmsRegion.value_of("cn-north-4")) \
            .build()

    def get_iam_region(self):
        """
        查询IAM用户可以访问的项目列表
        @return: {region_name: region_id}
        """
        try:
            data = {}
            request = KeystoneListAuthProjectsRequest()
            response = self.iam_client.keystone_list_auth_projects(request).to_dict().get("projects")
            for info in response:
                if info.get("enabled") == True:
                    iam_id = info.get("id")
                    iam_name = info.get("name")
                    data[iam_name] = iam_id
            return data

        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.error_code)
            print(e.error_msg)

    def migrate_project(self, enterprise_project_id, resource_id, project_id):
        """
        迁移企业项目
        @param enterprise_project_id: 企业项目id
        @param resource_id: 资源id
        @param project_id: 区域的id 如贵阳的id
        @return:
        """
        try:
            request = MigrateResourceRequest()
            request.enterprise_project_id = enterprise_project_id
            request.body = MigrateResource(
                associated=True,  # 关联硬盘及ip
                resource_type="ecs",
                resource_id=resource_id,
                project_id=project_id
            )
            response = self.eps_client.migrate_resource(request)

        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.error_code)
            print(e.error_msg)

    def get_all_enterprise_projects(self):
        """
        获取所有的企业项目
        @return: {ep_id: ep_name}
        """
        try:
            enterprise_projects = {}
            request = ListEnterpriseProjectRequest()
            # request.status = 1  # 1启用 2停用
            response = self.eps_client.list_enterprise_project(request).to_dict().get("enterprise_projects")
            for info in response:
                enterprise_project_id = info.get("id")
                name = info.get("name")
                enterprise_projects[enterprise_project_id] = name
            return enterprise_projects

        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.error_code)
            print(e.error_msg)

    def get_all_servers(self):
        """
        获取所有的ecs
        @return: {"ecs_id": {"region_id": region_id, "public_ip":public_ip, "tags": tags, "ep_id": ep_id}}
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
                            tags = info.get("tags")
                            ep_id = info.get("ep_id")
                            public_ip = properties.get("addresses")[1].get("addr") if len(
                                properties.get("addresses")) > 1 else None
                            tmp = {
                                "region_id": region_id,
                                "public_ip": public_ip,
                                "tags": tags,
                                "ep_id": ep_id
                            }
                            if ecs_id in data:
                                data[ecs_id].update(tmp)
                            else:
                                data[ecs_id] = tmp
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

    def get_all_volumes(self):
        """
        获取所有的磁盘
        @return: {"evs_id": {"ecs_id": ecs_id, "tags": tags, "region_id": region_id}}
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
                            tags = info.get("tags")
                            ecs_id = properties.get("attachments")[0].get("serverId")
                            tmp = {
                                "ecs_id": ecs_id,
                                "tags": tags,
                                "region_id": region_id
                            }
                            if evs_id in data:
                                data[evs_id].update(tmp)
                            else:
                                data[evs_id] = tmp
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

    def get_all_eips(self):
        """
        获取所有的公网ip
        @return: {"public_ip": tags}
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
                        properties = info.get("properties")
                        # # 只获取使用中和绑定服务器的
                        # if properties.get("associateInstanceType") == "PORT" and properties.get("status") == "ACTIVE":
                        eip_id = info.get("id")
                        tags = dict(info.get("tags"))
                        public_ip = properties.get("publicIpAddress")
                        tmp = {"eip_id": eip_id}
                        tags.update(tmp)
                        tmp = {public_ip: tags}
                        data.update(tmp)
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

    def update_ip_tag(self, client: EipClient, eip_id, tags):
        """
        修改公网ip标签
        @param client: eip client
        @param eip_id: eip id
        @param tags: eip 标签
        @return:
        """
        try:
            request = BatchCreatePublicipTagsRequest()
            request.publicip_id = eip_id
            listResourceTagOptionTagsbody = [ResourceTagOption(key=k, value=v) for k, v in tags.items()]

            request.body = BatchCreatePublicipTagsRequestBody(
                action="create",
                tags=listResourceTagOptionTagsbody
            )
            response = client.batch_create_publicip_tags(request)
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.error_code)
            print(e.error_msg)

    def update_volume_tag(self, client: EvsClient, volume_id, tags):
        """
        修改volume标签
        @param client: evs client
        @param volume_id: evs id
        @param tags: evs tag
        @return:
        """
        try:
            request = BatchCreateVolumeTagsRequest()
            request.volume_id = volume_id
            listTagTagsbody = [Tag(key=k, value=v) for k, v in tags.items()]

            request.body = BatchCreateVolumeTagsRequestBody(
                tags=listTagTagsbody,
                action="create"
            )
            response = client.batch_create_volume_tags(request)
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.error_code)
            print(e.error_msg)

    def migrate_project_job(self, all_servers):
        """
        迁移企业项目任务
        @param all_servers: ecs字典
        @return:
        """
        all_regions = self.get_iam_region()
        enterprise_projects = self.get_all_enterprise_projects()  
        res = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for ecs_id, info in all_servers.items():
                ep_id = info.get("ep_id")
                tags = info.get("tags")
                region_name = info.get("region_id")
                ecs_prj_name = tags.get("projectname") if "projectname" in tags else None  #projectname为你自定义的标签key
                if ecs_prj_name and enterprise_projects[ep_id] != ecs_prj_name:  # 如企业项目对不上标签的体系
                    new_prj_id = list(filter(lambda x: enterprise_projects[x] == ecs_prj_name, enterprise_projects))[0]
                    region_id = all_regions.get(region_name)
                    future = executor.submit(self.migrate_project, enterprise_project_id=new_prj_id,
                                             resource_id=ecs_id, project_id=region_id)
                    res.append(future)
        return res

    def update_tag_job(self, all_servers, all_volumes, all_eips):
        """
        更新标签任务
        @param all_servers: ecs字典
        @param all_volumes: evs字典
        @param all_eips: eip字典
        @return:
        """
        res = []
        eip_region = evs_region = None
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 更新eip
            for info in all_servers.values():
                public_ip = info.get("public_ip")
                if public_ip:
                    ecs_tags = info.get("tags")
                    eip_tags = all_eips.get(public_ip)
                    eip_id = eip_tags.get("eip_id")
                    if ecs_tags:
                        eip_result = ecs_tags.items() - eip_tags.items()  # 对比公网IP是不是跟服务器的标签一样
                        if eip_result:
                            region_id = info.get("region_id")
                            if eip_region != region_id:
                                eip_region = region_id
                                eip_client = EipClient.new_builder() \
                                    .with_credentials(self.basic_credentials) \
                                    .with_region(EipRegion.value_of(eip_region)) \
                                    .build()
                            need_update_tag = dict(eip_result)
                            f1 = executor.submit(self.update_ip_tag, client=eip_client, eip_id=eip_id,
                                                 tags=need_update_tag)
                            res.append(f1)
                    else:
                        print(public_ip)

            # 更新磁盘
            for evs_id, info in all_volumes.items():
                ecs_id = info.get("ecs_id")
                evs_tags = info.get("tags")
                ecs_tags = all_servers.get(ecs_id).get("tags") if all_servers.get(ecs_id) else None  # rms接口获取不到硬盘冻结信息
                if ecs_tags:
                    evs_result = ecs_tags.items() - evs_tags.items()  # 对比硬盘是不是跟服务器的标签一样
                    if evs_result:
                        region_id = info.get("region_id")
                        if evs_region != region_id:
                            evs_region = region_id
                            evs_client = EvsClient.new_builder() \
                                .with_credentials(self.basic_credentials) \
                                .with_region(EvsRegion.value_of(evs_region)) \
                                .build()
                        need_update_tag = dict(evs_result)
                        f2 = executor.submit(self.update_volume_tag, client=evs_client, volume_id=evs_id,
                                             tags=need_update_tag)
                        res.append(f2)
        return res


def handler(event, context):
    ak = context.getAccessKey()
    sk = context.getSecretKey()
    max_workers = multiprocessing.cpu_count() * 4  # 线程池任务最大数量

    task = HuaWeiCloudTask(ak, sk, max_workers)
    all_servers = task.get_all_servers()
    all_volumes = task.get_all_volumes()
    all_eips = task.get_all_eips()
    t_jobs = task.update_tag_job(all_servers, all_volumes, all_eips)
    t_result = [i.result() for i in as_completed(t_jobs) if i.result() is None]
    print(f"修改标签{len(t_result)}个")

    m_jobs = task.migrate_project_job(all_servers)
    m_result = [i.result() for i in as_completed(m_jobs) if i.result() is None]
    print(f"迁移企业项目{len(m_result)}个")
    return {
        "statusCode": 200,
        "isBase64Encoded": False,
        "body": json.dumps(event),
        "headers": {
            "Content-Type": "application/json"
        }
    }
