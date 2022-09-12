# coding: utf-8

from huaweicloudsdkcore.auth.credentials import BasicCredentials, GlobalCredentials
from huaweicloudsdkrms.v1.region.rms_region import RmsRegion

from huaweicloudsdkces.v1.region.ces_region import CesRegion
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkces.v1 import *
from huaweicloudsdkrms.v1 import *

import requests
import json


class HuaweiCloud:
    def __init__(self, ak, sk, region):
        self.ak = ak
        self.sk = sk
        self.region = region
        self.basic_credentials = BasicCredentials(ak, sk)
        self.global_credentials = GlobalCredentials(ak, sk)

        self.ces_client = CesClient.new_builder() \
            .with_credentials(self.basic_credentials) \
            .with_region(CesRegion.value_of(self.region)) \
            .build()

        # 此处目前只能填cn-north-4
        self.rms_client = RmsClient.new_builder() \
            .with_credentials(self.global_credentials) \
            .with_region(RmsRegion.value_of("cn-north-4")) \
            .build()

		 # 需提前在CES控制台创建示例的组如all_ecs
        self.resource_type = {
            "all_ecs": {"type": "ecs.cloudservers", "namespace": "SYS.ECS", "dimension_name": "instance_id"},
            # "all_evs": {"type": "evs.volumes", "namespace": "SYS.EVS", "dimension_name": "disk_name"},
            "all_eip": {"type": "vpc.bandwidths", "namespace": "SYS.VPC", "dimension_name": "bandwidth_id"},
            "all_rds": {"type": "rds.instances", "namespace": "SYS.RDS", "dimension_name": "rds_cluster_id"}
        }

    def list_resource_group(self) -> dict:
        """
        列出所有的监控组
        :return: {'all_ecs': 'rg16463628544956zYVmz3G1', 'all_eip': 'rs16498178017805y4J5BNn8'...}
        """
        data = {}
        try:
            request = ListResourceGroupRequest()
            response = self.ces_client.list_resource_group(request).to_dict()
            if response:
                for d in response.get("resource_groups"):
                    group_name = d.get("group_name")
                    group_id = d.get("group_id")
                    data[group_name] = group_id
            return data
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.error_code)
            print(e.error_msg)

    def show_resource_group(self, group_id) -> list:
        """
        查询具体的组的资源
        :param group_id: 组id
        :return: 资源id列表
        """
        data = []
        try:
            request = ShowResourceGroupRequest()
            request.group_id = group_id
            response = self.ces_client.show_resource_group(request).to_dict()
            if response:
                for d in response.get("resources"):
                    resource_id = d.get("dimensions")[0].get("value")
                    data.append(resource_id)
            return data
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.error_code)
            print(e.error_msg)

    def get_all_resources(self, resource_type) -> list:
        """
        根据type获取所有的资源id
        :param resource_type: 资源类型，如ecs.cloudservers evs.volumes
        :return: 资源id列表
        """
        marker = None
        data = []
        request = ListAllResourcesRequest()
        request.limit = 200  # 目前最大200
        request.type = resource_type
        while True:
            try:
                request.marker = marker
                request.region_id = self.region  # ces有region区分。
                response = self.rms_client.list_all_resources(request).to_dict()
                if response:
                    page_info = response.get("page_info")
                    next_marker = page_info.get("next_marker")
                    # [5f26b1232-6589-4a7b-83f9-1848c547d585-vdb, 2222222-6589-4a7b-83f9-1848c547d585-vda]
                    # 硬盘监控组比较特殊，id为ecs id + 挂载点
                    resource_id = [i.get("properties").get("attachments")[0].get("serverId") + "-" +
                                   i.get("properties").get("attachments")[0].get("device")[-3:]
                                   if resource_type == "evs.volumes" else i.get("id")
                                   for i in response.get("resources")]
                    data.extend(resource_id)
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

    def update_resource_groups(self, group_name, group_id, namespace, dimension_name, resources_id: list):
        """
        更新监控组的资源
        :param group_name:  监控组名称
        :param group_id:  监控组id
        :param namespace:  namespace 如instance_id
        :param dimension_name: 如SYS.ECS
        :param resources_id:  资源id
        :return:
        """
        try:
            request = UpdateResourceGroupRequest()
            request.group_id = group_id
            listCreateResourceGroupResourcesbody = []

            for value in resources_id:
                dimensions = [
                    MetricsDimension(
                        name=dimension_name,
                        value=value
                    )] if dimension_name else [
                    MetricsDimension(
                        name="instance_id",
                        value=value.get("instance_id")
                    ),
                    MetricsDimension(
                        name="mount_point",
                        value=value.get("mount_point")
                    )
                ]

                listCreateResourceGroupResourcesbody.extend([
                    CreateResourceGroup(
                        namespace=namespace,
                        dimensions=dimensions
                    )
                ])

            request.body = UpdateResourceGroupRequestBody(
                resources=listCreateResourceGroupResourcesbody,
                group_name=group_name
            )
            response = self.ces_client.update_resource_group(request)
            print(response)

        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.error_code)
            print(e.error_msg)


def get_token(region, domain_name, username, password) -> str:
    """
    获取华为云token，需提前创建iam用户并授权CES权限
    :param region: region
    :param domain_name: iam账户名
    :param username: 用户名
    :param password:密码
    :return:
    """
    region = region
    iam_url = "https://iam.cn-southwest-2.myhuaweicloud.com/v3/auth/tokens"
    headers = {'Content-Type': 'application/json'}
    data = {
        "auth": {
            "identity": {
                "methods": [
                    "password"
                ],
                "password": {
                    "user": {
                        "domain": {
                            "name": domain_name  # IAM用户所属帐号名
                        },
                        "name": username,  # IAM用户名
                        "password": password  # IAM用户密码
                    }
                }
            },
            "scope": {
                "project": {
                    "name": region  # 项目名称
                }
            }
        }
    }
    try:
        response = requests.post(url=iam_url, data=json.dumps(data), headers=headers)
        token = response.headers.get("X-Subject-Token")
        return token
    except Exception as e:
        print(e)


def get_mount_point(token) -> list:
    """
    获取已筛选的挂载点，返回list类型的id
    :param token: 华为云token
    :return: mountpoint list
    """
    data = []
    region_id = "09d72227ab8025212ffcc0080c1cc471"  # 华为云企业项目id，自行查询对应的区域的id
    region = "cn-southwest-2"  # 企业项目
    url = f"https://console.huaweicloud.com/ces/rest/V1.0/ecs/{region_id}/instances"
    namespace = "AGT.ECS" # 使用agent方式的namespace会更准确。需给所有ECS安装上监控agent
    start = 0
    limit = 1000
    dim_name = "mount_point"
    headers = {
        "x-auth-token": token,
        "region": region,
        "projectname": region
    }

    mount_point_filter = ["var", "run", "iso", "pods", "docker","cd1","dm"]  # 过滤这些挂载点，可自定义。

    while True:
        params = {
            "namespace": namespace,
            "start": start,
            "limit": limit,
            "dim.0.name": dim_name
        }
        try:
            # https://console.huaweicloud.com/ces/rest/V1.0/ecs/09d72227ab8025212ffcc0080c1cc471/instances?namespace=AGT.ECS&start=0&limit=1000&dim.0.name=mount_point
            res = requests.get(url=url, params=params, headers=headers)
            res_json = json.loads(res.content.decode("utf-8"))
            total = res_json.get("total")
            instances = res_json.get("instances")
            for i in instances:
                name = i.get("name")
                is_filter = [False if i in name else True for i in mount_point_filter]
                if all(is_filter):
                    tmp = {
                        "instance_id": i.get("instance_id"),
                        "mount_point": i.get("mount_point"),
                        "name": name
                    }
                    data.append(tmp)

            start += 1000
            if start > total:
                break

        except Exception as e:
            print(e)
    return data


def handler(event, context):
    ak = context.getAccessKey()
    sk = context.getSecretKey()
    region = "cn-southwest-2"  # 自己的区域
    iam_name = context.getUserData("iam_name")
    username = context.getUserData("hw_iam_username")
    password = context.getUserData("hw_iam_password")
    huaweicloud = HuaweiCloud(ak, sk, region)
    groups = huaweicloud.list_resource_group()

    for group_name, group_id in groups.items():
        # 过滤default组
        if group_name != "default":

            # 挂载点
            if group_name == "all_mountpoint":
                token = get_token(region, iam_name, username, password)
                mount_point_data = get_mount_point(token)
                huaweicloud.update_resource_groups(group_name, group_id, "SYS.ECS", None, mount_point_data)
            else:
                # group_resources = huaweicloud.show_resource_group(group_id)
                r_type = huaweicloud.resource_type.get(group_name)
                if r_type:
                    resource_type = r_type.get("type")
                    namespace = huaweicloud.resource_type.get(group_name).get("namespace")
                    dimension_name = huaweicloud.resource_type.get(group_name).get("dimension_name")
                    resources_id = huaweicloud.get_all_resources(resource_type)
                    # print(namespace, dimension_name, group_id, group_name)
                    huaweicloud.update_resource_groups(group_name, group_id, namespace, dimension_name, resources_id)
