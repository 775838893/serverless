# coding: utf-8

from huaweicloudsdkcore.auth.credentials import GlobalCredentials
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkrms.v1.region.rms_region import RmsRegion
from huaweicloudsdkrms.v1 import *

from utils import *
import yagmail
import jinja2
import json
import os

_cur_path = os.path.dirname(os.path.realpath(__file__))


class HuaWeiCloud:
    def __init__(self, ak, sk):
        self.ak = ak
        self.sk = sk
        self.global_credentials = GlobalCredentials(ak, sk)

        self.services_type = {
            "AGT.ECS": {"provider": "ecs", "resource_type": "cloudservers"},
            "SYS.ECS": {"provider": "ecs", "resource_type": "cloudservers"},
            "AGT.EVS": {"provider": "evs", "resource_type": "volumes"},
            "SYS.EVS": {"provider": "evs", "resource_type": "volumes"},
            "SYS.RDS": {"provider": "rds", "resource_type": "instances"},
            "SYS.VPC": {"provider": "vpc", "resource_type": "bandwidth_id"},
        }

        # 此处目前只能填cn-north-4
        self.rms_client = RmsClient.new_builder() \
            .with_credentials(self.global_credentials) \
            .with_region(RmsRegion.value_of("cn-north-4")) \
            .build()

    def get_manager(self, resource_provider, resource_type, resource_id) -> str:
        """
        获取指定资源的标签中的负责人信息
        :param resource_provider: 如ecs evs rds等
        :param resource_type: 资源类型 ecs的是cloudservers等
        :param resource_id: 资源id
        :return: 负责人
        """
        try:
            request = ShowResourceByIdRequest()
            request.provider = resource_provider
            request.type = resource_type
            request.resource_id = resource_id
            response = self.rms_client.show_resource_by_id(request).to_dict()
            manager = response.get("tags").get("负责人") if response else None 
            return manager

        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.error_code)
            print(e.error_msg)


class MyTemplateMail:
    def __init__(self, user, password, host, subject, template_name):
        """
        @param user: 发送者邮件
        @param password: 密码
        @param host: smtp地址
        @param subject: 邮件标题
        @param template: 模板文件名
        """
        self.user = user
        self.password = password
        self.host = host
        self.subject = subject
        self.template_name = template_name

        self.template_env = jinja2.Environment(
            undefined=jinja2.StrictUndefined,
            loader=jinja2.FileSystemLoader([_cur_path])
        )

    def send_email(self, value, to, cc=None):
        """
        发送邮件
        @param value: 数据
        @param to: 邮件接收者 list
        @param cc: 抄送 list
        @return:
        """
        try:
            template = self.template_env.get_template(self.template_name)
            contents = template.render(**value).replace("\n", "")
            with yagmail.SMTP(user=self.user, password=self.password, host=self.host) as yag:
                yag.send(to=to, cc=cc, subject=self.subject, contents=contents)
                print("发送邮件成功。")
        except Exception as e:
            print(e)


def handler(event, context):
    ak = context.getAccessKey()
    sk = context.getSecretKey()
    message = event['record'][0]['smn']['message']  # 返回的是字符串，并不是dict
    subject = event['record'][0]['smn']['subject']  # 返回的是字符串，并不是dict

    username = context.getUserData("username")
    password = context.getUserData("password")
    cc = context.getUserData("cc") # 抄送
    host = context.getUserData("host")

    huaweicloud = HuaWeiCloud(ak, sk)

    message = json.loads(message)
    namespace = message.get("namespace")
    template_variable = message.get("template_variable")

    resource_provider = huaweicloud.services_type[namespace]['provider']
    resource_type = huaweicloud.services_type[namespace]['resource_type']
    sms_content = message.get("sms_content")

    # 忽略vpn网关的带宽
    if namespace == "SYS.VPC" and "vpngw" in sms_content:
        print(f"过滤日志: {sms_content}")
        return

    id_str = template_variable.get("ResourceId")
    # 硬盘的id返回格式 '3349c437-e0b6-415a-b426-39d948db2eb1<br>挂载点：5ee61012dc310f6dcafdf54cd7c05fff'
    resource_id = id_str if "挂载点" not in id_str else id_str.split("<br>")[0]
    manager = huaweicloud.get_manager(resource_provider, resource_type, resource_id)
    if manager:
        value = {
            "manager": manager,
            "regulation": message.get("alarm_name"),
            "sms_content": sms_content,
            "region_id": template_variable.get("Region"),
            "dimension_name": template_variable.get("DimensionName"),
            "resource_name": template_variable.get("ResourceName"),
            "metric_name": template_variable.get("MetricName"),
            "inner_ip": template_variable.get("PrivateIp"),
            "public_ip": template_variable.get("PublicIp"),
            "ep_name": template_variable.get("EpName"),
            "current_data": template_variable.get("CurrentData"),
            "occour_time": template_variable.get("AlarmTime"),
            "alarm_level": template_variable.get("AlarmLevel"),
            "occour_data": template_variable.get("DataPoint"),
        }

        mail = MyTemplateMail(username, password, host, subject, "template.html")
        send_to = hanzi2pinyin(manager) + "@your_domain.com"
        cc_to = [cc]
        mail.send_email(value, to=send_to, cc=cc_to)
    else:
        print("manager 为空。")
