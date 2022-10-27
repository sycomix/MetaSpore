import os
import subprocess
import time
from string import Template

from metasporeflow.online.check_service import notifyRecommendService
from metasporeflow.online.cloud_consul import putServiceConfig, Consul, putConfigByKey
from metasporeflow.online.online_generator import OnlineGenerator


def k8s_template_by_file(filename, data):
    with open(filename, 'r') as template_file:
        template_content = template_file.read()
        tempTemplate = Template(template_content)
        print(tempTemplate.safe_substitute(data))


def k8s_template(template_content, data):
    tempTemplate = Template(template_content)
    return tempTemplate.safe_substitute(data)


class OnlineK8sExecutor(object):
    def __init__(self, resources):
        self._local_resource = resources.find_by_name("demo_metaspore_flow")
        self._online_resource = resources.find_by_name("online_local_flow")
        self._generator = OnlineGenerator(resource=self._online_resource, local_resource=self._local_resource)
        self._service_k8s_filename_template = "%s/k8s-%%s.yaml" % os.getcwd()

    def execute_up(self, **kwargs):
        consul_data, recommend_data, model_data = self._generator.gen_k8s_config()
        if consul_data is None or recommend_data is None or model_data is None:
            print("k8s online service config is empty")
            return
        print("**********************************")
        print(consul_data)
        print(recommend_data)
        print(model_data)
        print("*" * 80)
        self.k8s_consul(consul_data, "up")
        time.sleep(3)
        self.k8s_model(model_data, "up")
        time.sleep(3)
        self.k8s_recommend(recommend_data, "up")
        time.sleep(10)
        online_recommend_config = self._generator.gen_server_config()
        consul_client = Consul("%s.%s" % (consul_data.setdefault("name", "consul-k8s-service"),
                                          consul_data.setdefault("domain", "huawei.dmetasoul.com")), 80)
        putServiceConfig(consul_client, online_recommend_config)

    def execute_down(self, **kwargs):
        consul_data, recommend_data, model_data = self._generator.gen_k8s_config()
        if consul_data is None or recommend_data is None or model_data is None:
            print("k8s online service config is empty")
            return
        self.k8s_recommend(recommend_data, "down")
        self.k8s_model(model_data, "down")
        self.k8s_consul(consul_data, "down")

    def execute_status(self, **kwargs):
        pass

    def execute_reload(self, **kwargs):
        new_flow = kwargs.setdefault("resource", None)
        if not new_flow:
            print("config update to None")
            self.execute_down(**kwargs)
        else:
            self._resource = new_flow
            self._generator = OnlineGenerator(resource=self._resource)
            self.execute_up(**kwargs)
        print("online flow reload success!")

    def generate_k8s_file(self, service_name, k8s_content):
        service_k8s_filename = self._service_k8s_filename_template % (service_name)
        service_k8s_file = open(service_k8s_filename, "w")
        service_k8s_file.write(k8s_content)
        service_k8s_file.close()
        return service_k8s_filename

    def create_k8s_service(self, service_name, template_content, data):
        k8s_content = k8s_template(template_content, data)
        if not k8s_content:
            print("service: %s k8s config is empty!" % service_name)
            return False
        service_k8s_filename = self.generate_k8s_file(service_name, k8s_content)
        clear_ret = subprocess.run("kubectl delete -f %s" % service_k8s_filename, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        ret = subprocess.run("kubectl create -f %s" % service_k8s_filename, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        if ret.returncode != 0:
            print("service: %s k8s create fail!" % (service_name), ret)
            return False
        return True

    def delete_k8s_service(self, service_name, template_content, data):
        k8s_content = k8s_template(template_content, data)
        if not k8s_content:
            print("service: %s k8s config is empty!" % service_name)
            return False
        service_k8s_filename = self.generate_k8s_file(service_name, k8s_content)
        ret = subprocess.run("kubectl delete -f %s" % service_k8s_filename, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        if ret.returncode != 0:
            print("service: %s k8s delete fail!" % (service_name), ret)
            return False
        return True

    def k8s_service(self, service_name, command, template, data, default):
        if not data:
            data = {}
        if not data and not default:
            print("service:%s config data is empty! %s fail" % (service_name, command))
            return
        for key, value in default.items():
            if key not in data or data.get(key) is None:
                data[key] = value
        if command == "up":
            if self.create_k8s_service(service_name, template, data):
                print("%s k8s service create successfully!"%service_name)
            else:
                print("%s k8s service create fail!" % service_name)
        elif command == "down":
            if self.delete_k8s_service(service_name, template, data):
                print("%s k8s service delete successfully!" % service_name)
            else:
                print("%s k8s service delete fail!" % service_name)

    def k8s_consul(self, data, command):
        from metasporeflow.online.k8s_template.consul_template import template, default
        self.k8s_service("consul-server", command, template, data, default)

    def k8s_recommend(self, data, command):
        from metasporeflow.online.k8s_template.recommend_template import template, default
        self.k8s_service("recommend-service", command, template, data, default)

    def k8s_model(self, data, command):
        from metasporeflow.online.k8s_template.model_template import template, default
        self.k8s_service("model-serving", command, template, data, default)


if __name__ == '__main__':
    from metasporeflow.flows.flow_loader import FlowLoader
    from metasporeflow.online.online_flow import OnlineFlow
    import asyncio

    flow_loader = FlowLoader()
    flow_loader._file_name = 'test/metaspore-flow.yml'
    resources = flow_loader.load()

    online_flow = resources.find_by_type(OnlineFlow)
    print(type(online_flow))
    print(online_flow)


    flow_executor = OnlineK8sExecutor(resources)
    flow_executor.execute_up()

    widedeep_model_info = '''
    {
    "name": "amazonfashion_widedeep",
    "service": "model-k8s-service",
    "path": "s3://dmetasoul-bucket/qinyy/test-model-watched/amazonfashion_widedeep",
    "version": "20221024",
    "util_cmd": "aws s3 cp --recursive"
    }
    '''
    consul_client = Consul("consul-k8s-service.huawei.dmetasoul.com", 80)
    putConfigByKey(consul_client, widedeep_model_info, "dev/amazonfashion_widedeep")
    
