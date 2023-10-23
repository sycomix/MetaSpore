import decimal
import json

from metasporeflow.online.common import dictToObj

import boto3
import datetime
import time
import re
import os
import shutil
import tempfile
import tarfile

from metasporeflow.online.online_generator import OnlineGenerator

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        elif isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return json.JSONEncoder.default(self, obj)


class SageMakerExecutor(object):
    def __init__(self, resources):
        from metasporeflow.online.online_flow import OnlineFlow
        self.now_time = datetime.datetime.now()
        self.model_version = datetime.datetime.strftime(self.now_time, "%Y%m%d-%H%M%S")
        self._resources = resources
        self._online_resource = resources.find_by_type(OnlineFlow)
        self._generator = OnlineGenerator(resource=self._online_resource)
        self.server_config = self._generator.gen_server_config()
        self.configure = self._online_resource.data
        self.region = self._get_aws_region()
        self.role = self._get_iam_role()
        config = self._get_boto3_client_config()
        self.sm_client = boto3.client("sagemaker", self.region, config=config)
        self.runtime_sm_client = boto3.client("runtime.sagemaker", self.region, config=config)
        self.account_id = boto3.client("sts", self.region, config=config).get_caller_identity()["Account"]
        self.bucket, self.prefix = self._get_bucket_and_prefix()

    def _get_scene_name(self):
        import re
        from metasporeflow.flows.metaspore_flow import MetaSporeFlow
        flow_resource = self._resources.find_by_type(MetaSporeFlow)
        return flow_resource.name

    def _get_endpoint_name(self):
        scene_name = self._get_scene_name()
        return re.sub('[^A-Za-z0-9]', '-', scene_name)

    def _get_sage_maker_config(self):
        from metasporeflow.flows.sage_maker_config import SageMakerConfig
        resource = self._resources.find_by_type(SageMakerConfig)
        return resource.data

    def _get_iam_role(self):
        config = self._get_sage_maker_config()
        return config.roleArn

    def _get_s3_endpoint(self):
        config = self._get_sage_maker_config()
        return config.s3Endpoint

    def _get_s3_work_dir(self):
        config = self._get_sage_maker_config()
        return config.s3WorkDir

    def _get_serving_dir(self):
        s3_work_dir = self._get_s3_work_dir()
        scene_name = self._get_scene_name()
        flow_dir = os.path.join(s3_work_dir, 'flow')
        scene_dir = os.path.join(flow_dir, 'scene', scene_name)
        model_dir = os.path.join(scene_dir, 'model')
        return os.path.join(model_dir, 'serving')

    def _get_aws_region(self):
        import re
        pattern = r's3\.([A-Za-z0-9\-]+?)\.amazonaws\.com(\.cn)?$'
        s3_endpoint = self._get_s3_endpoint()
        match = re.match(pattern, s3_endpoint)
        if match is None:
            message = 'invalid s3 endpoint %r' % s3_endpoint
            raise RuntimeError(message)
        return match.group(1)

    def _get_bucket_and_prefix(self):
        from urllib.parse import urlparse
        serving_dir = self._get_serving_dir()
        results = urlparse(serving_dir, allow_fragments=False)
        bucket = results.netloc
        prefix = results.path.strip('/') + '/'
        return bucket, prefix

    def _get_container_image(self):
        return (
            '132825542956.dkr.ecr.cn-northwest-1.amazonaws.com.cn'
            + '/dmetasoul-repo/metaspore-sagemaker-release:v1.0.6'
        )

    def _endpoint_exists(self, endpoint_name):
        import botocore
        try:
            _response = self.sm_client.describe_endpoint(EndpointName=endpoint_name)
            return True
        except botocore.exceptions.ClientError:
            return False

    def _get_boto3_client_config(self):
        from botocore.config import Config
        config = Config(connect_timeout=5, read_timeout=60, retries={'max_attempts': 20})
        return config

    def _get_endpoint_status(self, endpoint_name):
        import boto3
        import botocore
        config = self._get_boto3_client_config()
        client = boto3.client('sagemaker', self.region, config=config)
        try:
            response = client.describe_endpoint(EndpointName=endpoint_name)
        except botocore.exceptions.ClientError as ex:
            message = "endpoint %r not found" % endpoint_name
            raise RuntimeError(message) from ex
        return response['EndpointStatus']

    def _wait_endpoint(self, endpoint_name):
        import time
        counter = 0
        while True:
            status = self._get_endpoint_status(endpoint_name)
            if counter > 7200:
                message = 'fail to wait endpoint %r' % endpoint_name
                raise RuntimeError(message)
            if counter % 60 == 0:
                print('Wait endpoint %r ... [%s]' % (endpoint_name, status))
            if status in ('InService', 'Failed'):
                return status
            time.sleep(1)
            counter += 1

    def create_model(self, endpoint_name, key):
        model_name = f"{endpoint_name}-model-{self.model_version}"
        model_url = f"s3://{self.bucket}/{key}"
        container_image = self._get_container_image()
        environment = dict()
        environment["CONSUL_ENABLE"] = "false"
        environment["SERVICE_PORT"] = "8080"
        container = {"Image": container_image, "ModelDataUrl": model_url, "Environment": environment}
        config = self._get_sage_maker_config()
        if config.securityGroups and config.subnets:
            vpc_config = {
                'SecurityGroupIds': config.securityGroups,
                'Subnets': config.subnets,
            }
            create_model_response = self.sm_client.create_model(
                ModelName=model_name,
                ExecutionRoleArn=self.role,
                Containers=[container],
                VpcConfig=vpc_config
            )
        else:
            create_model_response = self.sm_client.create_model(
                ModelName=model_name,
                ExecutionRoleArn=self.role,
                Containers=[container],
            )
        print("model resp:", create_model_response)
        print("Model Arn: " + create_model_response["ModelArn"])
        resp = self.sm_client.describe_model(ModelName=model_name)
        print(f"model: {model_name} resp: {json.dumps(resp, cls=DateEncoder)}")
        print("model_url=", model_url)
        return self.create_endpoint_config(endpoint_name, model_name)

    def create_endpoint_config(self, endpoint_name, model_name):
        endpoint_config_name = f"{endpoint_name}-config-{self.model_version}"
        create_endpoint_config_response = self.sm_client.create_endpoint_config(
            EndpointConfigName=endpoint_config_name,
            ProductionVariants=[
                {
                    # NOTE: Default to ml.m5.2xlarge with 8 vCPUs and 32 GiB Memory
                    "InstanceType": "ml.m5.2xlarge",
                    "InitialInstanceCount": 1,
                    "InitialVariantWeight": 1,
                    "ModelName": model_name,
                    "VariantName": "variant-name-1",
                }
            ],
        )
        print("Endpoint config Arn: " + create_endpoint_config_response["EndpointConfigArn"])
        resp = self.sm_client.describe_endpoint_config(EndpointConfigName=endpoint_config_name)
        print(
            f"endpoint config: {endpoint_config_name} resp: {json.dumps(resp, cls=DateEncoder)}"
        )
        return endpoint_config_name

    def create_endpoint(self, endpoint_name, endpoint_config_name):
        create_endpoint_response = self.sm_client.create_endpoint(
            EndpointName=endpoint_name, EndpointConfigName=endpoint_config_name
        )
        print("Endpoint Arn: " + create_endpoint_response["EndpointArn"])

        resp = self.sm_client.describe_endpoint(EndpointName=endpoint_name)
        status = resp["EndpointStatus"]
        print(f"Endpoint Status: {status}")
        print(f"Waiting for {endpoint_name} endpoint to be in service...")
        status = self._wait_endpoint(endpoint_name)
        print(f"Endpoint Status: {status}")
        print(f"{endpoint_name} endpoint create successfully, is in service...")

    def update_endpoint(self, endpoint_name, endpoint_config_name):
        create_endpoint_response = self.sm_client.update_endpoint(
            EndpointName=endpoint_name, EndpointConfigName=endpoint_config_name
        )
        print("Endpoint Arn: " + create_endpoint_response["EndpointArn"])

        resp = self.sm_client.describe_endpoint(EndpointName=endpoint_name)
        status = resp["EndpointStatus"]
        print(f"Endpoint Status: {status}")
        print(f"Waiting for {endpoint_name} endpoint to be update in service...")
        status = self._wait_endpoint(endpoint_name)
        print(f"Endpoint Status: {status}")
        print(f"{endpoint_name} endpoint update successfully is in service...")

    def create_or_update_endpoint(self, endpoint_name, endpoint_config_name):
        if self._endpoint_exists(endpoint_name):
            self.update_endpoint(endpoint_name, endpoint_config_name)
        else:
            self.create_endpoint(endpoint_name, endpoint_config_name)

    def invoke_endpoint(self, endpoint_name, request):
        resp = self.sm_client.describe_endpoint(EndpointName=endpoint_name)
        status = resp["EndpointStatus"]
        print(f"Endpoint:{endpoint_name} Status: {status}")
        print(f"Endpoint:{endpoint_name} resp: {json.dumps(resp, cls=DateEncoder)}")
        if not isinstance(request, dict):
            print("request type is not match request dict")
            return None
        request_body = json.dumps(request)
        response = self.runtime_sm_client.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType="application/json",
            Accept='application/json',
            Body=request_body,
        )
        return json.loads(response["Body"].read())

    def add_model_to_s3(self, endpoint_name, model_paths):
        temp_dir = tempfile.mkdtemp()
        try:
            s3 = boto3.resource('s3', self.region)
            model = self.process_model_info(endpoint_name, model_paths, temp_dir)
            key = os.path.join(self.prefix, os.path.basename(model))
            with open(model, "rb") as file_obj:
                s3.Bucket(self.bucket).Object(key).upload_fileobj(file_obj)
            return key
        finally:
            shutil.rmtree(temp_dir)

    def process_model_info(self, endpoint_name, model_paths, temp_dir):
        tarball_dir = os.path.join(temp_dir, "tarball_dir")
        os.makedirs(tarball_dir)
        config_file = os.path.join(tarball_dir, "recommend-config.yaml")
        with open(config_file, "w") as file:
            file.write(self.server_config)
        model_info_file = os.path.join(tarball_dir, "model-infos.json")
        model_infos = []
        for model_name, model_prefix in model_paths.items():
            rel_model_path = os.path.join("model", model_name)
            rt_model_path = os.path.join("/opt/ml/model", rel_model_path)
            model_path = os.path.join(tarball_dir, rel_model_path)
            if not os.path.isdir(model_path):
                os.makedirs(model_path)
            bucket = self.bucket
            if model_prefix.startswith("s3://"):
                idx = model_prefix.find("/", len("s3://"))
                if idx == -1:
                    print(f"model: {model_name} path: {model_prefix} is error!")
                    continue
                bucket = model_prefix[len("s3://"):idx]
                model_prefix = model_prefix[idx + 1:]
            self.download_directory(bucket, model_prefix, model_path)
            model_info = dict(
                modelName=model_name,
                version="1",
                dirPath=rt_model_path,
                host="127.0.0.1",
                port=50000,
            )
            model_infos.append(model_info)
        with open(model_info_file, "w") as model_file:
            json.dump(model_infos, model_file)
            print(file=model_file)
        tarball_name = f"{endpoint_name}-{self.model_version}.tar.gz"
        tarball_path = os.path.join(temp_dir, tarball_name)
        with tarfile.open(tarball_path, "w:gz") as tar:
            for name in os.listdir(tarball_dir):
                path = os.path.join(tarball_dir, name)
                tar.add(path, name)
        shutil.rmtree(tarball_dir)
        return tarball_path

    def download_directory(self, bucket_name, path, local_path):
        if not os.path.isdir(local_path):
            os.mkdir(local_path)
        s3 = boto3.resource('s3', self.region)
        bucket = s3.Bucket(bucket_name)
        for obj in bucket.objects.filter(Prefix=path):
            local_file = os.path.join(local_path, obj.key[len(path):].lstrip("/"))
            if not os.path.isdir(os.path.dirname(local_file)):
                os.makedirs(os.path.dirname(local_file))
            key = obj.key
            print(f'Downloading {key}')
            bucket.download_file(key, local_file)

    def execute_up(self, **kwargs):
        model_paths = kwargs.get("models", {})
        endpoint_name = self._get_endpoint_name()
        model_data_path = self.add_model_to_s3(endpoint_name, model_paths)
        endpoint_config = self.create_model(endpoint_name, model_data_path)
        self.create_or_update_endpoint(endpoint_name, endpoint_config)

    def execute_down(self, **kwargs):
        endpoint_name = self._get_endpoint_name()
        try:
            self.sm_client.delete_endpoint(EndpointName=endpoint_name)
        except:
            print("the endpoint is not exist! or endpoint is creating")
        next_token = ''
        while next_token is not None:
            configs = self.sm_client.list_endpoint_configs(
                SortBy='CreationTime',
                SortOrder='Ascending',
                NameContains=f"{endpoint_name}-config-",
                MaxResults=10,
                NextToken=next_token,
            )
            for item in configs.get('EndpointConfigs', []):
                if item.get("EndpointConfigName"):
                    print("delete endpoint config:", item.get("EndpointConfigName"))
                    self.sm_client.delete_endpoint_config(EndpointConfigName=item.get("EndpointConfigName"))
            next_token = configs.get('NextToken')
        next_token = ''
        while next_token is not None:
            models = self.sm_client.list_models(
                SortBy='CreationTime',
                SortOrder='Ascending',
                NameContains=f"{endpoint_name}-model-",
                MaxResults=10,
                NextToken=next_token,
            )
            for item in models.get('Models', []):
                if item.get("ModelName"):
                    print("delete model:", item.get("ModelName"))
                    self.sm_client.delete_model(ModelName=item.get("ModelName"))
            next_token = models.get('NextToken')

    def execute_status(self, **kwargs):
        endpoint_name = self._get_endpoint_name()
        info = {"status": "DOWN"}
        try:
            resp = self.sm_client.describe_endpoint(EndpointName=endpoint_name)
            info[
                "endpoint_url"
            ] = f"https://{self.region}.console.amazonaws.cn/sagemaker/home?region={self.region}#/endpoints/{endpoint_name}"
            info["endpoint_status"] = resp["EndpointStatus"]
            if info["endpoint_status"] in ["InService", 'Updating']:
                info["status"] = "UP"
        except:
            info["endpoint_status"] = "NOT_EXIST"
            print("the endpoint is not exist! or endpoint is creating")
        if info.get('status', "DOWN") != "UP":
            info["msg"] = f"endpoint {endpoint_name} is not InService!"
            return info
        try:
            configs = self.sm_client.list_endpoint_configs(
                SortBy='CreationTime',
                SortOrder='Descending',
                NameContains=f"{endpoint_name}-config-",
            )
            info["endpoint_config_list"] = []
            for item in configs.get('EndpointConfigs', []):
                if item.get("EndpointConfigName"):
                    info["endpoint_config_list"].append(item.get("EndpointConfigName"))
            models = self.sm_client.list_models(
                SortBy='CreationTime',
                SortOrder='Descending',
                NameContains=f"{endpoint_name}-model-",
            )
            info["model_list"] = []
            for item in models.get('Models', []):
                if item.get("ModelName"):
                    info["model_list"].append(item.get("ModelName"))
        except:
            print("the model and endpoint config list fail!")
        return info

    def execute_reload(self, **kwargs):
        endpoint_name = self._get_endpoint_name()
        model_paths = kwargs.get("models", {})
        model_data_path = self.add_model_to_s3(endpoint_name, model_paths)
        endpoint_config = self.create_model(endpoint_name, model_data_path)
        self.create_or_update_endpoint(endpoint_name, endpoint_config)

    def execute_update(self):
        message = "execute_update is not supported by SageMaker"
        raise RuntimeError(message)

    def download_file(self, file_path, local_path):
        if os.path.isfile(local_path):
            os.remove(local_path)
        if not os.path.exists(local_path):
            os.mkdir(local_path)
        s3 = boto3.resource('s3', self.region)
        if not file_path.startswith("s3://"):
            print(f"file path: {file_path} is not s3 path!")
            return
        idx = file_path.find("/", len("s3://"))
        if idx == -1:
            print(f"file path: {file_path} is error!")
            return
        bucket_name = file_path[len("s3://"):idx]
        file_prefix = file_path[idx + 1:]
        bucket = s3.Bucket(bucket_name)
        local_file = os.path.join(local_path, os.path.basename(file_prefix))
        print(f'Downloading {file_prefix}')
        bucket.download_file(file_prefix, local_file)
        return local_file

if __name__ == "__main__":
    from metasporeflow.flows.flow_loader import FlowLoader
    from metasporeflow.online.online_flow import OnlineFlow

    flow_loader = FlowLoader()
    #flow_loader._file_name = 'metaspore-flow.yml'
    resources = flow_loader.load()
    #online_flow = resources.find_by_type(OnlineFlow)

    executor = SageMakerExecutor(resources)
    #print(executor.execute_update())
    #executor.execute_down()
    #executor.execute_up(models={"amazonfashion_widedeep": "s3://dmetasoul-test-bucket/qinyy/test-model-watched/amazonfashion_widedeep"})
    #executor.execute_reload(models={"amazonfashion_widedeep": "s3://dmetasoul-test-bucket/qinyy/test-model-watched/amazonfashion_widedeep"})
    print(executor.execute_status())
    #executor.execute_down()

    #executor.execute_up(models={"amazonfashion_widedeep": "s3://dmetasoul-test-bucket/qinyy/test-model-watched/amazonfashion_widedeep"})
    #with open("recommend-config.yaml") as config_file:
    #    res = executor.invoke_endpoint("guess-you-like", {"operator": "updateconfig", "config": config_file.read()})
    #    print(res)

    endpoint_name = executor._get_endpoint_name()
    res = executor.invoke_endpoint(endpoint_name, {"operator": "recommend", "request": {"user_id": "A1P62PK6QVH8LV", "scene": "guess-you-like"}})
    print(res)
    #executor.process_model_info("guess-you-like", {"amazonfashion_widedeep": "s3://dmetasoul-test-bucket/qinyy/test-model-watched/amazonfashion_widedeep"})
