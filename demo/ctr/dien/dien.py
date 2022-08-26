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

import metaspore as ms
import pyspark
import numpy as np
import yaml
import subprocess
import argparse
import sys 
from operator import itemgetter

sys.path.append('../../../')
from python.algos.sequential import DIEN,DIENAgent

def load_config(path):
    params=dict()
    with open(path,'r') as stream:
        params=yaml.load(stream,Loader=yaml.FullLoader)
        print('Debug--load config:',params)
    return params

def init_spark():
    subprocess.run(['zip', '-r', 'demo/ctr/dien/python.zip', 'python'], cwd='../../../')
    spark_confs={
        "spark.network.timeout":"500",
        "spark.submit.pyFiles":"python.zip",
    }
    spark_session = ms.spark.get_session(local=local,
                                        app_name=app_name,
                                        batch_size=batch_size,
                                        worker_count=worker_count,
                                        server_count=server_count,
                                        worker_memory=worker_memory,
                                        server_memory=server_memory,
                                        coordinator_memory=coordinator_memory,
                                        spark_confs=spark_confs)
    
    sc = spark_session.sparkContext
    print('Debug -- spark init')
    print('Debug -- version:', sc.version)   
    print('Debug -- applicaitonId:', sc.applicationId)
    print('Debug -- uiWebUrl:', sc.uiWebUrl)
    return spark_session

def stop_spark(spark):
    print('Debug--spark stop')
    spark.sparkContext.stop()

def read_dataset(spark):
    train_dataset=spark.read.parquet(train_path)
    test_dataset=spark.read.parquet(test_path)
    print('Debug -- match train dataset sample:')
    train_dataset.show(20)
    print('Debug--match train test dataset sample:')
    test_dataset.show(10)
    print('Debug -- train dataset positive count:', train_dataset[train_dataset['label']=='1'].count())
    print('Debug -- train dataset negative count:', train_dataset[train_dataset['label']=='0'].count())
    print('Debug -- test dataset count:', test_dataset.count())
    return train_dataset, test_dataset
    
def train(spark, trian_dataset, **model_params):
    ## init wide and deep model
    module = DIEN(embedding_size=embedding_size,
                aux_mlp_layer=aux_mlp_layer,
                gru_num_layer = gru_num_layer,
                att_hidden_size = att_hidden_size,
                dnn_hidden_mlp_list=dnn_hidden_mlp_list,
                column_name_path=column_name_path,
                combine_schema_path=combine_schema_path,
                sparse_init_var=sparse_init_var,
                feature_slice = feature_slice,
                aux_mlp_bn= aux_mlp_bn,
                aux_mlp_dropout = aux_mlp_dropout,
                aux_mlp_activation= aux_mlp_activation,
                att_hidden_bn= att_hidden_bn,
                att_hidden_dropout=att_hidden_dropout,
                att_hidden_activation= att_hidden_activation,
                dnn_hidden_mlp_bn= dnn_hidden_mlp_bn,
                dnn_hidden_mlp_dropout= dnn_hidden_mlp_dropout,
                dnn_hidden_mlp_activation= dnn_hidden_mlp_activation,
                ) #None
    
    
    estimator = ms.PyTorchEstimator(module=module,
                                  worker_count=worker_count,
                                  server_count=server_count,
                                  agent_class = DIENAgent,
                                  model_out_path=model_out_path,
                                  model_export_path=None,
                                  model_version=model_version,
                                  experiment_name=experiment_name,
                                  metric_update_interval=metric_update_interval,
                                  input_label_column_index=input_label_column_index,
                                  target_loss_weight=target_loss_weight,
                                  auxilary_loss_weight = auxilary_loss_weight) #100
    train_dataset.show(20)
    model = estimator.fit(trian_dataset)
     ## dnn learning rate
    estimator.updater = ms.AdamTensorUpdater(adam_learning_rate)
    import subprocess
    run_res = subprocess.run(['aws', 's3', 'ls', model_out_path], 
                            stdout=subprocess.PIPE)
    print('Debug -- model output:')
    print("%s\n"%run_res.stdout.decode("utf-8"))
    return model

def transform(spark, model, test_dataset):
    test_result = model.transform(test_dataset)
    print('Debug -- test result sample:')
    test_result.show(20)
    return test_result

def evaluate(spark, test_result):
    evaluator = pyspark.ml.evaluation.BinaryClassificationEvaluator()
    auc = evaluator.evaluate(test_result)
    return auc

if __name__=="__main__":
    print('Debug -- CTR Demo DIEN')
    parser = argparse.ArgumentParser()
    parser.add_argument('--conf', type=str, action='store', default='', help='config file path')
    args = parser.parse_args()
    params = load_config(args.conf)
    locals().update(params)
    spark = init_spark()
    ## read datasets
    train_dataset, test_dataset = read_dataset(spark)
    ## train
    model = train(spark, train_dataset, **params)
    ## transform
    train_result = transform(spark, model, train_dataset)
    test_result = transform(spark, model, test_dataset)
    ## evaluate
    train_auc = evaluate(spark, train_result)
    test_auc = evaluate(spark, test_result)
    print('Debug -- Train AUC: ', train_auc)
    print('Debug -- Test AUC: ', test_auc)
    stop_spark(spark)
