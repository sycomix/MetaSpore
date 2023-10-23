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
from python.algos.sequential import DIEN, DIENAgent

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
    module = DIEN(column_name_path = column_name_path,
                dien_combine_schema_path = dien_combine_schema_path,
                wide_combine_schema_path = wide_combine_schema_path,
                deep_combine_schema_path = deep_combine_schema_path,
                sparse_init_var = sparse_init_var,
                use_wide = use_wide,
                use_deep = use_deep,
                pos_item_seq = pos_item_seq,
                neg_item_seq = neg_item_seq,
                target_item = target_item,        
                dien_embedding_size = dien_embedding_size,
                dien_gru_num_layer = dien_gru_num_layer,
                dien_aux_hidden_units = dien_aux_hidden_units,
                dien_use_aux_bn = dien_use_aux_bn,
                dien_aux_dropout = dien_aux_dropout,
                dien_aux_activation = dien_aux_activation,
                dien_att_hidden_units = dien_att_hidden_units,
                dien_use_att_bn = dien_use_att_bn,
                dien_att_dropout = dien_att_dropout,
                dien_att_activation = dien_att_activation,
                dien_dnn_hidden_units = dien_dnn_hidden_units,
                dien_use_dnn_bn = dien_use_dnn_bn,
                dien_dnn_dropout = dien_dnn_dropout,
                dien_dnn_activation = dien_dnn_activation,   
                dien_use_gru_bias = dien_use_gru_bias,
                deep_hidden_units = deep_hidden_units,
                deep_dropout = deep_dropout,
                deep_activation = deep_activation,
                use_deep_bn = use_deep_bn,
                use_deep_bias = use_deep_bias,
                deep_embedding_size = deep_embedding_size,
                wide_embedding_size = wide_embedding_size
                ) #None
    
    
    estimator = ms.PyTorchEstimator(module = module,
                                  worker_count = worker_count,
                                  server_count = server_count,
                                  agent_class = DIENAgent,
                                  model_out_path = model_out_path,
                                  model_export_path = None,
                                  model_version = model_version,
                                  experiment_name = experiment_name,
                                  metric_update_interval = metric_update_interval,
                                  input_label_column_index = input_label_column_index,
                                  dien_target_loss_weight = dien_target_loss_weight,
                                  dien_auxilary_loss_weight = dien_auxilary_loss_weight,
                                  shuffle_training_dataset = shuffle_training_dataset,
                                  training_epoches = training_epoches) 
    train_dataset.show(20)
    model = estimator.fit(trian_dataset)
     ##learning rate
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
    return evaluator.evaluate(test_result)

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
