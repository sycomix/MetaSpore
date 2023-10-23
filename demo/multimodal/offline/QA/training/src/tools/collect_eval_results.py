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

import os
import csv
import sys

def read_csv(csv_file):
    with open(csv_file, 'r', encoding='utf8') as fin:
        reader = csv.DictReader(fin)
        rows = list(reader)
    row = rows[-1] if rows else None
    return (row['cosine_pearson'], row['cosine_spearman']) if row else (0.0, 0.0)


task_list = ['csts', 'ocnli', 'cmnli', 'csnli', 'lcqmc', 'pawsx', 'xiaobu', 'afqmc', 'bqcorpus']
print('', *task_list, sep='\t')

eval_dir = sys.argv[1]
for model_name in os.listdir(eval_dir):
    model_dir = os.path.join(eval_dir, model_name)
    task_results = {}
    for task_file in os.listdir(model_dir):
        task_name = task_file.split('_')[2]
        task_file = os.path.join(model_dir, task_file)
        pearson, spearman = read_csv(task_file)
        task_results[task_name] = spearman
    print(model_name, *[task_results.get(n, 0.0) for n in task_list], sep='\t')
