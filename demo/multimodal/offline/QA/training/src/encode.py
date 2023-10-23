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

import time
import logging
import argparse

import numpy as np
from tqdm import tqdm
from sentence_transformers import SentenceTransformer

logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()


parser.add_argument("--input-file", required=True, help="The input corpus file path.")
parser.add_argument("--output-file", default="", help="The output embedding file path.")
parser.add_argument("--model", default="bert-base-uncased", help="The pre-trained encoder model name or path.")
parser.add_argument("--batch-size", type=int, default=64, help="The inference batch size.")
parser.add_argument("--output-normalized", action="store_true", help="The output embeddings would be normalized.")
parser.add_argument("--device", default="cuda:0", help="The cuda device for training")
parser.add_argument("--max-seq-len", default=256, type=int, help="The max length of sequence.")
parser.add_argument("--disable-progress", action="store_true", help="Disable progress bar.")
args = parser.parse_args()


#logger.info("Loading model: {}".format(args.model))
model = SentenceTransformer(args.model, device=args.device)
model.max_seq_length = args.max_seq_len

#logger.info("Loading data from {}...".format(args.input_file))
sentences = []
attrs = []
with open(args.input_file, 'r', encoding='utf8') as fin:
    for line in tqdm(fin):
        line = line.strip('\r\n')
        if not line:
            continue
        fields = line.split('\t')
        sentences.append(fields[0])
        attrs.append(fields[1:])

#logger.info("Start embedding...")
embeddings = []
batch = []
latency = []
for sent in sentences:
    batch.append(sent)
    if len(batch) == args.batch_size:
        start_time = time.time()
        embs = model.encode(batch, batch_size=args.batch_size, 
            convert_to_numpy=True, normalize_embeddings=args.output_normalized, show_progress_bar=not args.disable_progress)
        latency.append((time.time()-start_time)*1000)
        embeddings.extend(embs)
        batch = []
if batch:
    embs = model.encode(batch, batch_size=args.batch_size, 
        convert_to_numpy=True, normalize_embeddings=args.output_normalized, show_progress_bar=not args.show_progress)
    embeddings.extend(embs)
    batch = []
cost_time = sum(latency)/1000
#logger.info("Embedding cost {}s, {}sentence/s, latency {}ms".format(int(cost_time), 
#    len(sentences)/cost_time, sum(latency)/len(latency)))
print('samples', 'cost', 'throughput', 'latency', sep='\t')
print(len(sentences), int(cost_time), len(sentences)/cost_time, sum(latency)/len(latency), sep='\t')

if args.output_file:
    logger.info(f"Dumping data to {args.output_file}")
    with open(args.output_file, 'w', encoding='utf8') as fout:
        for i, (emb, attr) in enumerate(zip(embeddings, attrs)):
            attr = '\t'.join(attr)
            emb = ' '.join([str(x) for x in emb.tolist()])
            fout.write(f'{i}\t{emb}\t{attr}\n')
