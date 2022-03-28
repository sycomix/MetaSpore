//
// Copyright 2022 DMetaSoul
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package com.dmetasoul.metaspore.demo.movielens.abtesting.layer.bucketizer;

import com.dmetasoul.metaspore.demo.movielens.model.RecommendContext;

public interface LayerBucketizer {
    public String toBucket(RecommendContext context);

    public default double[] normalize(double[] prob) {
        double total = 0;
        for (int i = 0; i < prob.length; i++) {
            total += prob[i];
        }
        if (total == 0) {
            throw new IllegalArgumentException("Sum of probability is zero...");
        }

        double[] probability = new double[prob.length];
        for (int i = 0; i < probability.length; i++) {
            probability[i] = prob[i] / total;
        }

        return probability;
    }
}