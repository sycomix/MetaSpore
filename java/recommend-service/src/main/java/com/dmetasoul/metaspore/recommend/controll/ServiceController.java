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
package com.dmetasoul.metaspore.recommend.controll;

import com.dmetasoul.metaspore.recommend.common.Utils;
import com.dmetasoul.metaspore.recommend.configure.FeatureConfig;
import com.dmetasoul.metaspore.recommend.configure.RecommendConfig;
import com.dmetasoul.metaspore.recommend.configure.TaskFlowConfig;
import com.dmetasoul.metaspore.recommend.data.DataContext;
import com.dmetasoul.metaspore.recommend.data.DataResult;
import com.dmetasoul.metaspore.recommend.data.ServiceRequest;
import com.dmetasoul.metaspore.recommend.data.ServiceResult;
import com.dmetasoul.metaspore.recommend.TaskServiceRegister;
import com.dmetasoul.metaspore.recommend.dataservice.DataService;
import com.dmetasoul.metaspore.recommend.enums.DataTypeEnum;
import com.dmetasoul.metaspore.recommend.recommend.Experiment;
import com.dmetasoul.metaspore.recommend.recommend.Layer;
import com.dmetasoul.metaspore.recommend.recommend.Scene;
import com.dmetasoul.metaspore.recommend.recommend.Service;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

import static org.springframework.web.bind.annotation.RequestMethod.POST;

/**
 * 用于实现restfull接口
 * Created by @author qinyy907 in 14:24 22/07/15.
 */
@Slf4j
@RestController
@RequestMapping("/service")
public class ServiceController {

    @Autowired
    public TaskFlowConfig taskFlowConfig;

    @Autowired
    public TaskServiceRegister taskServiceRegister;

    /**
     * 用于实现restfull接口 /service/get/{task}
     * @param task 需要调用的任务名称，需要在配置中事先定义好，不可为空
     * @param req 需要传递给任务task的请求参数
     * @return ServiceResult 包含状态码，错误信息以及数据结果
     * Created by @author qinyy907 in 14:24 22/07/15.
     */
    @SuppressWarnings("unchecked")
    @RequestMapping(value = "/get/{task}", method = POST, produces = "application/json")
    public ServiceResult getDataServiceResult(@PathVariable String task, @RequestBody Map<String, Object> req) {
        DataService taskService = taskServiceRegister.getDataService(task);
        if (taskService == null) {
            return ServiceResult.of(-1, "taskService is not exist!");
        }
        DataContext context = new DataContext(req);
        List<String> services = null;
        if (taskFlowConfig.getFeatures().containsKey(task) && taskFlowConfig.getFeatureRelyServices().containsKey(task)) {
            services = taskFlowConfig.getFeatureRelyServices().get(task);
        }
        if (taskFlowConfig.getAlgoTransforms().containsKey(task)) {
            FeatureConfig.AlgoTransform algoTransform = taskFlowConfig.getAlgoTransforms().get(task);
            services = getRelyServiceList(algoTransform);
        }
        if (CollectionUtils.isNotEmpty(services)) {
            for (String item : services) {
                if (!req.containsKey(item) || !(req.get(item) instanceof List)) {
                    return ServiceResult.of(-1, "taskService need depend data: " + item);
                }
                List<Map<String, Object>> data = (List<Map<String, Object>>) req.get(item);
                RecommendConfig.Service serviceConfig = taskFlowConfig.getServices().get(item);
                List<Field> fields = Lists.newArrayList();
                List<DataTypeEnum> types = Lists.newArrayList();
                if (CollectionUtils.isNotEmpty(serviceConfig.getColumnNames())) {
                    for (String col : serviceConfig.getColumnNames()) {
                        fields.add(serviceConfig.getFieldMap().get(col));
                        types.add(serviceConfig.getColumnMap().get(col));
                    }
                    DataResult resultItem = new DataResult();
                    resultItem.setFeatureData(item, fields, types, data);
                    taskService.setDataResultByName(item, resultItem, context);
                }
            }
        }
        DataResult result;
        result = taskService.execute(new ServiceRequest(req), context);
        if (result == null) {
            return ServiceResult.of(-1, "taskService execute fail!");
        }
        return ServiceResult.of(result.output());
    }

    private List<String> getRelyServiceList(FeatureConfig.AlgoTransform algoTransform) {
        List<String> services = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(algoTransform.getFeature())) {
            for (String table : algoTransform.getFeature()) {
                if (taskFlowConfig.getFeatureRelyServices().containsKey(table)) {
                    services.addAll(taskFlowConfig.getFeatureRelyServices().get(table));
                }
            }
        }
        if (CollectionUtils.isNotEmpty(algoTransform.getAlgoTransform())) {
            for (String table : algoTransform.getAlgoTransform()) {
                FeatureConfig.AlgoTransform algo = taskFlowConfig.getAlgoTransforms().get(table);
                services.addAll(getRelyServiceList(algo));
            }
        }
        return services;
    }

    @SneakyThrows
    private List<DataResult> executeTasks(List<DataResult> input, List<String> tasks, DataContext context) {
        List<DataResult> result = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(tasks)) {
            for (String task : tasks) {
                if (taskServiceRegister.getRecommendServices().containsKey(task)) {
                    Service taskService = taskServiceRegister.getRecommendService(task);
                    result.addAll(taskService.execute(input, context).get());
                } else if (taskServiceRegister.getExperimentMap().containsKey(task)) {
                    Experiment taskService = taskServiceRegister.getExperiment(task);
                    result.addAll(taskService.process(input, context).get());
                } else if (taskServiceRegister.getLayerMap().containsKey(task)) {
                    Layer taskService = taskServiceRegister.getLayer(task);
                    result.addAll(taskService.execute(input, context).get());
                } else if (taskServiceRegister.getSceneMap().containsKey(task)) {
                    Scene taskService = taskServiceRegister.getScene(task);
                    result.add(taskService.process(context));
                }
            }
        }
        return result;
    }

    @SneakyThrows
    @RequestMapping(value = "/recommend/{task}", method = POST, produces = "application/json")
    public ServiceResult getRecommendResult(@PathVariable String task, @RequestBody Map<String, Object> req) {
        DataContext context = new DataContext(req);
        List<String> preTasks = Utils.getField(req, "preTasks", List.of());
        List<DataResult> result = executeTasks(executeTasks(List.of(), preTasks, context), List.of(task), context);
        log.info("recommend result : {}", result);
        if (CollectionUtils.isEmpty(result)) {
            return ServiceResult.of(-1, "taskService execute fail!");
        }
        List<Map<String, Object>> output = Lists.newArrayList();
        for (DataResult item : result) {
            output.addAll(item.output());
        }
        return ServiceResult.of(output);
    }
    /**
     * 用于实现restfull接口 /service/recommend/{scene}/{id}
     * req 设置debug字段输出各个阶段的输出结果
     * @param scene 需要推荐的场景名称，需要在配置中事先定义好，不可为空
     * @param id 需要被推荐用户的id，比如user id，不可为空
     * @param req 需要传递给推荐任务的请求参数，可为空
     * @return ServiceResult 包含状态码，错误信息以及数据结果
     * Created by @author qinyy907 in 14:24 22/07/15.
     */
    @RequestMapping(value = "/recommend/{scene}/{id}", method = POST, produces = "application/json")
    public ServiceResult recommend(@PathVariable String scene, @PathVariable String id, @RequestBody Map<String, Object> req) {
        Scene sceneService = taskServiceRegister.getScene(scene);
        if (sceneService == null) {
            return ServiceResult.of(-1, String.format("scene:%s is not support!", scene));
        }
        DataContext context = new DataContext(req);
        if (StringUtils.isEmpty(id)) {
            return ServiceResult.of(-1, String.format("scene:%s recommend need id, eg:userId!", scene));
        }
        context.setId(id);
        List<Map<String, Object>> data = sceneService.output(context);
        return ServiceResult.of(data, id);
    }
}
