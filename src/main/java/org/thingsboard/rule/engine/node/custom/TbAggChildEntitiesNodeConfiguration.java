/**
 * Copyright © 2018 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.rule.engine.node.custom;

import lombok.Data;
import org.thingsboard.rule.engine.api.NodeConfiguration;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Data
public class TbAggChildEntitiesNodeConfiguration implements NodeConfiguration<TbAggChildEntitiesNodeConfiguration> {

    private TimeUnit periodTimeUnit;
    private int periodValue;

    private int fetchParentEntitiesPeriod;
    private TimeUnit fetchParentEntitiesPeriodTimeUnit;

    private int relationsMaxLevel;

    private String parentEntityType;
    private List<String> childEntitiesTypes;
    private String relationType;

    private String attributeKey;
    private List<String> attributeValues;

    @Override
    public TbAggChildEntitiesNodeConfiguration defaultConfiguration() {
        TbAggChildEntitiesNodeConfiguration configuration = new TbAggChildEntitiesNodeConfiguration();
        configuration.setPeriodValue(1);
        configuration.setPeriodTimeUnit(TimeUnit.MINUTES);
        configuration.setFetchParentEntitiesPeriod(10);
        configuration.setFetchParentEntitiesPeriodTimeUnit(TimeUnit.MINUTES);
        configuration.setRelationsMaxLevel(1);
        configuration.setRelationType("Contains");
        configuration.setParentEntityType("");
        configuration.setChildEntitiesTypes(Collections.emptyList());
        configuration.setAttributeKey("");
        configuration.setAttributeValues(Collections.emptyList());
        return configuration;
    }
}
