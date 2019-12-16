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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.page.TextPageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.EntityRelationsQuery;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.msg.session.SessionMsgType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.thingsboard.rule.engine.api.TbRelationTypes.SUCCESS;

@Slf4j
@RuleNode(
        type = ComponentType.ANALYTICS,
        name = "child entities count",
        configClazz = TbAggChildEntitiesNodeConfiguration.class,
        nodeDescription = "Puts the number of related entities into timeseries",
        nodeDetails = "Performs aggregation of child entities and devices with specified types with configurable period. " +
                "Generates 'POST_TELEMETRY_REQUEST' messages with aggregated values for each parent entity.",
        inEnabled = false,
        //uiResources = {"static/rulenode/rulenode-core-config.js", "static/rulenode/rulenode-core-config.css"},
        //configDirective = "tbAnalyticsNodeAggregateLatestConfig",
        icon = "functions"
)
public class TbAggChildEntitiesNode implements TbNode {

    private static final String TB_AGE_CHILD_DATA_NODE_MSG = "CHILD_AGG";
    private static final Gson GSON = new Gson();

    private TbAggChildEntitiesNodeConfiguration config;
    private ScheduledExecutorService executorService;
    private String trainType;
    private UUID nextTickId;
    private long delay;
    private long lastScheduledTs;
    private int relationsMaxLevel;

    private List<Asset> trains = new ArrayList<>();
    private List<String> childEntitiesTypes;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbAggChildEntitiesNodeConfiguration.class);
        trainType = config.getParentEntityType();
        childEntitiesTypes = config.getChildEntitiesTypes();
        delay = config.getPeriodTimeUnit().toMillis(config.getPeriodValue());
        relationsMaxLevel = config.getRelationsMaxLevel();
        executorService = Executors.newScheduledThreadPool(5);
        executorService.scheduleAtFixedRate(() -> {
            trains = ctx.getAssetService().findAssetsByTenantIdAndType(ctx.getTenantId(), trainType, new TextPageLink(1000)).getData();
        }, 0, config.getFetchParentEntitiesPeriod(), config.getFetchParentEntitiesPeriodTimeUnit());
        scheduleTickMsg(ctx);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        if (msg.getType().equals(TB_AGE_CHILD_DATA_NODE_MSG) && msg.getId().equals(nextTickId)) {
            List<ListenableFuture<TbMsg>> msgFutures = process(ctx);
            DonAsynchron.withCallback(tellNextMessages(ctx, msg, msgFutures), result -> scheduleTickMsg(ctx), throwable -> {
                ctx.tellFailure(msg, throwable);
                scheduleTickMsg(ctx);
            }, ctx.getDbCallbackExecutor());
        }
    }

    @Override
    public void destroy() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private ListenableFuture<Boolean> tellNextMessages(TbContext ctx, TbMsg msg, List<ListenableFuture<TbMsg>> msgFutures) {
        return Futures.transformAsync(Futures.allAsList(msgFutures), messages -> {
            if (messages != null) {
                messages.forEach(outMsg -> ctx.tellNext(outMsg, SUCCESS));
            }
            return Futures.immediateFuture(true);
        });
    }

    private void scheduleTickMsg(TbContext ctx) {
        long curTs = System.currentTimeMillis();
        if (lastScheduledTs == 0L) {
            lastScheduledTs = curTs;
        }
        lastScheduledTs = lastScheduledTs + delay;
        long curDelay = Math.max(0L, (lastScheduledTs - curTs));
        TbMsg tickMsg = ctx.newMsg(TB_AGE_CHILD_DATA_NODE_MSG, ctx.getSelfId(), new TbMsgMetaData(), "");
        nextTickId = tickMsg.getId();
        ctx.tellSelf(tickMsg, curDelay);
    }

    private List<ListenableFuture<TbMsg>> process(TbContext ctx) {
        List<ListenableFuture<TbMsg>> msgFutures = new ArrayList<>();
        for (Asset asset : trains) {
            ListenableFuture<List<EntityRelation>> entityRelationsFuture = ctx.getRelationService().findByQuery(ctx.getTenantId(), buildQuery(asset.getId()));
            ListenableFuture<List<EntityId>> relatedEntitiesFuture = getRelatedEntities(ctx, entityRelationsFuture);
            msgFutures.add(getListenableFutureMsgEntities(ctx, relatedEntitiesFuture, asset.getId()));
        }
        return msgFutures;
    }

    private EntityRelationsQuery buildQuery(EntityId originator) {
        EntityRelationsQuery query = new EntityRelationsQuery();
        RelationsSearchParameters parameters = new RelationsSearchParameters(originator,
                EntitySearchDirection.FROM, relationsMaxLevel, false);
        query.setParameters(parameters);
        return query;
    }

    private ListenableFuture<TbMsg> getListenableFutureMsgEntities(TbContext ctx, ListenableFuture<List<EntityId>> relatedEntitiesFuture, EntityId trainId) {
        return Futures.transformAsync(relatedEntitiesFuture, entityIds -> {
            Map<String, AtomicInteger> childEntitiesCounts = new ConcurrentHashMap<>();
            for (String type : childEntitiesTypes) {
                childEntitiesCounts.put(type, new AtomicInteger(0));
            }
            if (entityIds != null && !entityIds.isEmpty()) {
                for (EntityId tempEntityId : entityIds) {
                    String entityType = tempEntityId.getEntityType().toString();
                    switch (entityType) {
                        case "ASSET":
                            Asset tempAsset = ctx.getAssetService().findAssetById(ctx.getTenantId(), new AssetId(UUID.fromString(tempEntityId.toString())));
                            entityType = tempAsset.getType();
                            break;
                        case "DEVICE":
                            Device tempDevice = ctx.getDeviceService().findDeviceById(ctx.getTenantId(), new DeviceId(UUID.fromString((tempEntityId.toString()))));
                            entityType = tempDevice.getType();
                            break;
                        default:
                            log.warn("Entity is not a device or asset!");
                            break;

                    }
                    childEntitiesCounts.computeIfPresent(entityType, (key, val) -> new AtomicInteger(val.incrementAndGet()));
                }
            }
            TbMsg newMsg = ctx.newMsg(String.valueOf(SessionMsgType.POST_TELEMETRY_REQUEST), trainId, new TbMsgMetaData(), GSON.toJson(childEntitiesCounts));
            return Futures.immediateFuture(newMsg);
        });
    }

    private ListenableFuture<List<EntityId>> getRelatedEntities(TbContext ctx, ListenableFuture<List<EntityRelation>> relatedEntities) {
        return Futures.transformAsync(relatedEntities, entityRelations -> {
            if (!entityRelations.isEmpty()) {
                List<EntityId> listenableFutureListEntityIds = new ArrayList<>();
                for (EntityRelation entityRelation : entityRelations) {
                    listenableFutureListEntityIds.add(entityRelation.getTo());
                }
                return Futures.immediateFuture(listenableFutureListEntityIds);
            } else {
                return Futures.immediateFuture(Collections.emptyList());
            }
        }, ctx.getDbCallbackExecutor());

    }

}
