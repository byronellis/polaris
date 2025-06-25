/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.persistence.relational.spanner.model;

import static org.apache.polaris.persistence.relational.spanner.util.SpannerUtil.INT64_TYPE;
import static org.apache.polaris.persistence.relational.spanner.util.SpannerUtil.JSON_TYPE;
import static org.apache.polaris.persistence.relational.spanner.util.SpannerUtil.STRING_TYPE;
import static org.apache.polaris.persistence.relational.spanner.util.SpannerUtil.column;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.pagination.HasPageOffset;
import org.apache.polaris.core.persistence.pagination.HasPageSize;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.persistence.relational.spanner.util.SpannerUtil;

public final class Entity {

  public static final String TABLE_NAME = "Entities";

  public static final String NAME_LOOKUP_INDEX = "EntityNameIndex";

  public static final String CHILDREN_INDEX = "EntityChildrenIndex";

  public static final String ENTITY_VERSION = "EntityVersion";
  public static final String GRANT_RECORDS_VERSION = "GrantRecordsVersion";

  public static final List<Struct> TABLE_SCHEMA =
      ImmutableList.copyOf(
          new Struct[] {
            column("RealmId", STRING_TYPE, false, true),
            column("CatalogId", INT64_TYPE, false, true),
            column("Id", INT64_TYPE, false, true),
            column("ParentId", INT64_TYPE, false, false),
            column("Name", STRING_TYPE, false, false),
            column(ENTITY_VERSION, INT64_TYPE, false, false),
            column("TypeCode", INT64_TYPE, false, false),
            column("SubTypeCode", INT64_TYPE, false, false),
            column("CreateTimestamp", INT64_TYPE, false, false),
            column("DropTimestamp", INT64_TYPE, false, false),
            column("PurgeTimestamp", INT64_TYPE, false, false),
            column("ToPurgeTimestamp", INT64_TYPE, false, false),
            column("LastUpdateTimestamp", INT64_TYPE, false, false),
            column("Properties", JSON_TYPE, false, false),
            column("InternalProperties", JSON_TYPE, false, false),
            column(GRANT_RECORDS_VERSION, INT64_TYPE, false, false),
          });

  public static final List<String> TABLE_COLUMNS =
      TABLE_SCHEMA.stream().map(column -> column.getString("Name")).toList();

  public static <T extends PolarisEntityCore> Key toKey(String realmId, T entity) {
    return Key.of(realmId, entity.getId());
  }

  public static Key toKey(String realmId, PolarisEntityId entityId) {
    return Key.of(realmId, entityId.getId());
  }

  public static Key toKey(String realmId, long entityId) {
    return Key.of(realmId, entityId);
  }

  public static KeyRange allEntitiesKey(String realmId) {
    return KeyRange.prefix(Key.of(realmId));
  }

  public static Key namedEntityKey(
      String realmId, long catalogId, long parentId, long typeCode, String name) {
    return Key.of(realmId, catalogId, parentId, typeCode, name);
  }

  public static PolarisBaseEntity fromStruct(Struct result) {
    if (result == null) {
      return null;
    }
    // Should this maybe use PolarisEntity? That has builders, but all of the persistence
    // stuff seems to deal with PolarisBaseEntity and PolarisEntity is built on that...
    PolarisBaseEntity entity =
        new PolarisBaseEntity(
            result.getLong("CatalogId"),
            result.getLong("Id"),
            PolarisEntityType.fromCode((int) result.getLong("TypeCode")),
            PolarisEntitySubType.fromCode((int) result.getLong("SubTypeCode")),
            result.getLong("ParentId"),
            result.isNull("Name") ? null : result.getString("Name"));
    // No constructor for these so set them manually
    entity.setCreateTimestamp(result.getLong("CreateTimestamp"));
    entity.setDropTimestamp(result.getLong("DropTimestamp"));
    entity.setPurgeTimestamp(result.getLong("PurgeTimestamp"));
    entity.setToPurgeTimestamp(result.getLong("ToPurgeTimestamp"));
    entity.setLastUpdateTimestamp(result.getLong("LastUpdateTimestamp"));

    entity.setProperties(result.getJson("Properties"));
    entity.setInternalProperties(result.getJson("InternalProperties"));
    entity.setGrantRecordsVersion((int) result.getLong("GrantRecordsVersion"));
    return entity;
  }

  public static Mutation upsert(String tableName, String realmId, PolarisBaseEntity entity) {
    return Mutation.newInsertOrUpdateBuilder(tableName)
        .set("RealmId")
        .to(realmId)
        .set("CatalogId")
        .to(entity.getCatalogId())
        .set("Id")
        .to(entity.getId())
        .set("ParentId")
        .to(entity.getParentId())
        .set("Name")
        .to(Value.string(entity.getName()))
        .set("EntityVersion")
        .to(entity.getEntityVersion())
        .set("TypeCode")
        .to(entity.getTypeCode())
        .set("SubTypeCode")
        .to(entity.getSubTypeCode())
        .set("CreateTimestamp")
        .to(entity.getCreateTimestamp())
        .set("DropTimestamp")
        .to(entity.getDropTimestamp())
        .set("PurgeTimestamp")
        .to(entity.getPurgeTimestamp())
        .set("ToPurgeTimestamp")
        .to(entity.getToPurgeTimestamp())
        .set("LastUpdateTimestamp")
        .to(entity.getLastUpdateTimestamp())
        .set("Properties")
        .to(SpannerUtil.jsonValue(entity.getPropertiesAsMap()))
        .set("InternalProperties")
        .to(SpannerUtil.jsonValue(entity.getInternalPropertiesAsMap()))
        .set("GrantRecordsVersion")
        .to(entity.getGrantRecordsVersion())
        .build();
  }

  public static Mutation upsert(String realmId, PolarisBaseEntity entity) {
    return upsert(TABLE_NAME, realmId, entity);
  }

  public static Mutation delete(String tableName, String realmId, PolarisBaseEntity entity) {
    return Mutation.delete(tableName, toKey(realmId, entity));
  }

  public static Mutation delete(String realmId, PolarisBaseEntity entity) {
    return delete(TABLE_NAME, realmId, entity);
  }

  public static Statement.Builder listEntities(PageToken pageToken) {
    String LimitClause =
        (pageToken != null && (pageToken instanceof HasPageSize))
            ? String.format("LIMIT %d", ((HasPageSize) pageToken).getPageSize())
            : "";
    String OffsetClause =
        (pageToken != null && (pageToken instanceof HasPageOffset))
            ? String.format("OFFSET %d", ((HasPageOffset) pageToken).getPageOffset())
            : "";
    String OrderClause =
        LimitClause.length() > 0 || OffsetClause.length() > 0 ? "ORDER BY RealmId,Id" : "";
    return Statement.newBuilder(
        String.join(
            " ",
            "SELECT",
            String.join(",", TABLE_COLUMNS),
            String.format("FROM `%s`", TABLE_NAME),
            "WHERE",
            String.join(
                " AND ",
                "RealmId = @realmId",
                "CatalogId = @catalogId",
                "ParentId = @parentId",
                "TypeCode = @typeCode"),
            LimitClause,
            OffsetClause,
            OrderClause));
  }
}
