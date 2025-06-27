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
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;

public class PolicyMapping {

  public static final String TABLE_NAME = "PolicyMapping";

  public static final String POLICY_INDEX = "PolicyMappingPolicyIndex";

  public static final List<Struct> TABLE_SCHEMA =
      ImmutableList.copyOf(
          new Struct[] {
            column("RealmId", STRING_TYPE, false, true),
            column("TargetCatalogId", INT64_TYPE, false, true),
            column("TargetId", INT64_TYPE, false, true),
            column("PolicyTypeCode", INT64_TYPE, false, true),
            column("PolicyCatalogId", INT64_TYPE, false, true),
            column("PolicyId", INT64_TYPE, false, true),
            column("Parameters", JSON_TYPE, true, false)
          });

  public static final List<String> TABLE_COLUMNS =
      TABLE_SCHEMA.stream().map(column -> column.getString("Name")).toList();

  public static Key toKey(
      String realmId,
      long targetCatalogId,
      long targetId,
      int policyTypeCode,
      long policyCatalogId,
      long policyId) {
    return Key.of(realmId, targetCatalogId, targetId, policyTypeCode, policyCatalogId, policyId);
  }

  public static KeyRange toKeyRange(
      String realmId, long targetCatalogId, long targetId, int policyTypeCode) {
    return KeyRange.prefix(Key.of(realmId, targetCatalogId, targetId, policyTypeCode));
  }

  public static KeyRange toKeyRange(String realmId, long targetCatalogId, long targetId) {
    return KeyRange.prefix(Key.of(realmId, targetCatalogId, targetId));
  }

  public static Key toKey(String realmId, PolarisPolicyMappingRecord mapping) {
    return toKey(
        realmId,
        mapping.getTargetCatalogId(),
        mapping.getTargetId(),
        mapping.getPolicyTypeCode(),
        mapping.getPolicyCatalogId(),
        mapping.getPolicyId());
  }

  public static PolarisPolicyMappingRecord fromStruct(StructReader struct) {
    return new PolarisPolicyMappingRecord(
        struct.getLong("TargetCatalogId"),
        struct.getLong("TargetId"),
        struct.getLong("PolicyCatalogId"),
        struct.getLong("PolicyId"),
        (int) struct.getLong("PolicyTypeCode"),
        struct.getJson("Parameters"));
  }

  public static Mutation upsert(
      String tableName, String realmId, PolarisPolicyMappingRecord mapping) {
    return Mutation.newInsertOrUpdateBuilder(tableName)
        .set("RealmId")
        .to(realmId)
        .set("TargetCatalogId")
        .to(mapping.getTargetCatalogId())
        .set("TargetId")
        .to(mapping.getTargetId())
        .set("PolicyTypeCode")
        .to(mapping.getPolicyTypeCode())
        .set("PolicyCatalogId")
        .to(mapping.getPolicyCatalogId())
        .set("PolicyId")
        .to(mapping.getPolicyId())
        .set("Parameters")
        .to(mapping.getParameters())
        .build();
  }

  public static Mutation upsert(String realmId, PolarisPolicyMappingRecord mapping) {
    return upsert(PolicyMapping.TABLE_NAME, realmId, mapping);
  }

  public static Mutation delete(
      String tableName, String realmId, PolarisPolicyMappingRecord mapping) {
    return Mutation.delete(tableName, toKey(realmId, mapping));
  }

  public static Mutation delete(String realmId, PolarisPolicyMappingRecord mapping) {
    return delete(PolicyMapping.TABLE_NAME, realmId, mapping);
  }
}
