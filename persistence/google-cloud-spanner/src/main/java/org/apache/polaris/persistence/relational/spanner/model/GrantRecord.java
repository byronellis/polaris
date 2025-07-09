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
import static org.apache.polaris.persistence.relational.spanner.util.SpannerUtil.STRING_TYPE;
import static org.apache.polaris.persistence.relational.spanner.util.SpannerUtil.column;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.polaris.core.entity.PolarisGrantRecord;

public final class GrantRecord {

  public static final String TABLE_NAME = "GrantRecords";

  public static final String GRANTEE_INDEX_NAME = "GrantRecordsGranteeIndex";

  public static final List<Struct> TABLE_SCHEMA =
      ImmutableList.copyOf(
          new Struct[] {
            column("RealmId", STRING_TYPE, false, true),
            column("SecurableCatalogId", INT64_TYPE, false, true),
            column("SecurableId", INT64_TYPE, false, true),
            column("GranteeCatalogId", INT64_TYPE, false, true),
            column("GranteeId", INT64_TYPE, false, true),
            column("PrivilegeCode", INT64_TYPE, true, true)
          });

  public static final List<String> TABLE_COLUMNS =
      TABLE_SCHEMA.stream().map(column -> column.getString("Name")).toList();

  public static Key toKey(
      String realmId,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      long privilegeCode) {
    return Key.of(
        realmId, securableCatalogId, securableId, granteeCatalogId, granteeId, privilegeCode);
  }

  public static Key toKey(String realmId, PolarisGrantRecord grant) {
    return toKey(
        realmId,
        grant.getSecurableCatalogId(),
        grant.getSecurableId(),
        grant.getGranteeCatalogId(),
        grant.getGranteeId(),
        grant.getPrivilegeCode());
  }

  public static KeyRange entityGrantKey(String realmId, long securableCatalogId, long securableId) {
    return KeyRange.prefix(Key.of(realmId, securableCatalogId, securableId));
  }

  public static Mutation upsert(String tableName, String realmId, PolarisGrantRecord grantRecord) {
    return Mutation.newInsertOrUpdateBuilder(tableName)
        .set("RealmId")
        .to(realmId)
        .set("SecurableCatalogId")
        .to(grantRecord.getSecurableCatalogId())
        .set("SecurableId")
        .to(grantRecord.getSecurableId())
        .set("GranteeCatalogId")
        .to(grantRecord.getGranteeCatalogId())
        .set("GranteeId")
        .to(grantRecord.getGranteeId())
        .set("PrivilegeCode")
        .to(grantRecord.getPrivilegeCode())
        .build();
  }

  public static Mutation upsert(String realmId, PolarisGrantRecord grantRecord) {
    return upsert(TABLE_NAME, realmId, grantRecord);
  }

  public static Mutation delete(String tableName, String realmId, PolarisGrantRecord grantRecord) {
    return Mutation.delete(tableName, toKey(realmId, grantRecord));
  }

  public static Mutation delete(String realmId, PolarisGrantRecord grantRecord) {
    return delete(TABLE_NAME, realmId, grantRecord);
  }

  public static PolarisGrantRecord fromStruct(StructReader row) {
    if(row == null) {
      return null;
    }
    return new PolarisGrantRecord(
        row.getLong("SecurableCatalogId"),
        row.getLong("SecurableId"),
        row.getLong("GranteeCatalogId"),
        row.getLong("GranteeId"),
        (int) row.getLong("PrivilegeCode"));
  }
}
