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
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;

public final class PrincipalAuthenticationData {

  public static final String TABLE_NAME = "PrincipalAuthenticationData";

  public static final List<Struct> TABLE_SCHEMA =
      ImmutableList.copyOf(
          new Struct[] {
            column("RealmId", STRING_TYPE, false, true),
            column("PrincipalClientId", STRING_TYPE, false, true),
            column("PrincipalId", INT64_TYPE, false, true),
            column("MainSecretHash", STRING_TYPE, false, false),
            column("SecondarySecretHash", STRING_TYPE, false, false),
            column("SecretSalt", STRING_TYPE, false, false),
          });
  public static final Iterable<String> TABLE_COLUMNS =
      TABLE_SCHEMA.stream().map(column -> column.getString("Name")).toList();

  public static Key toKey(String realmId, String clientId, long principalId) {
    return Key.of(realmId, clientId, principalId);
  }

  public static KeyRange toKeyRange(String realmId, String clientId) {
    return KeyRange.prefix(Key.of(realmId, clientId));
  }

  public static PolarisPrincipalSecrets fromStruct(Struct row) {
    if (row == null) {
      return null;
    }
    return new PolarisPrincipalSecrets(
        row.getLong("PrincipalId"),
        row.getString("PrincipalClientId"),
        null,
        null,
        row.getString("SecretSalt"),
        row.getString("MainSecretHash"),
        row.getString("SecondarySecretHash"));
  }

  public static Mutation delete(
      String tableName, String realmIdentifier, String clientId, long principalId) {
    return Mutation.delete(tableName, toKey(realmIdentifier, clientId, principalId));
  }

  public static Mutation delete(String realmIdentifier, String clientId, long principalId) {
    return delete(TABLE_NAME, realmIdentifier, clientId, principalId);
  }

  public static Mutation upsert(
      String tableName, String realmIdentifier, PolarisPrincipalSecrets secrets) {
    return Mutation.newInsertOrUpdateBuilder(tableName)
        .set("RealmId")
        .to(realmIdentifier)
        .set("PrincipalClientId")
        .to(secrets.getPrincipalClientId())
        .set("PrincipalId")
        .to(secrets.getPrincipalId())
        .set("SecretSalt")
        .to(secrets.getSecretSalt())
        .set("MainSecretHash")
        .to(secrets.getMainSecretHash())
        .set("SecondarySecretHash")
        .to(secrets.getSecondarySecretHash())
        .build();
  }

  public static Mutation upsert(String realmIdentifier, PolarisPrincipalSecrets secrets) {
    return upsert(TABLE_NAME, realmIdentifier, secrets);
  }
}
