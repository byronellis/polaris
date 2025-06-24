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

package org.apache.polaris.core.persistence.pagination;

import java.util.List;

public class OffsetLimitPageToken extends PageToken implements HasPageSize, HasPageOffset {

  public static final String PREFIX = "offset-limit";

  private final long offset;
  private final int pageSize;

  public OffsetLimitPageToken(long offset, int pageSize) {
    this.offset = offset;
    this.pageSize = pageSize;
  }

  @Override
  public long getPageOffset() {
    return offset;
  }

  @Override
  public int getPageSize() {
    return pageSize;
  }

  @Override
  public String toTokenString() {
    return String.format("%s/%d/%d", PREFIX, pageSize, offset);
  }

  @Override
  protected PageToken updated(List<?> newData) {
    if (newData.size() < pageSize) {
      return new DonePageToken();
    } else {
      return new OffsetLimitPageToken(offset + newData.size(), pageSize);
    }
  }
}
