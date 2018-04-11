/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.report.proto;

import java.util.List;

/**
 * Represents a list of information about the reports owned by a user in an HTTP response.
 */
public class ReportList {
  private final int offset;
  private final int limit;
  private final int total;
  private final List<ReportStatusInfo> reports;

  public ReportList(int offset, int limit, int total, List<ReportStatusInfo> reports) {
    this.offset = offset;
    this.limit = limit;
    this.total = total;
    this.reports = reports;
  }

  /**
   * @return the offset in the whole list of reports where the report information's
   *         start to be added to this {@link ReportList}
   */
  public int getOffset() {
    return offset;
  }

  /**
   * @return the max number of report information's contained in this {@link ReportList}
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @return the total number of reports owned by a user
   */
  public int getTotal() {
    return total;
  }

  /**
   * @return the list of report information's
   */
  public List<ReportStatusInfo> getReports() {
    return reports;
  }
}
