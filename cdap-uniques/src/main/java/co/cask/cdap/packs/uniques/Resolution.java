/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.packs.uniques;

/**
 * Defines the resolution.
 */
public enum Resolution {
  FIVE_MIN(Constants.LOWEST_RESOLUTION_SECONDS),
  TEN_MIN(Constants.LOWEST_RESOLUTION_SECONDS * 2),
  FIFTEEN_MIN(Constants.LOWEST_RESOLUTION_SECONDS * 3),
  HALF_HOUR(Constants.LOWEST_RESOLUTION_SECONDS * 6),
  ONE_HOUR(Constants.LOWEST_RESOLUTION_SECONDS * 12),
  ONE_DAY(Constants.LOWEST_RESOLUTION_SECONDS * 12 * 24),
  FIFTEEN_DAYS(Constants.LOWEST_RESOLUTION_SECONDS * 12 * 24 * 15),
  ONE_MONTH(Constants.LOWEST_RESOLUTION_SECONDS * 12 * 24 * 30);

  private final long resolution;

  Resolution(long resolution) {
    this.resolution = resolution;
  }

  public long getResolution() {
    return resolution;
  }
}
