/**
 * Copyright 2012 Twitter, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.udbac.ua.uap;

import java.util.Map;

/**
 * Device parsed data class
 *
 * @author Steve Jiang (@sjiang) <gh at iamsteve com>
 */
public class Device {
  public final String family,brand, model;

  public Device(String family, String brand, String model) {
    this.family = family;
    this.brand = brand;
    this.model = model;
  }

  public static Device fromMap(Map<String, String> m) {
    return new Device(m.get("family"), m.get("brand"), m.get("model"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Device device = (Device) o;

    if (family != null ? !family.equals(device.family) : device.family != null) return false;
    if (brand != null ? !brand.equals(device.brand) : device.brand != null) return false;
    return model != null ? model.equals(device.model) : device.model == null;
  }

  @Override
  public int hashCode() {
    int result = family != null ? family.hashCode() : 0;
    result = 31 * result + (brand != null ? brand.hashCode() : 0);
    result = 31 * result + (model != null ? model.hashCode() : 0);
    return result;
  }

    @Override
  public String toString() {
    return String.format("{\"family\": %s}",
                         family == null ? Constants.EMPTY_STRING : '"' + family + '"');
  }
}