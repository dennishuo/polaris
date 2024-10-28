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
package org.apache.polaris.service.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class DefaultConfigurationStore implements PolarisConfigurationStore {

  private final Map<String, Object> properties;

  // FIXME the whole PolarisConfigurationStore + PolarisConfiguration needs to be refactored
  // to become a proper Quarkus configuration object
  public DefaultConfigurationStore(
      ObjectMapper objectMapper,
      @ConfigProperty(name = "polaris.config.feature-configurations")
          Map<String, String> properties) {
    Map<String, Object> m = new HashMap<>();
    for (String configName : properties.keySet()) {
      String json = properties.get(configName);
      try {
        JsonNode node = objectMapper.readTree(json);
        m.put(configName, getConfigValue(node));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(
            "Invalid JSON value for feature configuration: " + configName, e);
      }
    }
    this.properties = Map.copyOf(m);
  }

  private static Object getConfigValue(JsonNode node) {
    return switch (node.getNodeType()) {
      case BOOLEAN -> node.asBoolean();
      case STRING -> node.asText();
      case NUMBER ->
          switch (node.numberType()) {
            case INT, LONG -> node.asLong();
            case FLOAT, DOUBLE -> node.asDouble();
            default ->
                throw new IllegalArgumentException("Unsupported number type: " + node.numberType());
          };
      case ARRAY -> {
        List<Object> list = new ArrayList<>();
        node.elements().forEachRemaining(n -> list.add(getConfigValue(n)));
        yield List.copyOf(list);
      }
      default ->
          throw new IllegalArgumentException(
              "Unsupported feature configuration JSON type: " + node.getNodeType());
    };
  }

  @Override
  public <T> @Nullable T getConfiguration(PolarisCallContext ctx, String configName) {
    @SuppressWarnings("unchecked")
    T o = (T) properties.get(configName);
    return o;
  }
}
