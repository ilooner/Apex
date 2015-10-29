/*
 * Copyright 2015 Apache Software Foundation.
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

package com.datatorrent.stram.tuple;

import com.datatorrent.api.Operator;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import java.util.Collection;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Set;

public class ModifyConfigurationTuple extends Tuple
{
  private ConfigurationChangeBatch configurationChangeBatch;
  private Set<String> seenOperators;

  public ModifyConfigurationTuple(ConfigurationChangeBatch configurationChangeBatch, long windowId)
  {
    super(MessageType.MODIFY_CONFIGURATION, windowId);

    Preconditions.checkNotNull(configurationChangeBatch);
    Preconditions.checkArgument(!configurationChangeBatch.isEmpty(), "The given " + ConfigurationChangeBatch.class.getSimpleName() + " cannot be empty.");

    this.configurationChangeBatch = configurationChangeBatch;
    seenOperators = Sets.newHashSet();
  }

  public Collection<ConfigurationChange> remove(String operatorName)
  {
    return configurationChangeBatch.remove(operatorName);
  }

  public Collection<ConfigurationChange> get(String operatorName)
  {
    return configurationChangeBatch.get(operatorName);
  }

  public boolean isEmpty()
  {
    return configurationChangeBatch.isEmpty();
  }

  public void addSeenOperator(String operatorName)
  {
    seenOperators.add(operatorName);
  }

  public boolean sawOperator(String operatorName)
  {
    return seenOperators.contains(operatorName);
  }

  public static class ConfigurationChangeBatch
  {
    private Map<String, Collection<ConfigurationChange>> configurationChangeMap;

    public ConfigurationChangeBatch()
    {
    }

    public Collection<ConfigurationChange> remove(String operatorName)
    {
      Preconditions.checkNotNull(operatorName);
      return configurationChangeMap.remove(operatorName);
    }

    public void add(String operatorName, ConfigurationChange configurationChange)
    {
      Preconditions.checkNotNull(operatorName);
      Preconditions.checkNotNull(configurationChange);

      Collection<ConfigurationChange> configurationChanges = configurationChangeMap.get(operatorName);

      if (configurationChanges == null) {
        configurationChanges = Lists.newArrayList();
        configurationChangeMap.put(operatorName, configurationChanges);
      }

      configurationChanges.add(configurationChange);
    }

    public Collection<ConfigurationChange> get(String operatorName)
    {
      Preconditions.checkNotNull(operatorName);
      return configurationChangeMap.get(operatorName);
    }

    public boolean isEmpty()
    {
      return configurationChangeMap.isEmpty();
    }
  }

  public static interface ConfigurationChange
  {
  }

  public static class PropertyChange implements ConfigurationChange
  {
    private String name;
    private String value;

    private PropertyChange()
    {
      //For Kryo
    }

    public PropertyChange(String propertyName, String propertyValue)
    {
      this.name = Preconditions.checkNotNull(propertyName);
      this.value = Preconditions.checkNotNull(propertyValue);
    }

    public String getName()
    {
      return name;
    }

    public String getValue()
    {
      return value;
    }

    public static void applyPropertyChanges(Collection<ConfigurationChange> configurationChanges, Operator operator)
    {
      if(configurationChanges == null || configurationChanges.isEmpty()) {
        return;
      }

      Map<String, String> properties = Maps.newHashMap();

      for (ConfigurationChange configurationChange : configurationChanges) {
        if (configurationChange instanceof PropertyChange) {
          PropertyChange propertyChange = (PropertyChange) configurationChange;
          properties.put(propertyChange.getName(), propertyChange.getValue());
        }
      }

      if(properties.isEmpty()) {
        return;
      }

      LogicalPlanConfiguration.setOperatorProperties(operator, properties);
    }
  }
}
