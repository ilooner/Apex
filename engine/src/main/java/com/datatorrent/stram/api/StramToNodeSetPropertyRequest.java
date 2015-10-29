/**
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
package com.datatorrent.stram.api;

import java.io.IOException;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Operator;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StatsListener.OperatorRequest;
import com.datatorrent.stram.api.RequestFactory.RequestDelegate;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.engine.InputNode;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.tuple.ModifyConfigurationTuple.PropertyChange;

/**
 * <p>StramToNodeChangeLoggersRequest class.</p>
 *
 * @since 1.0.4
 */
public class StramToNodeSetPropertyRequest extends StreamingContainerUmbilicalProtocol.StramToNodeRequest implements Serializable
{
  private String propertyKey;
  private String propertyValue;
  private String operatorName;

  public StramToNodeSetPropertyRequest()
  {
    requestType = RequestType.CUSTOM;
    //This is intentionally not set because we need to use a RequestDelegate to create our OperatorRequest.
    //The RequestDelegate is required because we nee
    cmd = null;
  }

  public String getPropertyKey()
  {
    return propertyKey;
  }

  public void setPropertyKey(String propertyKey)
  {
    this.propertyKey = propertyKey;
  }

  public String getPropertyValue()
  {
    return propertyValue;
  }

  public void setPropertyValue(String propertyValue)
  {
    this.propertyValue = propertyValue;
  }

  public String getOperatorName()
  {
    return operatorName;
  }

  public void setOperatorName(String operatorName)
  {
    this.operatorName = operatorName;
  }

  private static final long serialVersionUID = 201405271034L;

  private static final Logger logger = LoggerFactory.getLogger(StramToNodeSetPropertyRequest.class);

  private static class SetPropertyRequest implements StatsListener.OperatorRequest, Serializable
  {
    private static final long serialVersionUID = 201510290347L;
    private InputNode node;
    private String operatorName;
    private String propertyName;
    private String propertyValue;

    private SetPropertyRequest()
    {
    }

    private SetPropertyRequest(String operatorName,
                               String propertyName,
                               String propertyValue,
                               InputNode node)
    {
      this.operatorName = Preconditions.checkNotNull(operatorName);
      this.propertyName = Preconditions.checkNotNull(propertyName);
      this.propertyValue = Preconditions.checkNotNull(propertyValue);
      this.node = Preconditions.checkNotNull(node);
    }

    @Override
    public StatsListener.OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
    {
      node.addConfigurationChange(operatorName, new PropertyChange(propertyName, propertyValue));
      return null;
    }

    @Override
    public String toString()
    {
      return "Set Property";
    }
  }

  public static class SetPropertyRequestDelegate implements RequestDelegate
  {
    @Override
    public OperatorRequest getRequestExecutor(Node<?> node, StramToNodeRequest snr)
    {
      if (!(node instanceof InputNode)) {
        throw new IllegalArgumentException(StramToNodeSetPropertyRequest.class.getName() +
                                           " can only be applied on " + InputNode.class.getName());
      }

      if(!(snr instanceof StramToNodeSetPropertyRequest)) {
        throw new IllegalArgumentException("This request delegate can only be applied to " + StramToNodeSetPropertyRequest.class.getName());
      }

      StramToNodeSetPropertyRequest setPropReq = (StramToNodeSetPropertyRequest) snr;
      return new SetPropertyRequest(setPropReq.operatorName, setPropReq.propertyKey, setPropReq.propertyValue, (InputNode) node);
    }
  }
}
