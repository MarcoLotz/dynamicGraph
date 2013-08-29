/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.qmul.giraph.dynamicgraph;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.Text;

/**
 * Aggregator used for Text.
 * @author: Marco Aurelio Lotz
 */
public class PathAggregator extends BasicAggregator<Text> {
	
/* 
 * @see org.apache.giraph.aggregators.Aggregator#aggregate(org.apache.hadoop.io.Writable)
 * Aggregate just updates the path string, should be only called by the MasterCompute method.
 */
	
@Override
  public void aggregate(Text value) {
	  setAggregatedValue(value);
  }

  @Override
  public Text createInitialValue() {
    return new Text();
  }
}