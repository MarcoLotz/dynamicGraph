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
package org.apache.giraph.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
  * VertexInputFormat that features <code>long</code> vertex ID's,
  * <code>double</code> vertex values and <code>float</code>
  * out-edge weights, and <code>double</code> message types,
  *  specified in JSON format.
  */

/**
 * JSONDynamic VertexReader that features <code>double</code> vertex
 * values and <code>float</code> out-edge weights. The
 * files should be in the following JSON format:
 * JSONArray(<vertex id>, <vertex value>,
 *   JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
 * Here is an example with vertex id 1, vertex value 4.3, and two edges.
 * First edge has a destination vertex 2, edge value 2.1.
 * Second edge has a destination vertex 3, edge value 0.7.
 * [1,4.3,[[2,2.1],[3,0.7]]]
 */

public class JSONDynamicReader {

	public static JSONArray preprocessLine(Text line) throws JSONException {
		return new JSONArray(line.toString());
	}

	public LongWritable getId(JSONArray jsonVertex) throws JSONException,
			IOException {
		return new LongWritable(jsonVertex.getLong(0));
	}

	public DoubleWritable getValue(JSONArray jsonVertex) throws JSONException,
			IOException {
		return new DoubleWritable(jsonVertex.getDouble(1));
	}

	public Iterable<Edge<LongWritable, FloatWritable>> getEdges(
			JSONArray jsonVertex) throws JSONException, IOException {
		JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
		List<Edge<LongWritable, FloatWritable>> edges = Lists
				.newArrayListWithCapacity(jsonEdgeArray.length());
		for (int i = 0; i < jsonEdgeArray.length(); ++i) {
			JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
			edges.add(EdgeFactory.create(new LongWritable(jsonEdge.getLong(0)),
					new FloatWritable((float) jsonEdge.getDouble(1))));
		}
		return edges;
	}

	public Vertex<LongWritable, DoubleWritable, FloatWritable> handleException(
			Text line, JSONArray jsonVertex, JSONException e) {
		throw new IllegalArgumentException("Couldn't get vertex from line "
				+ line, e);
	}
}