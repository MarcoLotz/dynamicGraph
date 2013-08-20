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

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * Demonstrates the basic Pregel applied to Dynamic Graphs.
 * This is a just a vertex computation prototype class to check how the system may recognise
 * a modification in a FS file.
 * 
 * @author MarcoLotz
 */

@Algorithm(name = "Dynamic Graph Computation", description = "Makes computation on dynamic graphs")
public class DynamicGraphComputation
		extends
		BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	
	/**
	 * Gets the maximum number of Super steps to be computed
	 */
	public final int MAX_SUPERSTEPS = 6;

	/** Class logger */
	private static final Logger LOG = Logger
			.getLogger(DynamicGraphComputation.class);
	
	/**
	 *  Gets the job configuration.
	 */
	ImmutableClassesGiraphConfiguration<?, ?, ?> Gconf;

	/**
	 * Send messages to all the connected vertices. The content of the messages
	 * is not important, since just the event of receiving a message removes the
	 * vertex from the inactive status.
	 * 
	 * @param vertex
	 */
	public void BFSMessages(
			Vertex<LongWritable, DoubleWritable, FloatWritable> vertex) {
		for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
			sendMessage(edge.getTargetVertexId(), new DoubleWritable(1d));
		}
	}

	@Override
	public void compute(
			Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
			Iterable<DoubleWritable> messages) throws IOException {
		if (getSuperstep() == 0)
		{
//			String tmp;
//			LOG.info("[PROMETHEUS] getting configuration with Gconf");
//			Gconf = getConf();
//			LOG.info("[PROMETHEUS] Success!");
//			
//			LOG.info("[PROMETHEUS] Getting map.input.file:");
//			tmp = Gconf.get("map.input.file");
//			LOG.info("[PROMETHEUS] " + tmp);
		}
	
		if ( getSuperstep() < MAX_SUPERSTEPS)
		{
			LOG.info("[PROMETHEUS] Vertex: " + vertex.getId().get() + " Superstep: " + getSuperstep()
					+ " has sent messages");
			BFSMessages(vertex);
		}
		vertex.voteToHalt();
	}
	
	
	
	
	
	
	
	

	/**
	 * Worker context used with {@link DynamicGraphComputation}.
	 */
	public static class SimpleDynamicWorkerContext extends WorkerContext {
		
		private static final Logger LOG = Logger
				.getLogger(SimpleDynamicWorkerContext.class);

		@Override
		public void preApplication() throws InstantiationException,
				IllegalAccessException {
		}

		@Override
		public void postApplication() {
		}

		@Override
		public void preSuperstep() {
			LOG.info("[PROMETHEUS] Getting InputSplit information");
			InputSplit inputSplit = getContext().getInputSplit(); // I guess it works :)
			try {
				LOG.info("[PROMETHEUS] Size of input split: " + inputSplit.getLength());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			LOG.info("[PROMETHEUS] locations of input split:");
			String[] locations;
			try {
				locations = inputSplit.getLocations();
				for ( int i = 0 ; i < locations.length; i ++ )
				{
					LOG.info("[PROMETHEUS] " + locations[i]);
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void postSuperstep() {
		}
	}
}