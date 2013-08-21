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
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.giraph.aggregators.BooleanOverwriteAggregator;
import org.json.JSONArray;
import org.json.JSONException;

import uk.co.qmul.giraph.dynamicgraph.PathAggregator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * Demonstrates the basic Pregel applied to Dynamic Graphs. This is a just a
 * vertex computation prototype class to check how the system may recognise a
 * modification in a FS file.
 * 
 * @author MarcoLotz
 */

@Algorithm(name = "Dynamic Graph Computation", description = "Makes computation on dynamic graphs")
public class DynamicGraphComputation
		extends
		BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	/** Path aggregator name */
	private static String PATH_AGG = "PathAgg";

	/** FS Update aggregator name */
	private static String FS_AGG = "FileSystemAgg";

	/** Injection Complete Status aggregator */
	private static String INJ_RDY_AGG = "InjectionReadyAgg";

	/**
	 * Gets the maximum number of Super steps to be computed
	 */
	public final int MAX_SUPERSTEPS = 7;

	public static boolean WaitedRemoval = false;

	/**
	 * Injector vertex information
	 */
	public static final LongWritable INJECTOR_VERTEX_ID = new LongWritable(-1);
	public static final DoubleWritable INJECTOR_VERTEX_VALUE = new DoubleWritable(
			-100);

	/** Class logger */
	private static final Logger LOG = Logger
			.getLogger(DynamicGraphComputation.class);

	/**
	 * Checks if the FS suffered a modification
	 */
	BooleanWritable fsModificationStatus = new BooleanWritable();

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
		if (getSuperstep() < MAX_SUPERSTEPS) {

			fsModificationStatus = (BooleanWritable) getAggregatedValue(FS_AGG);

			// Check if it is the injector vertex
			if (vertex.getId() == INJECTOR_VERTEX_ID) {
				InjectorMonitor();
			} else {
				// If a modification in the fileSystem happened, remove all
				// vertex that are not the injector.
				if ((vertex.getId() != INJECTOR_VERTEX_ID)
						&& (fsModificationStatus.get())) {
					// removes all the current vertices
					LOG.info("Vertex :" + vertex.getId().get()
							+ " being removed in superstep " + getSuperstep());
					removeVertexRequest(vertex.getId());
				}
				// Vertices do standard computation here
				else {
					BFSMessages(vertex);
					vertex.voteToHalt();
				}
			}
		} else {
			vertex.voteToHalt();
		} // Always converge in the last superstep, even the Injector
	}

	public void InjectorMonitor() {
		// Checks for file system update
		if (true == fsModificationStatus.get()) {
			LOG.info("[PROMETHEUS] The master vertex as communicated a modification in the file system");
			LOG.info("[PROMETHEUS] In the superstep " + getSuperstep());

			// Only Injects in the second call of this method, the first call is
			// when the vertices will be getting removed.
			// One can also do this by using the getNumberVertex and wait until
			// it is only the injector.
			if (true == WaitedRemoval) {
				try {
					UpdateFileSystem();
				} catch (IOException e) {
					LOG.info("[PROMETHEUS] Problem Updating File System.");
				}
				WaitedRemoval = false;
			} else {
				WaitedRemoval = true;
			}
		}
	}

	public void UpdateFileSystem() throws IOException {
		FileSystem fs;
		Configuration config = new Configuration();
		FileStatus fileStatus;

		LOG.info("[PROMETHEUS] UpdatingFileSystem() in SuperStep "
				+ getSuperstep());
		Text inputString = getAggregatedValue(PATH_AGG); // Gets the paths in
															// the HDFS
		Path inputPath = new Path(inputString.toString());

		LOG.info("[PROMETHEUS] Injector: the path is" + inputPath.getParent()
				+ inputPath.getName());
		LOG.info("[PROMETHEUS] Checking file in HDFS");

		fs = FileSystem.get(config);

		// Remove next block, just checking if file system is ok.
		fileStatus = fs.getFileStatus(inputPath);// Just to check if the file
													// was correctly accessed.
		LOG.info("[PROMETHEUS] Correctly got file status");
		LOG.info("[PROMETHEUS] file status: Length : " + fileStatus.getLen());

		// Do the injection.
		Inject(fs, inputPath);

		InformMasterCompute();
	}

	/**
	 * Injector, injects new vertex from desired input. Note: One may modify it
	 * in the future with vertexMutations To allow vertex mutation without
	 * removing the vertices.
	 * 
	 * @param file
	 *            system that is going to be used
	 * @param path
	 *            that is going to be read
	 * @throws IOException
	 */
	public void Inject(FileSystem fs, Path path) throws IOException {

		// Maybe one should use the standard JSONReader.
		LOG.info("[PROMETHEUS] Creating JSON variables");
		String line;
		Text inputLine;
		JSONArray preProcessedLine;

		LOG.info("[PROMETHEUS] Creating JSON Reader:");
		JSONDynamicReader DynamicReader = new JSONDynamicReader();
		LOG.info("[PROMETHEUS] JSON reader created with success!");

		LOG.info("[PROMETHEUS] Creating Vertex Injection variables");
		LongWritable vertexId;
		DoubleWritable vertexValue;
		Iterable<Edge<LongWritable, FloatWritable>> vertexEdges;

		LOG.info("[PROMETHEUS] Creating a Buffered reader");
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(path)));
		try {
			line = br.readLine();
			while (line != null) {
				inputLine = new Text(line);
				preProcessedLine = DynamicReader.preprocessLine(inputLine);
				vertexId = DynamicReader.getId(preProcessedLine);
				vertexValue = DynamicReader.getValue(preProcessedLine);
				vertexEdges = DynamicReader.getEdges(preProcessedLine);

				// Try to use the method that has three parameters.
				addVertexRequest(vertexId, vertexValue);
				LOG.info("[PROMETHEUS] Adding vertex [id,value]:" + vertexId
						+ "," + vertexValue);

				// Assuming it add edges calls are made after the vertex add
				// calls
				for (Edge<LongWritable, FloatWritable> edge : vertexEdges) {
					addEdgeRequest(vertexId, edge);
				}
				LOG.info("[PROMETHEUS] All edges added to vertex" + vertexId);

				line = br.readLine();
			}
		} catch (JSONException e) {
			LOG.info("[PROMETHEUS] Problem in JSON reader");
			e.printStackTrace();
		} finally {
			br.close();
		}
		// //Do vertex mutations
		// //Use vertex resolver*/
	}

	@Override
	public void preSuperstep() {
		super.preSuperstep();
		if (getSuperstep() == 0) {
			try {
				addVertexRequest(INJECTOR_VERTEX_ID, INJECTOR_VERTEX_VALUE);
			} catch (IOException e) {
				LOG.info("[PROMETHEUS] Could not add injector vertex!");
				e.printStackTrace();
			}
			LOG.info("[PROMETHEUS] Injector vertex created!");
		}
	}

	public void InformMasterCompute() {
		// Tells the Master Compute that the database update is done.
		aggregate(INJ_RDY_AGG, new BooleanWritable(true));
	}

	/**
	 * Master Compute associated with {@link DynamicGraphComputation}. It is the
	 * first thing to run in each super step. It has a watcher to see if there
	 * is any modification in input
	 */

	public static class InjectorMasterCompute extends DefaultMasterCompute {

		private String inputPath = "/user/hduser/dynamic/GoogleJSON.txt";

		BooleanWritable isRdyForMutations;

		/** Class logger */
		private final Logger LOG = Logger
				.getLogger(InjectorMasterCompute.class);

		FileWatcher fileWatcher;

		// private final long SLEEPSECONDS = 1;

		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {

			// Register Aggregators
			registerPersistentAggregator(PATH_AGG, PathAggregator.class);
			registerPersistentAggregator(FS_AGG,
					BooleanOverwriteAggregator.class);
			registerPersistentAggregator(INJ_RDY_AGG,
					BooleanOverwriteAggregator.class);

			// set Aggregators initial values
			setAggregatedValue(PATH_AGG, new Text(inputPath));
			setAggregatedValue(FS_AGG, new BooleanWritable(false));
			setAggregatedValue(INJ_RDY_AGG, new BooleanWritable(true)); // is
																		// ready
																		// for
																		// new
																		// mutations.

			// Start the File Watcher
			fileWatcher = new FileWatcher(inputPath);
			LOG.info("[PROMETHEUS] Master Compute Initialized.");
		}

		@Override
		public void compute() {
			LOG.info("[PROMETHEUS] MasterCompute Compute() method:");
			LOG.info("[PROMETHEUS] Superstep number: " + getSuperstep());

			isRdyForMutations = (BooleanWritable) getAggregatedValue(INJ_RDY_AGG);

			if (true == isRdyForMutations.get()) {
				LOG.info("[PROMETHEUS] The structure is ready for mutations in superstep: "
						+ getSuperstep());
				LOG.info("[PROMETHEUS] Checking if the file was modified");

				// fileWatcher.checkFileModification();
				setAggregatedValue(FS_AGG, new BooleanWritable(true)); // Just
																		// testing

				// try {
				// LOG.info("[PROMETHEUS] Going to sleep for " +
				// TimeUnit.SECONDS.toMillis(SLEEPSECONDS));
				// Thread.sleep(TimeUnit.SECONDS.toMillis(SLEEPSECONDS));
				// //Holds
				// each superstep for SLEEPSECONDS
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }

				// If there was a modification, alert injector.
				if (fileWatcher.getFileModifed() == true) {
					LOG.info("[PROMETHEUS] The fileWatcher indicates that the file was modified.");
					setAggregatedValue(FS_AGG, new BooleanWritable(true));
				}
			}
		}

		/**
		 * Watches the HDFS for any modification in the input files
		 * 
		 * @author hduser
		 * 
		 */
		public static class FileWatcher {
			private long modificationTime;

			/**
			 * The path that is going to be analysed in every Superstep. One may
			 * modify it later to change it during the computation Or add more
			 * than one Path (since multiple paths are already possible in
			 * Giraph)
			 */
			private Path PATH;

			/**
			 * This flag will become true if the file was modified.
			 */
			private boolean fileModified = false;

			/** Class logger */
			private final Logger LOG = Logger.getLogger(FileWatcher.class);

			/**
			 * The configuration file that will be used for HDFS
			 */
			Configuration config = new Configuration();

			/**
			 * Used in order to access the HDFS
			 */
			private FileSystem fs;

			/**
			 * Is the file status of the target file. The TimeStamp is in it.
			 */
			FileStatus fileStatus;

			FileWatcher(String inputPath) {
				PATH = new Path(inputPath);

				// Initialises the file system
				try {
					fs = FileSystem.get(config);
				} catch (IOException e) {
					e.printStackTrace();
				}

				// Gets the designed file status
				try {
					fileStatus = fs.getFileStatus(PATH);
				} catch (IOException e) {
					e.printStackTrace();
				}

				modificationTime = fileStatus.getModificationTime();
				LOG.info("[PROMETHEUS] The modification time (UTC) is: "
						+ modificationTime);
				this.fileModified = false;
			}

			/**
			 * Check if the watched file was modified in the beginning of every
			 * super step
			 */
			public void checkFileModification() {

				// Check if one has to update the file status every single time.
				try {
					fileStatus = fs.getFileStatus(PATH);
				} catch (IOException e) {
					LOG.info("[PROMETHEUS ERROR] Error getting File status.");
				}

				long modtime = fileStatus.getModificationTime();
				if (modtime != modificationTime) {
					fileModified = true;
					this.modificationTime = modtime;
					LOG.info("[PROMETHEUS] The file was modified during the superstep!");
					LOG.info("[PROMETHEUS] The new modification time is: "
							+ this.modificationTime);
				} else {
					fileModified = false;
					LOG.info("[PROMETHEUS] The file was NOT modified in the current superstep.");
				}
			}

			public boolean getFileModifed() {
				return this.fileModified;
			}
		}
	}

	public static class JSONDynamicReader {

		public JSONDynamicReader() {
		}

		public JSONArray preprocessLine(Text line) throws JSONException {
			return new JSONArray(line.toString());
		}

		public LongWritable getId(JSONArray jsonVertex) throws JSONException,
				IOException {
			return new LongWritable(jsonVertex.getLong(0));
		}

		public DoubleWritable getValue(JSONArray jsonVertex)
				throws JSONException, IOException {
			return new DoubleWritable(jsonVertex.getDouble(1));
		}

		public Iterable<Edge<LongWritable, FloatWritable>> getEdges(
				JSONArray jsonVertex) throws JSONException, IOException {
			JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
			List<Edge<LongWritable, FloatWritable>> edges = Lists
					.newArrayListWithCapacity(jsonEdgeArray.length());
			for (int i = 0; i < jsonEdgeArray.length(); ++i) {
				JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
				edges.add(EdgeFactory.create(
						new LongWritable(jsonEdge.getLong(0)),
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
}