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
 * Demonstrates a basic structure in order to allow Pregel to make computations
 * over Dynamic Graphs.
 * 
 * @author Marco Aurelio Lotz
 */

@Algorithm(name = "Dynamic Graph Computation", description = "Makes computation on dynamic graphs")
public class DynamicGraphComputation
		extends
		BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	/**
	 * Path Aggregator name. This Aggregator is used to communicate the
	 * injection Path from the Master to the Injector vertex.
	 */
	private static String PATH_AGG = "PathAgg";

	/**
	 * Used to indicate the master that the update is finished
	 */
	private static final BooleanWritable UpdateFinish = new BooleanWritable(false);

	/**
	 * Serves as a flag of communication between vertices and the master.
	 * Its value controls the update behaviour.
	 */
	private static String U_UP_AGG = "UnderUpgradeAgg";

	/**
	 * Maximum number of Supersteps to be computed before halting.
	 */
	public final int MAX_SUPERSTEPS = 33;

	/**
	 * Flag used by the injector to wait a superstep after removal only Before
	 * starting the injection
	 */
	public static boolean WaitedRemoval = false;

	/** Class logger */
	private static final Logger LOG = Logger
			.getLogger(DynamicGraphComputation.class);

	@Override
	public void compute(
			Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
			Iterable<DoubleWritable> messages) throws IOException {
		if (getSuperstep() < MAX_SUPERSTEPS) {
			// ------------------------------------------------
			// Remove code and insert any computation code here
			sendMessage(vertex.getId(), new DoubleWritable(1d));
			vertex.voteToHalt();
			// ------------------------------------------------
		} else {
			vertex.voteToHalt();
		}
	}

	@Override
	public void Inject() {
		// checks for updateFlag, that is changed in the presuperstep
		if (true == isUnderUpdate()) {
			LOG.info("Superstep: "
					+ getSuperstep()
					+ " - The master has communicated a modification in the file system");

			// Only Injects in the second call of this method.
			// In the first call the other vertices will be being removed
			// One can also use the number of vertices to trigger this event.
			if (true == WaitedRemoval) {
				LOG.info("Injector updateFile System on superstep: "
						+ getSuperstep());
				try {
					UpdateFileSystem();
					// Tell the file system that the update is over.
					InformMasterCompute();
				} catch (IOException e) {
					LOG.info("Problem Updating vertex database.");
				}
				WaitedRemoval = false;
			} else {
				WaitedRemoval = true;
			}
		}
	}

	/**
	 * Gets the path from the aggregator and configures the file system in order
	 * to retrieve the file data
	 * 
	 * @throws IOException
	 */
	public void UpdateFileSystem() throws IOException {
		FileSystem fs;
		Configuration config = new Configuration();
		FileStatus fileStatus;

		// Gets the HDFS paths
		Text inputString = getAggregatedValue(PATH_AGG);
		Path inputPath = new Path(inputString.toString());

		LOG.info("Injector: the path is" + inputPath.getParent()
				+ inputPath.getName());

		fs = FileSystem.get(config);

		LOG.info("Checking file in File System");
		fileStatus = fs.getFileStatus(inputPath);

		if (null != fileStatus) {
			// Do the injection.
			BeginInjection(fs, inputPath);
		} else {
			LOG.info("Problem looking for the file in File System");
		}
	}

	/**
	 * injects new vertex from desired input.
	 * 
	 * @param file
	 *            system that is going to be used
	 * @param path
	 *            that is going to be read
	 * @throws IOException
	 */
	public void BeginInjection(FileSystem fs, Path path) throws IOException {

		// Create JSON variables
		String line;
		Text inputLine;
		JSONArray preProcessedLine;
		JSONDynamicReader DynamicReader = new JSONDynamicReader();

		// Create injection variables
		LongWritable vertexId;
		DoubleWritable vertexValue;
		Iterable<Edge<LongWritable, FloatWritable>> vertexEdges;

		// Creates a buffered reader to read the input file
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(path)));
		try {
			line = br.readLine();
			while (line != null) {
				// Processes the line information
				inputLine = new Text(line);
				preProcessedLine = DynamicReader.preprocessLine(inputLine);
				vertexId = DynamicReader.getId(preProcessedLine);
				vertexValue = DynamicReader.getValue(preProcessedLine);
				vertexEdges = DynamicReader.getEdges(preProcessedLine);

				// Requests vertex add
				addVertexRequest(vertexId, vertexValue);
				LOG.info("Superstep: " + getSuperstep()
						+ " Adding vertex [id,value]:" + vertexId + ","
						+ vertexValue);

				// Add edges to that vertex
				for (Edge<LongWritable, FloatWritable> edge : vertexEdges) {
					addEdgeRequest(vertexId, edge);
				}
				line = br.readLine();
			}
		} catch (JSONException e) {
			LOG.info("Problem in JSON reader");
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	@Override
	public void preSuperstep() {
		// Only creates the injector in the first superstep
		if (0 == getSuperstep()) {
			// One should make sure that only one injector is created
			// Or solve it through the vertex resolver.
			// This may cause race conditions in the execution of this code.
			try {
				addVertexRequest(INJECTOR_VERTEX_ID, INJECTOR_VERTEX_VALUE);
				LOG.info("Injector vertex created with success in superstep:"
						+ getSuperstep());
			} catch (IOException e) {
				LOG.info("Problem creating the injector vertex.");
				e.printStackTrace();
			}
		}
		// Checks if Master indicated a modification in the FileSystem
		LOG.info("[PROMETHEUS] Checking update status" + getSuperstep());
		setUnderUpdate(((BooleanWritable) getAggregatedValue(U_UP_AGG)));
	}

	/**
	 * Tells master compute that the vertex data-base is up-to-date
	 */
	public void InformMasterCompute() {
		LOG.info("Setting Under Update status to false");
		aggregate(U_UP_AGG, UpdateFinish);
	}

	/**
	 * Master Compute associated with {@link DynamicGraphComputation}. It is the
	 * first thing to run in each super step. It has an observer to track if
	 * there is any modification in input
	 */

	public static class InjectorMasterCompute extends DefaultMasterCompute {

		/**
		 * Insert the paths to be watched here. One can easily modify the
		 * Aggregator to use an array of paths.
		 */
		private String inputPath = "/user/hduser/dynamic/ReducedJournal.txt";

		/**
		 * Used by the master compute to avoid accessing the file system while
		 * the workers are still processing a previous mutation
		 */
		boolean isUnderUpdate = false;

		/**
		 * Number of supersteps that it waited before
		 * Before looking for another update.
		 */
		private int numberOfWaitedSupersteps = 0;

		/** Class logger */
		private final Logger LOG = Logger
				.getLogger(InjectorMasterCompute.class);

		/**
		 * Object that will track the given paths.
		 */
		FileObserver fileObserver;

		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {

			// Register Aggregators
			registerPersistentAggregator(PATH_AGG, PathAggregator.class);
			registerPersistentAggregator(U_UP_AGG,
					BooleanOverwriteAggregator.class);

			// set Aggregators initial values
			setAggregatedValue(PATH_AGG, new Text(inputPath));
			setAggregatedValue(U_UP_AGG, new BooleanWritable(false));

			// Start the File Observer
			fileObserver = new FileObserver(inputPath);

			LOG.info("Dynamic Master Compute successfully initialized.");
		}

		@Override
		public void compute() {
			LOG.info("MasterCompute - Superstep number: " + getSuperstep());
			isUnderUpdate = ((BooleanWritable) getAggregatedValue(U_UP_AGG))
					.get();

			// Waits two supersteps after an update in the files system
			// Then it assumes that the file system is up to date.
			// One can solve this by using aggregator.
			if (isUnderUpdate) {
				if (1 == numberOfWaitedSupersteps) {
					LOG.info("Changing is under update to FALSE");
					setAggregatedValue(U_UP_AGG, new BooleanWritable(false));
					numberOfWaitedSupersteps = 0;
				} else {
					numberOfWaitedSupersteps++;
				}
			}
			// If the framework already finished processing previous mutations
			if (!isUnderUpdate) {
				LOG.info("The framework is ready for mutations");
				// Reset aggregator value.
				setAggregatedValue(U_UP_AGG, new BooleanWritable(false));

				// Uncomment the line below in order to enable dynamic input
				// analysis
				// FileObserver.checkFileModification();

				// This next line is just for debugging the application. It will
				// indicate a file modification in determined supersteps.

				if (getSuperstep() % 10 == 9) {
					LOG.info("Creating framework update notification");
					setAggregatedValue(U_UP_AGG, new BooleanWritable(true));
				}

				// Inform injector if modification in FS
				if (true == fileObserver.getFileModifed()) {
					LOG.info("Modification in file:" + inputPath);
					setAggregatedValue(U_UP_AGG, new BooleanWritable(true));
				}
			}
		}

		/**
		 * Observes the HDFS for any modification in the input files
		 * 
		 */
		public static class FileObserver {
			private long modificationTime;

			/**
			 * Keeps track of the last modification time of the file.
			 */
			private Path PATH;

			/**
			 * True if the file was modified.
			 */
			private boolean fileModified = false;

			/** Class logger */
			private final Logger LOG = Logger.getLogger(FileObserver.class);

			/**
			 * Configuration file for the file system
			 */
			Configuration config = new Configuration();

			private FileSystem fs;

			/**
			 * Used to get the timestamp
			 */
			FileStatus fileStatus;

			FileObserver(String inputPath) {
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
				LOG.info("Original modification time (UTC) is: "
						+ modificationTime);
				this.fileModified = false;
			}

			/**
			 * Check if the tracked file was modified in the beginning of every
			 * super step
			 */
			public void checkFileModification() {

				// Check if one has to update the file status every single time.
				try {
					fileStatus = fs.getFileStatus(PATH);
				} catch (IOException e) {
					LOG.info("Error getting File status.");
				}

				long modtime = fileStatus.getModificationTime();
				if (modtime != modificationTime) {
					fileModified = true;
					this.modificationTime = modtime;
				} else {
					fileModified = false;
				}
			}

			public boolean getFileModifed() {
				return this.fileModified;
			}
		}
	}

	/**
	 * This is just a modification of the JSON Reader class available in the
	 * Giraph original classes.
	 */
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