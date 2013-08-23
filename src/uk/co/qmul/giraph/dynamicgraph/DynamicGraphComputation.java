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
import org.apache.giraph.edge.ArrayListEdges;
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
 * Demonstrates the Pregel concepts applied to Dynamic Graphs. It uses a File
 * Observer to warn a special vertex when and what to inject into the
 * application
 * 
 * @author Marco Aurelio Lotz
 */

@Algorithm(name = "Dynamic Graph Computation", description = "Computes dynamic graphs")
public class DynamicGraphComputation
		extends
		BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

	/**
	 * Path Aggregator name. This Aggregator is used to communicate the
	 * injection Path from the Master to the Injector vertex.
	 */
	private static String PATH_AGG = "PathAgg";

	/**
	 * File System update aggregator. This aggregator is used in order to
	 * communicate the injector that a modification in the observed file has
	 * happened.
	 */
	private static String FS_AGG = "FileSystemAgg";

	/**
	 * Injection Complete Status Aggregator. Used to tell the Master that the
	 * vertice database inside the workers is up-to-date. Blocks new data base
	 * update request from the master.
	 */

	private static String INJ_RDY_AGG = "InjectionReadyAgg";

	/**
	 * Maximum number of Supersteps to be computed before halting.
	 */
	public final int MAX_SUPERSTEPS = 7;

	/**
	 * Used by the injector vertex in order to wait a complete superstep before
	 * starting injection. In the superstep that the injector is waiting, nodes
	 * that are not the injector request removal.
	 */
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
	 * Send messages to all adjacent vertices. The content of the messages is
	 * not important, since just the event of receiving a message removes the
	 * vertex from the inactive status. This should be modified for other types
	 * of computations
	 * 
	 * @param current
	 *            vertex.
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

			// Checks if Master indicated a modification in the FileSystem
			fsModificationStatus = (BooleanWritable) getAggregatedValue(FS_AGG);

			// Injector vertex routine
			if (vertex.getId() == INJECTOR_VERTEX_ID) {
				InjectorMonitor();
			}
			// All other vertex computation are here.
			else {
				// Removes all nodes if there was a FS modification
				if ((vertex.getId() != INJECTOR_VERTEX_ID)
						&& (fsModificationStatus.get())) {
					LOG.info("Vertex :" + vertex.getId().get()
							+ " being removed in superstep: " + getSuperstep());
					removeVertexRequest(vertex.getId());
				} else {
					// Insert standard computation code here.
					// It is important the a vertex that requested to be removed
					// do not receive any message.
					BFSMessages(vertex);

					// The injector vertex should never halt.
					vertex.voteToHalt();
				}
			}
		} else {
			// Always converge if Maximum Superstep, even the Injector
			vertex.voteToHalt();
		}
	}

	/**
	 * Injector vertex checks for a modification in the file system.
	 */
	public void InjectorMonitor() {
		// Checks for file system update
		if (true == fsModificationStatus.get()) {
			LOG.info("Master indicated modification in input files.");
			LOG.info("Modification superstep: " + getSuperstep());

			// Only Injects a superstep after the modification, the first call
			// is
			// when the vertices will be getting removed.
			// One can also do this by using the getNumberVertex and wait until
			// it is only the injector or do in the same removal superstep by
			// using the VertexResolver
			if (true == WaitedRemoval) {
				try {
					updateVertexDataBase();

					// Informs master that the update is finished
					informMasterCompute();
				} catch (IOException e) {
					LOG.info("Problem updating vertex data base.");
					e.getStackTrace();
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
	public void updateVertexDataBase() throws IOException {
		FileSystem fs;
		Configuration config = new Configuration();
		FileStatus fileStatus;

		// Gets the File System path to the modified file
		Text inputString = getAggregatedValue(PATH_AGG);
		Path inputPath = new Path(inputString.toString());

		LOG.info("Injector: the path is" + inputPath.getParent()
				+ inputPath.getName());

		fs = FileSystem.get(config);

		LOG.info("Checking file in File System");
		fileStatus = fs.getFileStatus(inputPath);
		if (null != fileStatus) {
			// Do the injection.
			Inject(fs, inputPath);
		} else {
			LOG.info("Problem looking for the file in HDFS");
		}
	}

	/**
	 * injects new vertex from desired input. Note: One may modify it in the
	 * future with vertexMutations To allow vertex mutation without removing the
	 * vertices in a previous superstep
	 * 
	 * @param file
	 *            system that is going to be used
	 * @param path
	 *            that is going to be read
	 * @throws IOException
	 */
	public void Inject(FileSystem fs, Path path) throws IOException {

		// Creates JSON variables
		String line;
		Text inputLine;
		JSONArray preProcessedLine;

		// Creates a JSON reader
		JSONDynamicReader DynamicReader = new JSONDynamicReader();

		// Creates vertex injection variables
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

				// Transforms from <iterable>edge to outEdge
				ArrayListEdges<LongWritable, FloatWritable> outEdges = new ArrayListEdges<LongWritable, FloatWritable>();
				outEdges.initialize(vertexEdges);

				// Requests vertex add
				addVertexRequest(vertexId, vertexValue, outEdges);
				LOG.info("Adding vertex: id,value:" + vertexId + ","
						+ vertexValue);

				// Gets next file line
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
		super.preSuperstep();
		if (getSuperstep() == 0) {
			try {
				addVertexRequest(INJECTOR_VERTEX_ID, INJECTOR_VERTEX_VALUE);
			} catch (IOException e) {
				LOG.info("Could not create injector vertex!");
				e.printStackTrace();
			}
			LOG.info("Injector sucessfully created!");
		}
	}

	/**
	 * Tells master compute that the vertex data-base is up-to-date
	 */
	public void informMasterCompute() {
		aggregate(INJ_RDY_AGG, new BooleanWritable(true));
	}

	/**
	 * Master Compute associated with {@link DynamicGraphComputation}. It is the
	 * first thing to run in each super step. It has an observer to see if there
	 * is any modification in input
	 */

	public static class InjectorMasterCompute extends DefaultMasterCompute {

		/**
		 * Insert the paths to be watched here. One can easily modify the
		 * Aggregator to use an array of paths.
		 */
		private String inputPath = "/user/hduser/dynamic/GoogleJSON.txt";

		/**
		 * Used by the master compute to avoid accessing the file system while
		 * the workers are still processing a previous mutation
		 */
		BooleanWritable isRdyForMutations;

		/** Class logger */
		private final Logger LOG = Logger
				.getLogger(InjectorMasterCompute.class);

		/**
		 * Object that will watch the given paths.
		 */
		FileObserver FileObserver;

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
			setAggregatedValue(INJ_RDY_AGG, new BooleanWritable(true));

			// Start the File Watcher
			FileObserver = new FileObserver(inputPath);
			LOG.info("Dynamic Master Compute successfully initialized.");
		}

		@Override
		public void compute() {

			isRdyForMutations = (BooleanWritable) getAggregatedValue(INJ_RDY_AGG);

			if (true == isRdyForMutations.get()) {
				LOG.info("The structure is ready for mutations in superstep: "
						+ getSuperstep());
				LOG.info("Checking if the file was modified");

				// Uncomment the line below in order to enable dynamic input
				// analysis
				// FileObserver.checkFileModification();

				// This next line is just for debugging the application. It will
				// indicate a file modification in every check.
				setAggregatedValue(FS_AGG, new BooleanWritable(true));

				// If there was a modification, inform injector.
				if (FileObserver.getFileModifed() == true) {
					// Change this to create a get routine from file observer
					// This will ignore non-modified files in a multiple
					// file input
					LOG.info("Modification in file:" + inputPath);
					setAggregatedValue(FS_AGG, new BooleanWritable(true));
				}
			}
		}

		/**
		 * Observes the file system for any modification in the input files
		 * 
		 */
		public static class FileObserver {

			/**
			 * Keeps track of the last modification time of the file.
			 */
			private long modificationTime;

			/**
			 * Path to the observed file. May be modified to become an array an
			 * support more than one file.
			 */
			private Path PATH;

			/**
			 * This flag. True if the file was modified.
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
			 * File status of the target file. The time stamp is in it.
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
			 * Check if the observed file was modified in the beginning of every
			 * super step
			 */
			public void checkFileModification() {
				// Check if one has to update the file status every single time.
				try {
					fileStatus = fs.getFileStatus(PATH);
				} catch (IOException e) {
					LOG.info("Error getting File status.");
					e.getStackTrace();
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