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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
/**
 * Master compute associated with {@link DynamicGraphComputation}. It is the first thing to
 * run in each super step. It has a watcher to see if there is any modification in input
 */

public class InjectorMasterCompute extends DefaultMasterCompute {
	
	/** Class logger */
	private static final Logger LOG = Logger
			.getLogger(InjectorMasterCompute.class);
	
	FileWatcher fileWatcher;
	
	private final long SLEEPSECONDS = 10;
	
	@Override
	public void initialize() throws InstantiationException,
			IllegalAccessException {
		LOG.info("[Prometheus] Initializing the InjectorMasterCompute class");
		LOG.info("[Prometheus] Creating a FileWatcher object");
		fileWatcher = new  FileWatcher();
		if (fileWatcher != null){
			LOG.info("[Prometheus] FileWatcher object successfully created!");
		}
		LOG.info("[Prometheus] InjectorMasterCompute class initialization DONE!");
	}
	
	@Override
	public void compute() {
		LOG.info("[Prometheus] MasterCompute Compute() method:");
		LOG.info("[Prometheus] Superstep number: " + getSuperstep());
		LOG.info("[Prometheus] Checking if the file was modified");
		//checks if the target file was modified.
		fileWatcher.checkFileModification();
		
		try {
			LOG.info("[Prometheus] Going to sleep for " + TimeUnit.SECONDS.toMillis(SLEEPSECONDS));
			Thread.sleep(TimeUnit.SECONDS.toMillis(SLEEPSECONDS)); //Holds each superstep for SLEEPSECONDS
			// Each superstep is hold in order to be able to modify the input file
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		

		// If it found, alert the workers.
		if ( fileWatcher.getFileModifed() == true)
		{
			LOG.info("[Prometheus] The fileWatcher indicates that the file was modified.");
			//Compare the blocks so that only modified blocks should be updated:
			fileWatcher.compareBlocks();
			// I guess one should actually compare the input splits.
			// alert workers
		}
	}
	
	
	public static class FileWatcher {
		private String inputPath = "/user/hduser/dynamic/GoogleJSON.txt";
		private long modificationTime;
		
		/**
		 * The path that is going to be analysed in every Superstep.
		 * One may modify it later to change it during the computation
		 * Or add more than one Path (since multiple paths are already possible in Giraph)
		 */
		private Path PATH;
		
		/**
		 * This flag will become true if the file was modified.
		 */
		private boolean fileModified = false;
		
		/** Class logger */
		private static final Logger LOG = Logger
				.getLogger(FileWatcher.class);
		
		
		/**
		 * The configuration file that will be used for HDFS
		 */
		Configuration config = new Configuration();
		
		/**
		 *  Used in order to access the HDFS
		 */
		private FileSystem fs;
		
		/**
		 * Is the file status of the target file. The TimeStamp is in it.
		 */
		FileStatus fileStatus;
		
		/**
		 * Is the block locations of the file in the HDFS
		 */
		BlockLocation [] blockLocation;
		
		/**
		 * When the file is updated, this saves the old block Location in order to compare.
		 */
		BlockLocation [] oldBlockLocation;
		
		FileWatcher(){
			LOG.info("[PROMETHEUS] Initializing the FileWatcher.");
			LOG.info("[PROMETHEUS] The InputPath String is " + inputPath);
			
			LOG.info("[PROMETHEUS] Initializing the Path with the String.");
			PATH = new Path(inputPath);
			LOG.info("[PROMETHEUS] The Path content is " + PATH.getParent() + "/" + PATH.getName());

			System.out.println("TESTING OUTPUT TERMINAL!");
			LOG.info("[PROMETHEUS] Initializing file system. ");
			try
			{
				fs = FileSystem.get(config);
			} catch (IOException e) {
				LOG.info("[PROMETHEUS ERROR] Erros initializating the file system.");
			}
			LOG.info("[PROMETHEUS] FileSystem sucessfully created!");
			
			LOG.info("[PROMETHEUS] Getting the file status from the file system ");
			try{
				fileStatus = fs.getFileStatus(PATH);
			} catch (IOException e) {
				LOG.info("[PROMETHEUS ERROR] Error getting File status.");
			}
			
			LOG.info("[PROMETHEUS] File status sucessfully created!");
			
			LOG.info("[PROMETHEUS] Getting the file modification time: ");
			modificationTime = fileStatus.getModificationTime();
			LOG.info("[PROMETHEUS] Modification time got with success!");
			LOG.info("[PROMETHEUS] The modification time (UTC) is: " + modificationTime);
			
			LOG.info("[PROMETHEUS] setting the FileBlockLocations:");
			setFileBlockLocations();
			LOG.info("[PROMETHEUS] FileBlockLocations set!:");
			
			this.fileModified = false;
			LOG.info("[PROMETHEUS] File watcher initialization complete!");
		}

		/**
		 * Check if the watched file was modified in the beginning of every super step
		 */
		public void checkFileModification() {
			
			// Check if one has to update the file status every single time.
			try{
				fileStatus = fs.getFileStatus(PATH);
			} catch (IOException e) {
				LOG.info("[PROMETHEUS ERROR] Error getting File status.");
			}
			
			long modtime = fileStatus.getModificationTime();
			if ( modtime != modificationTime )
			{
				fileModified = true;
				this.modificationTime = modtime;
				LOG.info("[PROMETHEUS] The file was modified during the superstep!");
				LOG.info("[PROMETHEUS] The new modification time is: " + this.modificationTime);
				
				LOG.info("[PROMETHEUS] Storing previous blocks data");
				oldBlockLocation = blockLocation; //in order to compare the blocks.
				LOG.info("[PROMETHEUS] Previous blocks data stored!");
				
				LOG.info("[PROMETHEUS] Updating block data.");
				//updates the block locations:
				setFileBlockLocations();
				LOG.info("[PROMETHEUS] Done.");
				//check which splits were modified (maybe there are not modified splits).
				
			}
			else
			{
				fileModified = false;
				LOG.info("[PROMETHEUS] The file was NOT modified in the current superstep.");
			}
		}
		
		//Try to make HDFS accesses atomic!
		
		public void setFileBlockLocations()
		{
			LOG.info("[PROMETHEUS] Getting information about the file:");
			LOG.info("[PROMETHEUS] The block size is: " + fileStatus.getBlockSize());
			LOG.info("[PROMETHEUS] Geting the block locations... ");
			try
			{
				blockLocation = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
			} catch (IOException e) { 
				LOG.info("[PROMETHEUS ERROR] Error getting block locations of the file");			
			}
			
			LOG.info("[PROMETHEUS] Success geting block locations!");
			LOG.info("[PROMETHEUS] Printing block locations:");
			
			for ( int i = 0; i < blockLocation.length; i++)
			{
				LOG.info("[PROMETHEUS] " + blockLocation[i].toString());
			}
			
			LOG.info("[PROMETHEUS] setFileBlockLocations Done!:");
		}
		
		public void compareBlocks()
		{
			LOG.info("[PROMETHEUS] Comparing blocks");
			for (int i = 0; i < blockLocation.length; i ++)
			{
				boolean shouldUpdateBlock = true;
				//If true, then this block as no similarity with the previous one.
				
				for (int j = 0; j < oldBlockLocation.length; j++)
				{
					//if (blockLocation[i].hashCode() == oldBlockLocation[j].hashCode())
					if (blockLocation[i].equals(oldBlockLocation[j]))
					{
						LOG.info("[PROMETHEUS] The new block " + blockLocation[i].toString());
						LOG.info("[PROMETHEUS] and the old block " + oldBlockLocation[j].toString());
						LOG.info("[PROMETHEUS] have the same HashCode, so they probably are similar.");
						LOG.info("[PROMETHEUS] for this reason there is no need to update them to the workers");
						LOG.info("[PROMETHEUS] The Hash code is" + blockLocation[i].hashCode());
						
						shouldUpdateBlock = false;
					}
				}
				if ( true == shouldUpdateBlock )
				{

					LOG.info("[PROMETHEUS] The block has no matches in the old blocks");
					LOG.info("[PROMETHEUS] Note: There may be comparisson problems");
					//send this block to the worker with the old one.
				}
			}
		}
		
		public boolean getFileModifed(){
			return this.fileModified;
		}
		}
}