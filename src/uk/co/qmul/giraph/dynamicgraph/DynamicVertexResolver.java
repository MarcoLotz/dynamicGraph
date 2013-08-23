package uk.co.qmul.giraph.dynamicgraph;

import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexChanges;
import org.apache.giraph.graph.VertexResolver;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Default implementation of how to resolve vertex creation/removal, messages
 * to nonexistent vertices, etc.
 *
 * @param <I>
 * @param <V>
 * @param <E>
 * @param <M>
 * 
 * available at: https://code.google.com/p/mahout-port-for-graph-hyracks/source/browse/trunk/src/main/
 * java/org/apache/giraph/graph/VertexResolver.java?r=16
 */

/*TODO: finish the implementation of this class*/

@SuppressWarnings("rawtypes")

public class DynamicVertexResolver<I extends WritableComparable, V extends Writable,
E extends Writable, M extends Writable>
implements VertexResolver<I, V, E>, Configurable {
	
	/** Configuration */
    private Configuration conf = null;

    /** State of the Graph */
    private GraphState graphState;

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(DynamicVertexResolver.class);

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
	
	/**
	 * Sets the graph state
	 * @param graphState to be set
	 */
	public void setGraphState(GraphState graphState) {
	      this.graphState = graphState;
	    }
	
	/**
	 * Returns the GraphState
	 * @return Object Graph State
	 */
	public GraphState getGraphState()
	{
		return this.graphState;
	}
	
	@Override
	public Vertex<I, V, E> resolve(
			I vertexId, 
			Vertex<I, V, E> vertex,
			VertexChanges<I, V, E> vertexChanges, 
			boolean hasMessages) {
		
		LOG.info("[PROMETHEUS] Resolving");
		return null;
	}
}
