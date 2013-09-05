dynamicGraph
============

Main Goal: Implement a computation procedure that allows Apache Giraph to
compute dynamic graphs. By dynamic graph, one should understand as a graph
that changes while the system (in this case the Giraph framework) is deliberating about it.

Usage: Define in the MasterCompute class what is the input (or inputs)
that should be monitored. The Master will inform the vertex when one of
inputs changes.

Description: One could find a more in-depth description of this application 
in the pdf report. It includes benchmarks, flowcharts and possible improvements.

