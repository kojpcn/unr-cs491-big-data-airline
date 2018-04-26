reset
rm -rf output
hadoop com.sun.tools.javac.Main AirlineSearchEngine.java
jar cf wc.jar AirlineSearchEngine*.class

export HADOOP_ROOT_LOGGER="WARN"

hadoop jar wc.jar AirlineSearchEngine airports/ airlines/ routes/ output/
