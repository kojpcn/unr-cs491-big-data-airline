reset
rm -rf output
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class

export HADOOP_ROOT_LOGGER="WARN"

hadoop jar wc.jar WordCount airports/ airlines/ routes/ output/
