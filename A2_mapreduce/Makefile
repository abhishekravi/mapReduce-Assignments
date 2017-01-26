# define a makefile variable for the java compiler
#
JCC = javac -classpath opencsv-3.6.jar:/usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar -sourcepath .

pseudo: format hstart build run

build: BadDataException.class RecordData.class DataValidator.class A2.class 
	jar cvf job.jar *.class

run:
	export HADOOP_CLASSPATH=.:`hadoop classpath`;HADOOP_CLASSPATH=$HADOOP_CLASSPATH:opencsv-3.6.jar;hadoop jar job.jar A2 $(input)

BadDataException.class: BadDataException.java
	@$(JCC) BadDataException.java

RecordData.class: RecordData.java
	@$(JCC) RecordData.java

DataValidator.class: DataValidator.java
	@$(JCC) DataValidator.java

A2.class: A2.java
	@$(JCC) A2.java

format: 
	hdfs namenode -format

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop:
	mr-jobhistory-daemon.sh stop historyserver 
	stop-yarn.sh
	stop-dfs.sh
graph:
	Rscript -e "rmarkdown::render('A2.Rmd')"
