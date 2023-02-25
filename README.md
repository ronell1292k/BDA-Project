# BDA Project (Sem2)
Java Hadoop MapReduce code for my Big Data Analytics Project

## Pre-requisites:
1. JDK 
2. Hadoop

## Steps
1. Make sure HADOOP_CLASSPATH is correctly set to the tools.jar within jdkx.y/lib/
2. Compile the code: `hadoop com.sun.tools.javac.Main File.java`
3. Create JAR file: `jar cf File.jar *.class`
4. Create input directory in Hadoop: `hadoop dfs -mkdir /dir`
5. Upload input file into Hadoop: `hadoop dfs -put input.csv /dir/input.txt`
6. Run the code: `hadoop jar File.jar File /dir/input.txt /dir/out.txt`
7. Check the output directory: `hadoop dfs -ls /dir/*`
8. Verify the output `hadoop dfs -cat /dir/out.txt/part-r-00000`

