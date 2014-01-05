This is the source code for the following blog about Pivotal Hadoop Distribution and HAWQ distributed query engine:
http://bighadoop.wordpress.com/2014/01/05/pivotal-hadoop-distribution-and-hawq-realtime-query-engine/

The code example demonstrates a MapReduce algorithm to calculate the highest stock price of Apple, Google and Nokia and when these stocks reached their peak value.

BUILD AND RUN THE CODE
======================

Below are the steps to compile the code and execute the MapReduce algorithm:

$ mvn archetype:generate -DarchetypeGroupId=org.apache.maven.archetypes -DarchetypeArtifactId=maven-archetype-quickstart -DgroupId=highest_stock_price -DartifactId=highest_stock_price

$ vi pom.xml
Add dependecy and build

$ mvn clean compile
$ mvn -DskipTests package

$ cd ~/input
$ hadoop fs -mkdir /stock_demo/input
$ hadoop fs -put *.csv /stock_demo/input/
$ hadoop fs -ls /stock_demo/input/

$ hadoop jar target/highest_stock_price-1.0.jar highest_stock_price/HighestStockPriceDriver /stock_demo/input/ /stock_demo/output/

$ hadoop fs -cat /stock_demo/output/part*
AAPL:	2012-09-19	685.76
GOOG:	2013-07-15	924.69
NOK:	2000-06-19	42.24


$ hadoop job -status <JOB_ID>

