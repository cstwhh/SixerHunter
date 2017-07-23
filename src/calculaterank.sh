hadoop com.sun.tools.javac.Main CalculateRank.java
jar cf jar/CalculateRank.jar CalculateRank*.class
scp jar/CalculateRank.jar 2014011446@166.111.227.245:/home/2014011446/CalculateRank.jar
rm *.class

