hadoop com.sun.tools.javac.Main Preprocess.java
jar cf jar/Preprocess.jar Preprocess*.class
scp jar/Preprocess.jar 2014011446@166.111.227.245:/home/2014011446/Preprocess.jar
rm *.class
