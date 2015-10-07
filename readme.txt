Team 15
Naga Padmavathy Kancharla
Youra Cho
Rahul Kilambi


*********Conten Based Recommendation system***********
Based on a query this project returns k books that are similar is mostly closely related to the user's query.

TfIdfmain.java is the driver file containing all the jobs to be performed. This project computations contain
6 Map-Reduce jobs of which four are required for preprocessing that is these are offline jobs. Two Jobs are 
based on user's query. 

********OfflineComputations*****************
Job1 - performs the term count in a document
Mapper - WordCountMapper.java
Reducer - WordCountReducer.java
Class file - WordDoc.java (implementing the serializable interface)

Job2 - calculates total number of terms in a document
Mapper - TermnNumberMapper.java
Reducer - TermNumberReducer.java

Job3 - calculates tfIdf values for terms in a document
Mapper - TfIdfMapper.java
Reducer - TfIdfReducer.java

Job4 - constructs documents that have pair-wise similarity 
Mapper - SimilarityMapper.java
Reducer -SimilarityReducer.java

**********Online Computations***********
Job5 - provide user input string and view documents with highest tfidf value
Mapper - StringFinderMapper.java
Reducer -StringFinderReducer.java

Job6 - provide closest match document as argument and view k recommendations(output)
Mapper - RecoMapper.java
Reducer- RecoReducer.java

*********Commands of Execution*************
############# Offline Jobs ########
compiling the program:
javac -d . -classpath $CLASSPATH:/DCNFS/applications/cdh/5.2/app/hadoop-2.5.0-cdh5.2.0/share/hadoop/mapreduce1/lib/hadoop-common-2.5.0-cdh5.2.0.jar:/DCNFS/applications/cdh/5.2/app/hadoop-2.5.0-cdh5.2.0/share/hadoop/mapreduce2/hadoop-mapreduce-client-core-2.5.0-cdh5.2.0.jar <directory of all java files(*.java)> -d classfiles

creating a jar:(all classed are created in separate directory as classfiles)
<change to classfiles>
jar -cvf TfIdfmain.jar *.class

execution:
hadoop jar ./TfIdfmain.jar TfIdfmain <input directory> <output directory>

Viewing outputs:
for query results:
hdfs dfs -cat <output directory/path5>

###############User Intractive computation(Recommendations)###########
for Recommendations the driver is ljob.java which is the job6 containing Recos mapper and reducer
compiling the program:
(for Similarity reco's user has to change directory to ljob containing all last job file)
javac -d . -classpath $CLASSPATH:/DCNFS/applications/cdh/5.2/app/hadoop-2.5.0-cdh5.2.0/share/hadoop/mapreduce1/lib/hadoop-common-2.5.0-cdh5.2.0.jar:/DCNFS/applications/cdh/5.2/app/hadoop-2.5.0-cdh5.2.0/share/hadoop/mapreduce2/hadoop-mapreduce-client-core-2.5.0-cdh5.2.0.jar <directory of all java files(*.java)> -d classfiles

creating a jar:
jar -cvf ljob.jar *.class

execution:
hadoop jar ./ljob.jar ljob <argument-query book result from path5>

Viewing outputs:
for query results:
hdfs dfs -cat <output directory/path6>







