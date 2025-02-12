# Movie Script Analysis using Hadoop MapReduce

## Project Overview
This project implements a **Hadoop MapReduce** job to analyze a movie script dataset. The program processes a dataset of dialogues spoken by characters in movies and extracts key insights such as:
1. **Most Frequently Spoken Words by Characters**
2. **Total Dialogue Length per Character**
3. **Unique Words Used by Each Character**
4. **Hadoop Counters for Various Metrics**

The analysis helps in understanding character dialogues, their frequency, unique word usage, and overall dialogue length.

## Approach and Implementation

### **1. Most Frequently Spoken Words by Characters**
- **Mapper (`CharacterWordMapper.java`)**: Extracts words spoken by characters and emits key-value pairs in the format `<Character#Word, 1>`.
- **Reducer (`CharacterWordReducer.java`)**: Aggregates the word counts for each character and outputs the total occurrences.

### **2. Total Dialogue Length per Character**
- **Mapper (`DialogueLengthMapper.java`)**: Computes the length of dialogue for each character and emits key-value pairs in the format `<Character, DialogueLength>`.
- **Reducer (`DialogueLengthReducer.java`)**: Aggregates the total dialogue length per character.

### **3. Unique Words Used by Each Character**
- **Mapper (`UniqueWordsMapper.java`)**: Extracts unique words spoken by characters and emits key-value pairs `<Character, Word>`.
- **Reducer (`UniqueWordsReducer.java`)**: Collects all unique words spoken by a character and outputs them as a set.

### **4. Hadoop Counters for Various Metrics**
- **Mapper (`HadoopCountersMapper.java`)**: Implements Hadoop Counters to track:
  - Total number of lines processed
  - Total words processed
  - Total characters processed
  - Total unique words identified
  - Number of characters speaking dialogues
- **No Reducer Required** as Counters are automatically handled by Hadoop.

## Execution Steps

### **1. Setup and Build the Project**
```bash
mvn clean install
```

### **2. Start the Hadoop Cluster**
```bash
docker-compose up -d
```

### **3. Create HDFS Directories and Upload Data**
```bash
docker exec -it namenode /bin/bash
hadoop fs -mkdir -p /input/movie_script
hadoop fs -put input/movie_dialogues.txt /input/movie_script
```

### **4. Run the Hadoop MapReduce Job**
```bash
docker exec -it resourcemanager /bin/bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/hands-on2-movie-script-analysis-1.0-SNAPSHOT.jar \
com.movie.script.analysis.MovieScriptAnalysis /input/movie_script /output
```

### **5. View the Output**
#### Task 1: Most Frequently Spoken Words by Characters
```bash
hadoop fs -cat /output/task1/part-r-00000
```

#### Task 2: Total Dialogue Length per Character
```bash
hadoop fs -cat /output/task2/part-r-00000
```

#### Task 3: Unique Words Used by Each Character
```bash
hadoop fs -cat /output/task3/part-r-00000
```

#### Task 4: Hadoop Counter Output
```bash
hadoop fs -cat /output/task4/part-r-00000
```

## Challenges Faced & Solutions
### **Issue 1: Input Path Does Not Exist**
- **Problem**: Received an error indicating that input path `/input/movie_script/movie_dialogues.txt` does not exist.
- **Solution**: Verified the correct HDFS path and re-uploaded the file using `hadoop fs -put`.

### **Issue 2: Compilation Failure Due to Data Type Mismatch**
- **Problem**: The `HadoopCountersMapper.java` had a type mismatch between `Text` and `IntWritable`.
- **Solution**: Updated the reducer output format to match expected types.

### **Issue 3: Incorrect Arguments in Job Configuration**
- **Problem**: Used `args[0]` and `args[1]` in all jobs, causing path conflicts.
- **Solution**: Modified the job arguments to use `args[1]` for input and `args[2]` for output.

## Sample Input
```
HARRY: I am the chosen one!
HERMIONE: You are ridiculous.
RON: It's Leviosa, not Leviosar!
```

## Sample Output
### **Task 1: Most Frequently Spoken Words by Characters**
```
HARRY#the 1
HARRY#chosen 1
HARRY#one 1
HERMIONE#you 1
HERMIONE#are 1
HERMIONE#ridiculous 1
RON#it's 1
RON#leviosa 1
RON#not 1
RON#leviosar 1
```

### **Task 2: Total Dialogue Length per Character**
```
HARRY 23
HERMIONE 18
RON 31
```

### **Task 3: Unique Words Used by Each Character**
```
HARRY [the, chosen, one]
HERMIONE [you, are, ridiculous]
RON [it's, leviosa, not, leviosar]
```

### **Task 4: Hadoop Counter Output**
```
Total Lines Processed: 1032
Total Words Processed: 9600
Total Characters Processed: 49752
Total Unique Words Identified: 223068
Number of Characters Speaking: 1032
```

## Conclusion
This project successfully implemented **Hadoop MapReduce** to analyze movie script dialogues. The output provides valuable insights into dialogue frequency, character speech length, unique words, and script statistics through **Hadoop Counters**. 

