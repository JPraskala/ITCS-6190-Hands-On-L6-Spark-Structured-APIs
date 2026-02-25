# Music Streaming Analysis Using Spark Structured APIs

## Overview
The program creates two csv files from `datagen.py`. These two csv files are `listening_logs.csv` and `songs_metadata.csv`. Once these two csv files are created, they are used to execute four tasks in `main.py`. After these four tasks are computed, the results are written to four csv files in the outputs directory.

## Dataset Description
There are two datasets that the program uses. These two datasets are `listening_logs.csv` and `songs_metadata.csv`. 

## Repository Structure
The repository contains the outputs directory which contains the four csv files that are created from main.py. In the root, there are the rwo Python files, the ReadMe, and the two csv files that are created from `datagen.py`.

## Output Directory Structure
The outputs directory contains the four csv files. Each csv file pertains to a specific task that was executed in `main.py`. 

## Tasks and Outputs
### Task 1: User Favorite Genres.
The output is in outputs/favorite_genres.csv

#### Sample:
```
user_id,genre,user_genre_count
user_1,Pop,4
user_10,Classical,3
user_10,Pop,3
user_100,Rock,3
user_100,Jazz,3
user_11,Hip-Hop,4
```
### Task 2: Average Listen Time
The output is in outputs/average_listening_time.csv 

#### Sample: 
```
user_id,Average Listen Time
user_58,205.45454545454547
user_94,178.25
user_73,146.4
user_85,209.92307692307693
user_14,133.08333333333334
user_56,202.375
```
### Task 3: Create your own Genre Loyalty Scores and rank them and list out top 10
The output is in outputs/loyalty_score.csv

#### Sample:
```
user_id,genre,loyalty_score
user_75,Jazz,0.8671497584541062
user_39,Hip-Hop,0.8280542986425339
user_28,Classical,0.7386692381870781
user_68,Classical,0.7274844720496895
user_30,Classical,0.6917385943279901
user_96,Classical,0.6734463276836158
```
### Task 4: Identify users who listen between 12 AM and 5 AM
The output is in outputs/users_listen.csv

#### Sample:
```
user_id
user_58
user_94
user_14
user_56
user_68
user_22
```

## Execution Instructions
To execute the code, follow the instructions below. You will need pyspark and pandas installed on your machine or in a virtual environment to run the code. Also, ensure you have an older version of Java (Java 17 is recommended). 

## _Prerequisites_

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. _Python 3.x_:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. _PySpark_:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. _Apache Spark_:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### _2. Running the Analysis Tasks_

#### _Running Locally_

1. _Generate the Input_:

```bash
 python3 input_generator.py
```

2. **Execute Each Task Using spark-submit**:

   ```bash
     spark-submit main.py
   ```

3. _Verify the Outputs_:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Errors and Resolutions

### First Error

When I called the spark-submit command, I received an error which said `pyspark.errors.exceptions.captured.UnsupportedOperationException: getSubject is not supported`. I fixed this by downgrading to an earlier version of Java. The newer versions of Java are not compatible with Apache Spark. After downgrading from Java 24 to Java 17, it worked.

### Second Error

Initially, when I wrote my outputs to the csv files, I received an error stating that an object of type `None` has no attribute mode. I fixed this issue by removing the `show()` function from the outputs.

### Third Error 
The third error also occurs when I attempt to write my outputs to the csv files. The message I received was I did not have Pandas installed. After running the command `python3 -m pip install pandas`, I was able to successfully execute the code.
