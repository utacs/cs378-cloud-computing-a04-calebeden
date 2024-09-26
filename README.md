# Assignment 3

## Team members' names

1. Student Name: Caleb Eden

   Student UT EID: cae2672

2. Student Name: Leo Lei

   Student UT EID: ll36476

## Course Name: CS378 - Cloud Computing

## Unique Number: 51515

## Report

### Task 1 - Errors in GPS Position Records

### Task 2 - Top 5 Taxis by GPS Error Rates

### Task 3 - Top 10 Drivers by Earnings per Minute

### Task 4 - Executing on Google Dataproc

## How to compile the project

We use Apache Maven to compile and run this project.

You need to install Apache Maven (<https://maven.apache.org/>)  on your system.

Type on the command line:

```bash
mvn clean package
```

## How to run Task 1

```bash
java -cp target/MapReduce-TaxiData-0.1-SNAPSHOT-jar-with-dependencies.jar edu.cs.utexas.HadoopEx.HourGPSErrors gs://cs378/taxi-data-sorted-large.csv gs://<bucket>/task1-output
```

## How to run Task 2

```bash
java -cp target/MapReduce-TaxiData-0.1-SNAPSHOT-jar-with-dependencies.jar edu.cs.utexas.HadoopEx.TaxiGPSErrors gs://cs378/taxi-data-sorted-large.csv gs://<bucket>/task2-intermediate gs://<bucket>/task2-output
```

## How to run Task 3

```bash
java -cp target/MapReduce-TaxiData-0.1-SNAPSHOT-jar-with-dependencies.jar edu.cs.utexas.HadoopEx.DriverEarnings gs://cs378/taxi-data-sorted-large.csv gs://<bucket>/task3-intermediate gs://<bucket>/task3-output
```
