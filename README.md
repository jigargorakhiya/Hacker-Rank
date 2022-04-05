## Environment:
- Java version: 1.8
- Maven version: 3.*
- Spark Version: 3.0.0
- Scala Version: 2.12.12

## Read-Only Files:
- src/test/*
- src/main/scala/com/hackrrank/spark/base/DataCleaningJobInterface.scala
- src/main/resources/spark/eligibility.csv
- src/main/resources/spark/medical.csv

## Requirements:
In this challenge, you are going to write a spark job that does data cleaning. Basically you have to filter `medical.csv` file based on `eligibility.csv` file and do some data manipulation. Sample files are given in `src/main/resources/spark`. 

- `eligibility.csv`
  - it contains the data in the layout `memberId,firstName,lastName`
  - it is a csv file with one line per `memberId`
  
- `medical.csv`
  - it contains the data in the layout `memberId,fullName,paidAmount`
  - it is a csv file with one line per `memberId`

`Eligibility-Medical Relationship:`
 -  Each medical member has one corresponding eligibility record.

The project is partially completed and there are 4 methods and a spark session to be implemented in the class `DataCleaningJob.scala`:

- `sparkSession: SparkSession`:
  - create a spark session with master `local` and name `Data Cleaning`

- `filterMedical(eligibilityDs: Dataset[Eligibility], medicalDs: Dataset[Medical]): Dataset[Medical]`:
  - remove all the rows from `medicalDs` whose `memberId` is not present in `eligibilityDs`
  - returned the filtered `medicalDs`

- `generateFullName(eligibilityDs: Dataset[Eligibility], medicalDs: Dataset[Medical]): Dataset[Medical]`:
  - `fullName` column in `medicalDs` is empty. So populate it by concatenating `firstName` and `lastName` column like `firstName<SPACE>lastName` from `eligibilityDs`
  - return the `medicalDs`

- `findMaxPaidMember(medicalDs: Dataset[Medical]): String`:
  - find the member which has highest `paidAmount`
  - return the member's `memberId`

- `findTotalPaidAmount(medicalDs: Dataset[Medical]): Long`:
  - find the sum of `paidAmount` column in the `medicalDs`
  - return the total sum
    
Your task is to complete the implementation of that job so that the unit tests pass while running the tests. You can use the give tests check your progress while solving problem.

## Commands
- run: 
```bash
mvn clean package; spark-submit --class com.hackerrank.spark.AppMain --master local[*] target/data-cleaning-1.0.jar
```
- install: 
```bash
mvn clean install
```
- test: 
```bash
mvn clean test
```
