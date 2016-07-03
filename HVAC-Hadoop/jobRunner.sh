#!/bin/bash
hadoop fs -mkdir -p input
hadoop fs -put /home/iam/IdeaProjects/HVAC-Full-System/HVAC-Hadoop/data/firstResult.csv input/
hadoop jar /home/iam/IdeaProjects/HVAC-Full-System/HVAC-Hadoop/target/HVAC-1.0-SNAPSHOT.jar DataAnalysisDriver