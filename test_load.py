from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession

commentsDF = SparkSession.read.json("comments-minimal.json.bz2")
SparkSession.read.json("submissions.json.bz2")
SparkSession.read.csv("labeled_data.csv")

