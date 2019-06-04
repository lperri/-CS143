from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import StringType, ArrayType, IntegerType
import re
import string
import json
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator



def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing 3 strings
    1. The unigrams
    2. The bigrams
    3. The trigrams
    """
    text = text.replace('\n','')
    text = text.replace('\t','')
    text = re.sub('\[([\s\S]+)\] \((https?:\/\/)?(www.)?\w+\.\w+\)','\\1',text)
    parsed_text = re.findall('[\w\-\']+|[.,;:?!]', text)
    for i in range(len(parsed_text)):
        parsed_text[i] = (parsed_text[i]).lower()
    parsed_text = list(filter(None,parsed_text))
    parsed_text_str = ' '.join(parsed_text)
    u_sequences = re.findall('[\w\-\']+',parsed_text_str)
    b_sequences = [u_sequences[i:i+2] for i in range(len(u_sequences)-1)]
    t_sequences = [u_sequences[i:i+3] for i in range(len(u_sequences)-2)]
    unigrams_str = ' '.join(u_sequences)
    bigrams = ['_'.join(b_sequence) for b_sequence in b_sequences]
    bigrams_str = ' '.join(bigrams)
    trigrams = ['_'.join(t_sequence) for t_sequence in t_sequences]
    trigrams_str = ' '.join(trigrams)
    return [unigrams_str, bigrams_str, trigrams_str]

def combineNgrams(ngrams):
    ''' given an array [unigrams (string), bigrams (string), trigrams (string)]
        return one array containing all words in the three strings of the input array '''
    unigrams, bigrams, trigrams = ngrams
    ngrams_combined = unigrams.split(" ")
    for item in bigrams.split(" "):
        ngrams_combined.append(item)
    for item in trigrams.split(" "):
        ngrams_combined.append(item)
    return ngrams_combined

sanitize_udf = udf(lambda x: sanitize(x), ArrayType(StringType()))
combineNgrams_udf = udf(lambda x: combineNgrams(x), ArrayType(StringType()))

def main(context):
    """Main function takes a Spark SQL context."""
    comments_df = context.read.parquet("comments.parquet")
    submissions_df = context.read.parquet("submissions.parquet")
    labeled_data_df = context.read.parquet("labeled_data.parquet")
    comment_label_join = comments_df.join(labeled_data_df, comments_df.id == labeled_data_df._c0)
    columns_to_drop = ['author', 'author_cakeday', 'controversiality', 'author_flair_css_class', 'can_gild', 'can_mod_post', 'collapsed', 'collapsed_reason', 'distinguished', 'edited', 'gilded', 'is_submitter', 'link_id', 'parent_id', 'permalink', 'retrieved_on', 'stickied', 'subreddit', 'subreddit_id', 'subreddit_type', '_c0', '_c1', '_c2']
    df = comment_label_join.drop(*columns_to_drop)
    df = df.withColumnRenamed('_c3', 'label_djt')
    df = df.withColumn('body_sanitized', sanitize_udf(df.body))
    df = df.withColumn('ngrams_combined', combineNgrams_udf(df.body_sanitized))
    df = df.withColumn('poslabel', when(col("label_djt") == 1, 1).otherwise(0))
    df = df.withColumn('neglabel', when(col("label_djt") == -1, 1).otherwise(0))
    df = df.drop('body_sanitized')
    df.show(5,False)
    cv = CountVectorizer(inputCol="ngrams_combined", binary=True, outputCol="features", minDF=10.0)
    model = cv.fit(df)
    model.transform(df).show(truncate=False)


    # Initialize two logistic regression models.
    # labelCol is the column containing the label, and featuresCol is the column containing the features.
    poslr = LogisticRegression(labelCol="poslabel", featuresCol="features", maxIter=10)
    neglr = LogisticRegression(labelCol="neglabel", featuresCol="features", maxIter=10)
    # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
    posEvaluator = BinaryClassificationEvaluator()
    negEvaluator = BinaryClassificationEvaluator()
    # There are a few parameters associated with logistic regression. We do not know what they are a priori.
    # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
    # We will assume the parameter is 1.0. Grid search takes forever.
    posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
    negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
    # We initialize a 5 fold cross-validation pipeline.
    posCrossval = CrossValidator(
        estimator=poslr,
        evaluator=posEvaluator,
        estimatorParamMaps=posParamGrid,
        numFolds=5)
    negCrossval = CrossValidator(
        estimator=neglr,
        evaluator=negEvaluator,
        estimatorParamMaps=negParamGrid,
        numFolds=5)
    # Although crossvalidation creates its own train/test sets for
    # tuning, we still need a labeled test set, because it is not
    # accessible from the crossvalidator (argh!)
    # Split the data 50/50
    posTrain, posTest = pos.randomSplit([0.5, 0.5])
    negTrain, negTest = neg.randomSplit([0.5, 0.5])
    # Train the models
    print("Training positive classifier...")
    posModel = posCrossval.fit(posTrain)
    print("Training negative classifier...")
    negModel = negCrossval.fit(negTrain)

# Once we train the models, we don't want to do it again. We can save the models and load them again later.
posModel.save("pos.model")
negModel.save("neg.model")




if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)
