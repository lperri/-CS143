from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import udf, col, when, element_at
from pyspark.sql.types import StringType, FloatType, ArrayType, IntegerType
from pyspark.sql.window import Window
from os import path
import re
import string
import json
from pyspark.ml import PipelineModel
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
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


def cleanedCommentDF(comments_df):
    ''' cleans up comment dataframe '''
    comment_columns_to_drop = ['author', 'link_id', 'author_cakeday', 'controversiality', 'author_flair_css_class', 'can_gild', 'can_mod_post', 'collapsed', 'collapsed_reason', 'distinguished', 'edited', 'gilded', 'is_submitter', 'parent_id', 'permalink', 'retrieved_on', 'stickied', 'subreddit', 'subreddit_id', 'subreddit_type']
    comments_df = comments_df.withColumn('link_id_cleaned',remove_t3_from_link_udf(comments_df.link_id))
    comments_df = comments_df.drop(*comment_columns_to_drop)
    comments_df = comments_df.filter("body not like '%/s%'")
    comments_df = comments_df.filter("body not like '&gt;%'")
    comments_df = comments_df.withColumn('body_sanitized', sanitize_udf(comments_df.body))
    comments_df = comments_df.withColumn('ngrams_combined', combineNgrams_udf(comments_df.body_sanitized))
    comments_df = comments_df.drop('body_sanitized')
    return comments_df

def createLabeledDF(comments_df,labeled_data_df):
    ''' creates training dataframe and writes it as parquet file to local dir '''
    comment_label_join = comments_df.join(labeled_data_df, comments_df.id == labeled_data_df._c0)
    columns_to_drop = ['author', 'author_cakeday', 'controversiality', 'author_flair_css_class', 'can_gild', 'can_mod_post', 'collapsed', 'collapsed_reason', 'distinguished', 'edited', 'gilded', 'is_submitter', 'link_id', 'parent_id', 'permalink', 'retrieved_on', 'stickied', 'subreddit', 'subreddit_id', 'subreddit_type', '_c0', '_c1', '_c2']
    labeled_df = comment_label_join.drop(*columns_to_drop)
    labeled_df = labeled_df.withColumnRenamed('_c3', 'label_djt')
    labeled_df = labeled_df.withColumn('body_sanitized', sanitize_udf(labeled_df.body))
    labeled_df = labeled_df.withColumn('ngrams_combined', combineNgrams_udf(labeled_df.body_sanitized))
    labeled_df = labeled_df.withColumn('poslabel', when(col("label_djt") == 1, 1).otherwise(0))
    labeled_df = labeled_df.withColumn('neglabel', when(col("label_djt") == -1, 1).otherwise(0))
    labeled_df = labeled_df.drop('body_sanitized') 
    labeled_df.write.parquet("df_label.parquet")
    return labeled_df

def train(labeled_df):
    ''' train to get pos and neg models '''
    cv = CountVectorizer(inputCol="ngrams_combined", binary=True, outputCol="features", minDF=10.0)
    cvModel = cv.fit(labeled_df)
    labeled_df = cvModel.transform(labeled_df)
    cvModel.save("cvModel")
    poslr = LogisticRegression(labelCol="poslabel", featuresCol="features", maxIter=10)
    neglr = LogisticRegression(labelCol="neglabel", featuresCol="features", maxIter=10)
    # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
    posEvaluator = BinaryClassificationEvaluator(labelCol="poslabel")
    negEvaluator = BinaryClassificationEvaluator(labelCol="neglabel")
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
    pos = labeled_df
    neg = labeled_df
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
    return cvModel, posModel, negModel




sanitize_udf = udf(lambda x: sanitize(x), ArrayType(StringType()))
combineNgrams_udf = udf(lambda x: combineNgrams(x), ArrayType(StringType()))
remove_t3_from_link_udf = udf(lambda x: x.strip('t3_'), StringType())
get_probability_udf = udf(lambda x: float(x[1]), FloatType()) 


def main(context):
    """Main function takes a Spark SQL context."""
    comments_df = context.read.parquet("comments.parquet")
    submissions_df = context.read.parquet("submissions.parquet")
    labeled_data_df = context.read.parquet("labeled_data.parquet")

    if path.exists("df_label.parquet"): 
        labeled_df = context.read.parquet("df_label.parquet")
        comments_df = cleanedCommentDF(comments_df)
    
    else:
        labeled_df = createLabeledDF(comments_df,labeled_data_df)
        comments_df = cleanedCommentsDF(comments_df)    

    if path.exists("cvModel"):
        cvModel = CountVectorizerModel.load("cvModel")
        posModel = CrossValidatorModel.load("pos.model")
        negModel = CrossValidatorModel.load("neg.model")
    else:
        cvModel, posModel, negModel = train(labeled_df)

    # the "final join" without actually joining!
    output = cvModel.transform(comments_df)
    output = output.drop('score')
    output = output.drop('ngrams_combined')
    output = output.drop('link_id_cleaned')
    posResult = posModel.transform(output)
    posResult = posResult.drop('rawPrediction')
    posResult = posResult.drop('prediction')
    posResult = posResult.withColumnRenamed('probability','pos_prob')
    fullResult = negModel.transform(posResult)
    fullResult = fullResult.withColumnRenamed('probability','neg_prob')
    fullResult = fullResult.drop('rawPrediction')
    fullResult = fullResult.drop('prediction')
    fullResult = fullResult.withColumn('neg',when(get_probability_udf(fullResult.neg_prob) > 0.25, 1).otherwise(0))
    fullResult = fullResult.withColumn('pos',when(get_probability_udf(fullResult.pos_prob) > 0.2, 1).otherwise(0))
    fullResult.write.parquet("resulting_df.parquet")
    #fullResult_df = context.read.parquet("resulting_df.parquet")

    print(fullResult.count())
    # task 10 percentages



if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)
