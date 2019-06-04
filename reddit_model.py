from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import StringType, ArrayType, IntegerType
import re
import string
import json
from pyspark.ml import PipelineModel
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
remove_t3_from_link_udf = udf(lambda x: x.strip('t3_'), StringType())



def main(context):
    """Main function takes a Spark SQL context."""
    comments_df = context.read.parquet("comments.parquet")
    submissions_df = context.read.parquet("submissions.parquet")
    labeled_data_df = context.read.parquet("labeled_data.parquet")
    comment_label_join = comments_df.join(labeled_data_df, comments_df.id == labeled_data_df._c0)
    columns_to_drop = ['author', 'author_cakeday', 'controversiality', 'author_flair_css_class', 'can_gild', 'can_mod_post', 'collapsed', 'collapsed_reason', 'distinguished', 'edited', 'gilded', 'is_submitter', 'parent_id', 'permalink', 'retrieved_on', 'stickied', 'subreddit', 'subreddit_id', 'subreddit_type', '_c0', '_c1', '_c2']
    df_labeled = comment_label_join.drop(*columns_to_drop)
    df_labeled = df_labeled.withColumnRenamed('_c3', 'label_djt')
    df_labeled = df_labeled.withColumn('body_sanitized', sanitize_udf(df_labeled.body))
    df_labeled = df_labeled.withColumn('ngrams_combined', combineNgrams_udf(df_labeled.body_sanitized))
    df_labeled = df_labeled.withColumn('poslabel', when(col("label_djt") == 1, 1).otherwise(0))
    df_labeled = df_labeled.withColumn('neglabel', when(col("label_djt") == -1, 1).otherwise(0))
    df_labeled = df_labeled.drop('body_sanitized')
    df_labeled = df_labeled.withColumn('link_id_cleaned',remove_t3_from_link_udf(df_labeled.link_id))
    df_labeled = df_labeled.drop('link_id')
    df_labeled = df_labeled.withColumnRenamed('id','comment_id')
    df_with_post_title = df_labeled.selectExpr('author_flair_text as state','comment_id','body','created_utc','label_djt','ngrams_combined','poslabel','neglabel','link_id_cleaned').join(submissions_df.selectExpr('title','id'), df_labeled.link_id_cleaned == submissions_df.id)
    df_with_post_title = df_with_post_title.drop('id')
    df_with_post_title.write.parquet("df_label.parquet")

    #comment_columns_to_drop = ['author', 'link_id', 'author_cakeday', 'controversiality', 'author_flair_css_class', 'can_gild', 'can_mod_post', 'collapsed', 'collapsed_reason', 'distinguished', 'edited', 'gilded', 'is_submitter', 'parent_id', 'permalink', 'retrieved_on', 'stickied', 'subreddit', 'subreddit_id', 'subreddit_type']
    #comments_df = comments_df.withColumn('link_id_cleaned',remove_t3_from_link_udf(comments_df.link_id))
    #comments_df = comments_df.drop(*comment_columns_to_drop)
    #comments_df = comments_df
    #comments_df = comments_df.filter("body not like '%/s%'")
    #comments_df = comments_df.filter("body not like '&gt;%'")

    #cv = CountVectorizer(inputCol="ngrams_combined", binary=True, outputCol="features", minDF=10.0)
    #cv_model = cv.fit(df_with_post_title)
   
    #output = cv_model.transform(df_with_post_title)
    
    #output = cv_model.transform(comments_df)
    #output.show(2)
    
    # load LR models
    #pos_model = PipelineModel.load(pos_model)
    #neg_model = PipelineModel.load(neg_model)


if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)
