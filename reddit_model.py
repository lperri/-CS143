from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType
import re
import string
import json

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
    ngrams_combined.append(bigrams.split(" "))
    ngrams_combined.append(trigrams.split(" "))
    return ngrams_combined

sanitize_udf = udf(lambda y: sanitize(y), ArrayType(StringType()))
combineNgrams_udf = udf(lambda x: combineNgrams(x), ArrayType(StringType()))

def main(context):
    """Main function takes a Spark SQL context."""
    comments_df = context.read.parquet("comments.parquet")
    submissions_df = context.read.parquet("submissions.parquet")
    labeled_data_df = context.read.parquet("labeled_data.parquet")
    comment_label_join = comments_df.join(labeled_data_df, comments_df.id == labeled_data_df._c0)
    columns_to_drop = ['author', 'author_cakeday', 'controversiality', 'author_flair_css_class', 'can_gild', 'can_mod_post', 'collapsed', 'collapsed_reason', 'distinguished', 'edited', 'gilded', 'is_submitter', 'link_id', 'parent_id', 'permalink', 'retrieved_on', 'stickied', 'subreddit', 'subreddit_id', 'subreddit_type', '_c0']
    comment_label_df = comment_label_join.drop(*columns_to_drop)
    comment_label_df = comment_label_df.withColumnRenamed("_c1", "label_dem")
    comment_label_df = comment_label_df.withColumnRenamed('_c2', 'label_gop')
    comment_label_df = comment_label_df.withColumnRenamed('_c3', 'label_djt')
    comment_label_df = comment_label_df.withColumn('body_sanitized', sanitize_udf(comment_label_df.body))
    comment_label_df = comment_label_df.withColumn('ngrams_combined', combineNgrams_udf(comment_label_df.body_sanitized))
    comment_label_df.show(20,False)


if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)
