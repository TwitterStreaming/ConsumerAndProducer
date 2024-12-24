package org.bigdata

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CleanText(spark: SparkSession) {
  private val cleanTextUDF = udf((text: String) => {
    if (text == null) ""
    else {
      text
        .replaceAll("\\b(RT)\\b", "")
        .toLowerCase()
        .replaceAll("@[\\w_]+", "")
        .replaceAll("http\\S+|www\\.\\S+", "")
        .replaceAll("\\b(can't)\\b", "can not")
        .replaceAll("\\b(don't)\\b", "do not")
        .replaceAll("\\b(doesn't)\\b", "does not")
        .replaceAll("\\b(didn't)\\b", "did not")
        .replaceAll("\\b(aren't)\\b", "are not")
        .replaceAll("\\b(isn't)\\b", "is not")
        .replaceAll("\\b(wasn't)\\b", "was not")
        .replaceAll("\\b(weren't)\\b", "were not")
        .replaceAll("\\b(won't)\\b", "will not")
        .replaceAll("\\b(wouldn't)\\b", "would not")
        .replaceAll("\\b(shouldn't)\\b", "should not")
        .replaceAll("\\b(mightn't)\\b", "might not")
        .replaceAll("\\b(mustn't)\\b", "must not")
        .replaceAll("\\b(haven't)\\b", "have not")
        .replaceAll("\\b(hasn't)\\b", "has not")
        .replaceAll("\\b(hadn't)\\b", "had not")
        .replaceAll("\\b(couldn't)\\b", "could not")
        .replaceAll("\\b(i'd|i would)\\b", "i would")
        .replaceAll("\\b(I'll|we'll|you'll|he'll|she'll|it'll|they'll)\\b", "$1 will")
        .replaceAll("\\b(i'm)\\b", "i am")
        .replaceAll("\\b(you're|they're|we're)\\b", "$1 are")
        .replaceAll("\\b(he's|she's|it's|that's|how's|where's|what's)\\b", "$1 is")
        .replaceAll("\\b(\\w+)'s\\b", "$1s")
        .replaceAll("\\b(ll|s|re)\\b", "")
        .replaceAll("\\b(ain't)\\b", "is not")
        .replaceAll("\\s?[0-9]+", "")
        .replaceAll("&gt;", "greater than")
        .replaceAll("&lt;", "less than")
        .replaceAll("&eq;", "equal")
        .replaceAll("(\\p{Punct})\\1+", "$1")
        .replaceAll("[\\p{So}\\p{Cn}]", "")
        .replaceAll("[^\\w\\s]|_", " ")
        .replaceAll("[<>:;~][-']?[(DP]", "")
        .replaceAll("\\s+", " ")
        .trim
    }
  })

  def clean(df: DataFrame, columnName: String, newColumnName: String): DataFrame = {
    df.withColumn(newColumnName, cleanTextUDF(col(columnName)))
  }
}