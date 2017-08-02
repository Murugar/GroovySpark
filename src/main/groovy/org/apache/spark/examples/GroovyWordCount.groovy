package org.apache.spark.examples

import java.util.regex.Pattern

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.function.Function2
import org.apache.spark.api.java.function.PairFunction
import scala.Tuple2

public final class GroovyWordCount {

  private static final Pattern SPACE = Pattern.compile(" ")

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      println("Usage: GroovyWordCount <file>")
      System.exit(1)
    }

    def sparkConf = new SparkConf().setAppName("GroovyWordCount")
    def ctx = new JavaSparkContext(sparkConf)
    def lines = ctx.textFile(args[0], 1)

    def words = lines.flatMap( { Arrays.asList(SPACE.split(it)).iterator() } as FlatMapFunction<String, String> )

    def ones = words.mapToPair({ new Tuple2<String, Integer>(it, 1) } as PairFunction<String, String, Integer>)

    def counts = ones.reduceByKey({ Integer i1, Integer i2 -> i1 + i2 } as Function2<Integer, Integer, Integer>)

    counts.collect().each { tuple ->
        println("${tuple._1()}: ${tuple._2()}")
    }
    ctx.stop();
  }
}
