package com.cellariot.spark
// Code removes "you", "to", "your", "the", "a", "of", "and" from list of common words
// Added improvement : Considered only words whose len are > 2
// Publishes all other common words
// H Hurchand
// 06 March 2020

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSorted {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "WordCountBetterSorted")   
    
    // Load each line of my book into an RDD
    val input = sc.textFile("../book.txt")
    
    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val list = List("you", "to", "your", "the", "a", "of", "and") 
    val wordCounts0 = lowercaseWords.filter(x=> list.contains(x)==false && x.length()>2)
    val wordCounts = wordCounts0.map(x=>(x,1))
    val wordCounts_small = wordCounts.reduceByKey( (x,y) => x + y )
    
    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts_small.map( x => (x._2, x._1) ).sortByKey(false)
   
    // Print the results, flipping the (count, word) results to word: count as we go.
     for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
    
  }
  
}

