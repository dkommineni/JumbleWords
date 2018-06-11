package com.ibm.spark.anagrams

import scala.io.Source
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.log4j._
import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/** The jumble puzzle is a common newspaper puzzle, it contains a series of anagrams that must be solved. */
object JumbleWords {

  /**
   * custom class for Jumble
   */
  case class Jumble(scrambled_word: String, circled_letters: List[Long])

  /**
   * custom class for Anagram
   */
  case class Anagram(id: Long, words: List[Jumble], final_anagram_lengths: List[Long])

  /**
   *  Load up a Map of word and frequency.
   */
  def loadWordsDict(): Map[String, Int] = {
    // Create a Map of Ints to Strings, and populate it from u.item.
    var wordsDictMap: Map[String, Int] = Map()
    val lines = Source.fromFile("../freq_dict.json").getLines()
    for (line <- lines) {
      var fields = line.replace("\"", "").replace(",", "").split(':')
      if (fields.length > 1) {
        wordsDictMap += (fields(0).trim().toLowerCase -> fields(1).trim().toInt)
      }
    }
    return wordsDictMap
  }

  /**
   *  for given word, generate all possible words with given length
   *
   */
  def getWordsPermutations(word: String, wordLen: Int): List[String] = {
    return word.combinations(wordLen).flatMap(_.permutations).toList
  }

  /**
   *  algorithm to determine the most likely one
   *  most common english words are scored (1=most frequent, 9887=least frequent, 0=not scored due to infrequency of use)
   *
   */
  def getMostLikelyWord(wordsDictMap: Map[String, Int], possibleWords: List[String], userWordsToSkip: List[String]): String = {
    return possibleWords.flatMap(x => {
      val frequency = wordsDictMap.getOrElse(x, -1)
      if (frequency > -1 && !userWordsToSkip.contains(x)) {
        if (frequency == 0) {
          Some((x, 10000000))
        } else {
          Some((x, frequency))
        }
      } else {
        None
      }
    }).toSeq.sortBy(_._2).toList(0)._1

  }

  /**
   * Loop all jumble words,
   * find most likely word based on word frequency from dictionary
   * note circled letters for each word
   * find most likely words from the circled letters of individual jumble words
   */
  def solveAnagrams(wordsDictMap: Map[String, Int], anagrams: List[Anagram]): List[Tuple3[Int, List[String], List[String]]] = {
    val puzlles = new ListBuffer[Tuple3[Int, List[String], List[String]]]()

    //loop through all anagrams
    for (anagram <- anagrams) {
      var circledLetters: String = ""
      val userWordsToSkip = new ListBuffer[String]()
      val unscrambledWords = new ListBuffer[String]()
      for (word <- anagram.words) {
        //find all permutations for given scrambled word
        val possibleWords = word.scrambled_word.toLowerCase.permutations.toList

        //get most likely word from all permutations
        val mostLikelyWord = getMostLikelyWord(wordsDictMap, possibleWords, userWordsToSkip.toList)

        //add most likely word to list of likely words of Anagram
        unscrambledWords += mostLikelyWord

        //find circled words for each anagram
        for (circledWord <- word.circled_letters) {
          circledLetters += mostLikelyWord(circledWord.toInt)
        }
      }

      //Find final word from anagram circled letters
      val finalWords = new ListBuffer[String]()
      //loop for number of final words length times
      for (wordLength <- anagram.final_anagram_lengths) {
        //find all permutations for given word length
        val possibleWords = getWordsPermutations(circledLetters, wordLength.toInt)

        //get most likely word from all permutations
        val finalWord = getMostLikelyWord(wordsDictMap, possibleWords, userWordsToSkip.toList)

        //in case two or more words has same length, skip the same word from most likely word
        userWordsToSkip += finalWord

        //add most likely word to final words list
        finalWords += finalWord
      }

      puzlles += ((anagram.id.toInt, unscrambledWords.toList, finalWords.toList))
    }

    puzlles.toList
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create SparkSession using every core of the local machine, named JumbleSolver
    val spark = SparkSession
      .builder
      .appName("JumbleSolver")
      .master("local[*]")
      .getOrCreate()

    // Create a broadcast variable of Word -> frequency
    val wordsDictMap = spark.sparkContext.broadcast(loadWordsDict)

    // Read puzzle input
    val puzzleWords = spark.read.json("../puzzle_input.json")
    import spark.implicits._
    val anagrams = puzzleWords.as[Anagram].collectAsList.asScala.toList

    //find most used words and final anagram words from individual anagram
    val puzzles: List[Tuple3[Int, List[String], List[String]]] = solveAnagrams(wordsDictMap.value, anagrams)
    for (puzzle <- puzzles) {
      println("puzzle id: " + puzzle._1)
      println("words: " + puzzle._2.mkString(", "))
      println("final words: " + puzzle._3.mkString(", "))
    }

    spark.stop()
  }
}