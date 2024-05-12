package org.spark_heuristics
package analyzer

import java.io.File
import scala.io.Source

private[analyzer] case class Config(parameters: Map[String, String])

object Config {
  def apply(): Config = new Config(Map.empty)

  def apply(file: File): Config = {
    val config = Source.fromFile(file)
    try apply(config.mkString)
    finally config.close()
  }

  def apply(config: String): Config = apply(parse(config.mkString))

  def apply(parameters: Map[String, String]): Config = new Config(parameters)

  private def parse(config: String): Map[String, String] = ???
}
