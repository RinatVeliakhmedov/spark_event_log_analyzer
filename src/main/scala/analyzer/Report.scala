package org.spark_heuristics
package analyzer

import analyzer.Category.Category

case class Report(category: Category, summary: String, details: String) {

  override def toString: String = {
    s"[${category.toString}] $summary: $details"
  }

}
