package com.hackrrank.spark.model

final case class Eligibility(memberId: String, firstName: String, lastName: String) {
  override def toString: String = "Eligibility[" + memberId + "," + firstName + "," + lastName + "]"
}
