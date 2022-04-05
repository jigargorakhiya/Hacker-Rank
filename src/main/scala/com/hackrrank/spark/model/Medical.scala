package com.hackrrank.spark.model

final case class Medical(memberId: String, fullName: String, paidAmount: Int) {
  override def toString: String = "Medical[" + memberId + "," + fullName + "," + paidAmount + "]"
}
