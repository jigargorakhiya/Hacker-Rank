package com.hackrrank.spark

import com.hackrrank.spark.job.DataCleaningJob._
import com.hackrrank.spark.model.{Eligibility, Medical}
import org.apache.spark.sql.Dataset

object AppMain {
  val eligibilityPath = "src/main/resources/spark/eligibility.csv"
  val medicalPath = "src/main/resources/spark/medical.csv"

  def main(args: Array[String]): Unit = {
    println("<<Reading>>")
    val eligibilityDs: Dataset[Eligibility] = readEligibility(eligibilityPath)
    val medicalDs: Dataset[Medical] = readMedical(medicalPath)

    println("<<Filter>>")
    val filteredMedicalDs = filterMedical(eligibilityDs, medicalDs)

    println("<<Full Name>>")
    val faultyPlantsDS = generateFullName(eligibilityDs, filteredMedicalDs)

    println("<<Max Paid Member>>")
    val maxPaidMemberId = findMaxPaidMember(filteredMedicalDs)

    println("<<Full Name>>")
    val totalPaidAmount = findTotalPaidAmount(filteredMedicalDs)

    //stop context
    stop()
  }

}
