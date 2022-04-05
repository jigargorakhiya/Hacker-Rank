package com.hackrrank.spark

import java.io.{File, PrintWriter}

import com.hackrrank.spark.job.DataCleaningJob
import com.hackrrank.spark.job.DataCleaningJob._
import org.junit.Test

import scala.reflect.io.Directory

class ApplicationTest {
  val INPUT_BASE = "src/main/resources/spark/test"
  val random = new scala.util.Random

  val setup = {
    val directory = new Directory(new File(INPUT_BASE))
    if (directory.exists) {
      directory.deleteRecursively()
    }
    directory.createDirectory()
  }

  @Test
  def testSc(): Unit = {
    assert(initJob(), " -- spark session not implemented")
  }

  private def initJob(): Boolean =
    try {
      DataCleaningJob
      true
    } catch {
      case ex: Throwable =>
        println(ex)
        false
    }

  @Test
  def testFilter(): Unit = {
    val eligibilityPath = INPUT_BASE + "/test_eligibility_filter" + random.nextInt()
    val medicalPath = INPUT_BASE + "/test_medical_filter" + random.nextInt()

    val pw = new PrintWriter(new File(eligibilityPath))
    pw.write("#memberId,firstName,lastName")
    pw.write(System.lineSeparator())
    pw.write("101,Fizz,Buzz")
    pw.write(System.lineSeparator())
    pw.write("103,John,Sena")
    pw.close

    val pw1 = new PrintWriter(new File(medicalPath))
    pw1.write("#memberId,fullName,paidAmount")
    pw1.write(System.lineSeparator())
    pw1.write("101,,20")
    pw1.write(System.lineSeparator())
    pw1.write("105,,60")
    pw1.write(System.lineSeparator())
    pw1.write("103,,90")
    pw1.close

    val eligibilityDs = readEligibility(eligibilityPath)
    val medicalDs = readMedical(medicalPath)

    val filteredMedicalDs = filterMedical(eligibilityDs, medicalDs)
    //filteredMedicalDs.show()
    val medicalList = filteredMedicalDs.collect().sortBy(a => a.memberId)

    assert(2 == medicalList.length)
    assert("101".equals(medicalList(0).memberId))
    assert("103".equals(medicalList(1).memberId))
  }

  @Test
  def testFullName(): Unit = {
    val eligibilityPath = INPUT_BASE + "/test_eligibility_name" + random.nextInt()
    val medicalPath = INPUT_BASE + "/test_medical_name" + random.nextInt()

    val pw = new PrintWriter(new File(eligibilityPath))
    pw.write("#memberId,firstName,lastName")
    pw.write(System.lineSeparator())
    pw.write("101,Fizz,Buzz")
    pw.write(System.lineSeparator())
    pw.write("103,John,Sena")
    pw.close

    val pw1 = new PrintWriter(new File(medicalPath))
    pw1.write("#memberId,fullName,paidAmount")
    pw1.write(System.lineSeparator())
    pw1.write("101,,20")
    pw1.write(System.lineSeparator())
    pw1.write("105,,60")
    pw1.write(System.lineSeparator())
    pw1.write("103,,90")
    pw1.close

    val eligibilityDs = readEligibility(eligibilityPath)
    val medicalDs = readMedical(medicalPath)

    val filteredMedicalDs = filterMedical(eligibilityDs, medicalDs)
    val fullNameMedicalDs = generateFullName(eligibilityDs, filteredMedicalDs)
    fullNameMedicalDs.show()

    val medicalList = fullNameMedicalDs.collect().sortBy(a => a.memberId)

    assert(2 == medicalList.length)
    assert("101".equals(medicalList(0).memberId) && "Fizz Buzz".equals(medicalList(0).fullName))
    assert("103".equals(medicalList(1).memberId) && "John Sena".equals(medicalList(1).fullName))
  }

  @Test
  def testMaxMember(): Unit = {
    val medicalPath = INPUT_BASE + "/test_medical_max" + random.nextInt()

    val pw1 = new PrintWriter(new File(medicalPath))
    pw1.write("#memberId,fullName,paidAmount")
    pw1.write(System.lineSeparator())
    pw1.write("101,,20")
    pw1.write(System.lineSeparator())
    pw1.write("105,,60")
    pw1.write(System.lineSeparator())
    pw1.write("103,,90")
    pw1.close

    val medicalDs = readMedical(medicalPath)
    val maxMemberId = findMaxPaidMember(medicalDs)

    assert("103".equals(maxMemberId))
  }

  @Test
  def testTotalPaidAmount(): Unit = {
    val medicalPath = INPUT_BASE + "/test_medical_sum" + random.nextInt()

    val pw1 = new PrintWriter(new File(medicalPath))
    pw1.write("#memberId,fullName,paidAmount")
    pw1.write(System.lineSeparator())
    pw1.write("101,,20")
    pw1.write(System.lineSeparator())
    pw1.write("105,,60")
    pw1.write(System.lineSeparator())
    pw1.write("103,,90")
    pw1.close

    val medicalDs = readMedical(medicalPath)
    val total = findTotalPaidAmount(medicalDs)
    assert(170 == total)
  }
}
