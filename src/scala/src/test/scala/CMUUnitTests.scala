package edu.project1.tests

import org.scalatest.funsuite.AnyFunSuite

import java.text.SimpleDateFormat
import java.util.TimeZone

/**
 * CMUUnitTests -- Phase 20 (Testing / QA)
 * 
 * Validates the core parsing, conversion, and labelling logic used 
 * throughout the distributed analytics platform.
 */
class CMUUnitTests extends AnyFunSuite {

  // =========================================================================
  // 1. Log Line Parsing Regex Extraction Test
  // =========================================================================
  test("HDFS Log Regex correctly extracts timestamp, level, and message") {
    val LOG_PATTERN = """^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+(\w+)\s+([\w\.]+):\s+(.*)$""".r
    val sampleLogLine = "2015-10-18 18:05:43,491 INFO org.apache.hadoop.hdfs.server.datanode.DataNode: Receiving BP-73413533-10.10.10.10-1432103417637"

    sampleLogLine match {
      case LOG_PATTERN(timestamp, level, component, message) =>
        assert(timestamp == "2015-10-18 18:05:43,491")
        assert(level == "INFO")
        assert(component == "org.apache.hadoop.hdfs.server.datanode.DataNode")
        assert(message.startsWith("Receiving BP"))
      case _ => fail("Regex failed to match the standard HDFS log line format.")
    }
  }

  // =========================================================================
  // 2. Timestamp string to unix epoch converter test
  // =========================================================================
  test("Timestamp converter successfully parses to epoch milliseconds") {
    val timestampStr = "2010-05-09 23:59:59"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC")) // Ensure timezone neutrality
    
    val parsedDate = sdf.parse(timestampStr)
    val epochMs = parsedDate.getTime
    
    assert(epochMs == 1273449599000L) // Known epoch for this date in UTC
  }

  // =========================================================================
  // 3. Feature label logic test (Exclude KILLED, FAIL=1, SUCCESS=0)
  // =========================================================================
  test("Job status labelling logic correctly identifies failures and skips KILLED") {
    def encodeLabel(status: String): Option[Int] = {
      status match {
        case "SUCCESS" => Some(0)
        case "FAILED"  => Some(1)
        case "KILLED"  => None  // Exclude from dataset
        case _         => None
      }
    }

    assert(encodeLabel("SUCCESS").contains(0))
    assert(encodeLabel("FAILED").contains(1))
    assert(encodeLabel("KILLED").isEmpty)
    assert(encodeLabel("UNKNOWN").isEmpty)
  }
}
