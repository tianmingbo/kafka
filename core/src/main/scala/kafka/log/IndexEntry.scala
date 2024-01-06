package kafka.log

import org.apache.kafka.common.requests.ListOffsetsResponse

sealed trait IndexEntry {
  // We always use Long for both key and value to avoid boxing.
  def indexKey: Long
  def indexValue: Long
}

/**
 * The mapping between a logical log offset and the physical position
 * in some log file of the beginning of the message set entry with the
 * given offset.
 */
case class OffsetPosition(offset: Long, position: Int) extends IndexEntry {
  override def indexKey = offset
  override def indexValue = position.toLong
}


/**
 * The mapping between a timestamp to a message offset. The entry means that any message whose timestamp is greater
 * than that timestamp must be at or after that offset.
 * @param timestamp The max timestamp before the given offset.
 * @param offset The message offset.
 */
case class TimestampOffset(timestamp: Long, offset: Long) extends IndexEntry {
  override def indexKey = timestamp
  override def indexValue = offset
}

object TimestampOffset {
  val Unknown = TimestampOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, ListOffsetsResponse.UNKNOWN_OFFSET)
}
