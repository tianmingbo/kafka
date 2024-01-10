package kafka.server

import kafka.log.Log
import org.apache.kafka.common.KafkaException

object LogOffsetMetadata {
  val UnknownOffsetMetadata = LogOffsetMetadata(-1, 0, 0)
  val UnknownFilePosition = -1

  class OffsetOrdering extends Ordering[LogOffsetMetadata] {
    override def compare(x: LogOffsetMetadata, y: LogOffsetMetadata): Int = {
      x.offsetDiff(y).toInt
    }
  }

}

/**
 * A log offset structure, including:
 * 1. messageOffset 消息位移值,其实就是高水位值
 * 2. segmentBaseOffset 保存该位移值所在日志段的起始位移
 * 3. relativePositionInSegment 保存该位移值所在日志段的物理磁盘位置
 */
case class LogOffsetMetadata(messageOffset: Long,
                             segmentBaseOffset: Long = Log.UnknownOffset,
                             relativePositionInSegment: Int = LogOffsetMetadata.UnknownFilePosition) {

  // 与给定的偏移量相比，检查此偏移量是否已经位于较旧的日志段上
  def onOlderSegment(that: LogOffsetMetadata): Boolean = {
    if (messageOffsetOnly)
      throw new KafkaException(s"$this 无法段信息与 $that 进行比较，因为它只有消息偏移量信息")

    this.segmentBaseOffset < that.segmentBaseOffset
  }

  // 用来判断给定的两个 LogOffsetMetadata 对象是否处于同一个日志段的。
  def onSameSegment(that: LogOffsetMetadata): Boolean = {
    if (messageOffsetOnly)
      throw new KafkaException(s"$this 无法段信息与 $that 进行比较，因为它只有消息偏移量信息")

    this.segmentBaseOffset == that.segmentBaseOffset
  }

  // 计算此偏移量与给定偏移量之间的消息数
  def offsetDiff(that: LogOffsetMetadata): Long = {
    this.messageOffset - that.messageOffset
  }

  // 计算此偏移量与给定偏移量之间的字节数（如果它们位于同一段上并且此偏移量先于给定偏移量）
  def positionDiff(that: LogOffsetMetadata): Int = {
    if (!onSameSegment(that))
      throw new KafkaException(s"$this 无法将其段位置与 $that 进行比较，因为它们不在同一段上")
    if (messageOffsetOnly)
      throw new KafkaException(s"$this 无法将其段位置与 $that 进行比较，因为它只有消息偏移量信息")

    this.relativePositionInSegment - that.relativePositionInSegment
  }

  // 判断元数据是否只包含messageOffset
  def messageOffsetOnly: Boolean = {
    segmentBaseOffset == Log.UnknownOffset && relativePositionInSegment == LogOffsetMetadata.UnknownFilePosition
  }

  override def toString = s"(offset=$messageOffset segment=[$segmentBaseOffset:$relativePositionInSegment])"

}
