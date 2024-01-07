package kafka.log

import java.io.File
import java.nio.ByteBuffer

import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.errors.InvalidOffsetException

/**
 * 保存 < 位移值,文件磁盘物理位置 >
 * 将偏移量映射到特定日志段的物理文件位置的索引。 该索引可能是稀疏的：也就是说，它可能无法保存日志中所有消息的条目。
 *
 * 索引存储在一个预先分配的文件中，用于保存固定的最大数量的 8 字节条目。
 *
 * 该索引支持针对该文件的内存映射进行查找。 这些查找是使用简单的二分搜索变体来完成的，以找到小于或等于目标偏移的最大偏移的偏移/位置对。
 *
 * 索引文件可以通过两种方式打开：作为允许追加的空的可变索引或之前已填充的不可变只读索引文件。 makeReadOnly 方法会将可变文件转换为不可变文件并截断任何额外的字节。 这是在索引文件翻转时完成的。
 *
 * 不会尝试对此文件的内容进行校验和，如果发生崩溃，则会重建该文件。
 *
 * 文件格式是一系列条目。格式为相对位移(4B),文件物理位置(4B). 存储的偏移量是相对于索引文件的基本偏移量的。 如果基本偏移量为 50，则偏移量 55 将存储为 5。以这种方式使用相对偏移量，让我们仅使用 4 个字节作为偏移量。
 */
// Avoid shadowing mutable `file` in AbstractIndex
class OffsetIndex(_file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true)
  extends AbstractIndex(_file, baseOffset, maxIndexSize, writable) {

  import OffsetIndex._

  override def entrySize = 8

  /* 保存最后一个索引项的offset */
  private[this] var _lastOffset = lastEntry.offset

  debug(s"Loaded index file ${file.getAbsolutePath} with maxEntries = $maxEntries, " +
    s"maxIndexSize = $maxIndexSize, entries = ${_entries}, lastOffset = ${_lastOffset}, file position = ${mmap.position()}")

  /**
   * The last entry in the index
   */
  private def lastEntry: OffsetPosition = {
    inLock(lock) {
      _entries match {
        case 0 => OffsetPosition(baseOffset, 0)
        case s => parseEntry(mmap, s - 1)
      }
    }
  }

  def lastOffset: Long = _lastOffset

  /**
   * 找到小于或等于给定targetOffset的最大偏移量，并返回保存该偏移量及其对应的物理文件位置的对。
   *
   * @param targetOffset 要查找的偏移量。
   * @return 找到的偏移量以及该偏移量对应的文件位置
   *         如果目标偏移量小于索引中的最小条目（或者索引为空），
   *         返回 (baseOffset, 0) 对。
   */
  def lookup(targetOffset: Long): OffsetPosition = {
    //win需要加锁
    maybeLock(lock) {
      val idx = mmap.duplicate //创建一个副本
      val slot = largestLowerBoundSlotFor(idx, targetOffset, IndexSearchType.KEY)
      if (slot == -1)
        OffsetPosition(baseOffset, 0)
      else
        parseEntry(idx, slot)
    }
  }

  /**
   * Find an upper bound offset for the given fetch starting position and size. This is an offset which
   * is guaranteed to be outside the fetched range, but note that it will not generally be the smallest
   * such offset.
   */
  def fetchUpperBoundOffset(fetchOffset: OffsetPosition, fetchSize: Int): Option[OffsetPosition] = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = smallestUpperBoundSlotFor(idx, fetchOffset.position + fetchSize, IndexSearchType.VALUE)
      if (slot == -1)
        None
      else
        Some(parseEntry(idx, slot))
    }
  }

  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize)

  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 4)

  override protected def parseEntry(buffer: ByteBuffer, n: Int): OffsetPosition = {
    //baseOffset + relativeOffset(buffer, n)是绝对位移值, physical(buffer, n)是物理磁盘位置
    OffsetPosition(baseOffset + relativeOffset(buffer, n), physical(buffer, n))
  }

  /**
   * Get the nth offset mapping from the index
   *
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   */
  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if (n >= _entries)
        throw new IllegalArgumentException(s"Attempt to fetch the ${n}th entry from index ${file.getAbsolutePath}, " +
          s"which has size ${_entries}.")
      parseEntry(mmap, n)
    }
  }

  /**
   * 将给定 偏移,位置 添加到索引
   *
   * @throws IndexOffsetOverflowException if the offset causes index offset to overflow
   */
  def append(offset: Long, position: Int): Unit = {
    inLock(lock) {
      //1.判断索引文件是否已满
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").")
      //2.满足以下条件时，将索引项添加到索引文件中：
      //  2.1:当前索引文件为空
      //  2.2:要写入的位移大于当前所有已写入的索引项的位移
      if (_entries == 0 || offset > _lastOffset) {
        trace(s"Adding index entry $offset => $position to ${file.getAbsolutePath}")
        mmap.putInt(relativeOffset(offset)) //3.1.写入相对偏移
        mmap.putInt(position) //3.2写入物理位置
        /*
        4.更新其它元数据信息
          当前索引计数器_entries +1
          最后一个索引项的offset更新为当前索引项的offset
        * */
        _entries += 1
        _lastOffset = offset
        //5.确保索引文件大小与索引条目数量相匹配,否则说明文件已损坏
        require(_entries * entrySize == mmap.position(), s"$entries entries but file position in index is ${mmap.position()}.")
      } else {
        throw new InvalidOffsetException(s"Attempt to append an offset ($offset) to position $entries no larger than" +
          s" the last offset appended (${_lastOffset}) to ${file.getAbsolutePath}.")
      }
    }
  }

  override def truncate() = truncateToEntries(0)

  override def truncateTo(offset: Long): Unit = {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.KEY)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries =
        if (slot < 0)
          0
        else if (relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  /**
   * 截断索引为给定条目数
   *
   * @param entries 要截断的条目数
   */
  private def truncateToEntries(entries: Int): Unit = {
    inLock(lock) {
      _entries = entries
      mmap.position(_entries * entrySize)
      _lastOffset = lastEntry.offset //更新最后一个索引项的offset
      debug(s"Truncated index ${file.getAbsolutePath} to $entries entries;" +
        s" position is now ${mmap.position()} and last offset is now ${_lastOffset}")
    }
  }

  //完整性检查
  override def sanityCheck(): Unit = {
    if (_entries != 0 && _lastOffset < baseOffset)
      throw new CorruptIndexException(s"Corrupt index found, index file (${file.getAbsolutePath}) has non-zero size " +
        s"but the last offset is ${_lastOffset} which is less than the base offset $baseOffset.")
    if (length % entrySize != 0)
      throw new CorruptIndexException(s"Index file ${file.getAbsolutePath} is corrupt, found $length bytes which is " +
        s"neither positive nor a multiple of $entrySize.")
  }

}

object OffsetIndex extends Logging {
  override val loggerName: String = classOf[OffsetIndex].getName
}
