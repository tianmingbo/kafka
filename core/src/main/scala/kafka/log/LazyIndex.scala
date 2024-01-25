package kafka.log

import java.io.File
import java.nio.file.{Files, NoSuchFileException}
import java.util.concurrent.locks.ReentrantLock

import LazyIndex._
import kafka.utils.CoreUtils.inLock
import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Utils

/**
 * “AbstractIndex”实例的包装器，提供了一种延迟加载（即内存映射）底层索引的机制，直到通过“get”方法第一次访问它为止。
 *
 * 此外，此类公开了许多方法（例如 updateParentDir、renameTo、close 等），这些方法提供所需的行为而不会导致索引被加载。 如果索引先前已加载，则此类中的方法只需委托给索引中的相关方法。
 *
 * 如果有大量段，这对于代理启动和关闭时间来说是一个重要的优化。
 *
 * 该类的方法是线程安全的。 请务必检查“AbstractIndex”子类文档以建立其线程安全性。
 *
 * @param loadIndex 一个函数，它接受一个指向索引的“File”并返回一个加载的“AbstractIndex”实例。
 */
@threadsafe
class LazyIndex[T <: AbstractIndex] private(@volatile private var indexWrapper: IndexWrapper, loadIndex: File => T) {

  private val lock = new ReentrantLock()

  def file: File = indexWrapper.file

  def get: T = {
    indexWrapper match {
      case indexValue: IndexValue[T] => indexValue.index
      case _: IndexFile =>
        inLock(lock) {
          indexWrapper match {
            case indexValue: IndexValue[T] => indexValue.index
            case indexFile: IndexFile =>
              val indexValue = new IndexValue(loadIndex(indexFile.file))
              indexWrapper = indexValue
              indexValue.index
          }
        }
    }
  }

  def updateParentDir(parentDir: File): Unit = {
    inLock(lock) {
      indexWrapper.updateParentDir(parentDir)
    }
  }

  def renameTo(f: File): Unit = {
    inLock(lock) {
      indexWrapper.renameTo(f)
    }
  }

  def deleteIfExists(): Boolean = {
    inLock(lock) {
      indexWrapper.deleteIfExists()
    }
  }

  def close(): Unit = {
    inLock(lock) {
      indexWrapper.close()
    }
  }

  def closeHandler(): Unit = {
    inLock(lock) {
      indexWrapper.closeHandler()
    }
  }

}

object LazyIndex {

  def forOffset(file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true): LazyIndex[OffsetIndex] =
    new LazyIndex(new IndexFile(file), file => new OffsetIndex(file, baseOffset, maxIndexSize, writable))

  def forTime(file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true): LazyIndex[TimeIndex] =
    new LazyIndex(new IndexFile(file), file => new TimeIndex(file, baseOffset, maxIndexSize, writable))

  private sealed trait IndexWrapper {

    def file: File

    def updateParentDir(f: File): Unit

    def renameTo(f: File): Unit

    def deleteIfExists(): Boolean

    def close(): Unit

    def closeHandler(): Unit

  }

  private class IndexFile(@volatile private var _file: File) extends IndexWrapper {

    def file: File = _file

    def updateParentDir(parentDir: File): Unit = _file = new File(parentDir, file.getName)

    def renameTo(f: File): Unit = {
      try Utils.atomicMoveWithFallback(file.toPath, f.toPath, false)
      catch {
        case _: NoSuchFileException if !file.exists => ()
      }
      finally _file = f
    }

    def deleteIfExists(): Boolean = Files.deleteIfExists(file.toPath)

    def close(): Unit = ()

    def closeHandler(): Unit = ()

  }

  private class IndexValue[T <: AbstractIndex](val index: T) extends IndexWrapper {

    def file: File = index.file

    def updateParentDir(parentDir: File): Unit = index.updateParentDir(parentDir)

    def renameTo(f: File): Unit = index.renameTo(f)

    def deleteIfExists(): Boolean = index.deleteIfExists()

    def close(): Unit = index.close()

    def closeHandler(): Unit = index.closeHandler()

  }

}

