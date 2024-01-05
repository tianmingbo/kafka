package kafka.server

import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.Records

sealed trait FetchIsolation

case object FetchLogEnd extends FetchIsolation

case object FetchHighWatermark extends FetchIsolation

case object FetchTxnCommitted extends FetchIsolation

case class FetchDataInfo(fetchOffsetMetadata: LogOffsetMetadata,
                         records: Records,
                         firstEntryIncomplete: Boolean = false,
                         abortedTransactions: Option[List[FetchResponseData.AbortedTransaction]] = None)
