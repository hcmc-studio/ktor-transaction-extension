package studio.hcmc.ktor.transaction

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.withLock
import org.jetbrains.exposed.sql.Transaction
import java.util.*
import kotlin.collections.ArrayList

public suspend fun Transaction.awaitTransactionState(
    transactionId: UUID
) {
    ArrayList
    val channel = transactionLock.withLock {
        val present = transactions[transactionId]
        if (present == null) {
            val channel = Channel<TransactionState>()
            transactions[transactionId] = 1 to channel

            channel
        } else {
            val (count, channel) = present
            transactions[transactionId] = (count + 1) to channel

            channel
        }
    }

    when (channel.receive()) {
        TransactionState.Commit -> commit()
        TransactionState.Rollback -> rollback()
    }
}