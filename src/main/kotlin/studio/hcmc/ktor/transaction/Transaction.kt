package studio.hcmc.ktor.transaction

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.withLock
import org.jetbrains.exposed.sql.Transaction
import java.util.*

public suspend fun Transaction.awaitTransactionState(
    transactionId: UUID,
    delayMillis: Long = 100
) {
    var state: TransactionState? = null
    while (state == null) {
        state = transactionLock.withLock { transactions.remove(transactionId) }
        if (state == null) {
            delay(delayMillis)
        }
    }

    when (state) {
        TransactionState.Commit -> commit()
        TransactionState.Rollback -> rollback()
    }
}