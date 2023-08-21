package studio.hcmc.ktor.transaction

import io.ktor.utils.io.errors.*
import kotlinx.serialization.Serializable
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

sealed interface TransactionState {
    @Serializable
    data object Commit : TransactionState {
        const val byte = 0.toByte()
    }

    @Serializable
    data object Rollback : TransactionState {
        const val byte = 1.toByte()
    }

    data object RollbackException : IOException() {
        private fun readResolve(): Any = RollbackException
    }

    object KafkaSerializer : Serializer<TransactionState> {
        override fun serialize(topic: String?, data: TransactionState?) = when (data) {
            Commit -> byteArrayOf(Commit.byte)
            Rollback -> byteArrayOf(Rollback.byte)
            null -> byteArrayOf()
        }
    }

    object KafkaDeserializer : Deserializer<TransactionState> {
        override fun deserialize(topic: String?, data: ByteArray?) = when (data?.firstOrNull()) {
            Commit.byte -> Commit
            Rollback.byte -> Rollback
            else -> null
        }
    }
}