package studio.hcmc.ktor.transaction

import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.resources.post
import io.ktor.server.routing.*
import io.ktor.util.pipeline.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.sql.Transaction
import studio.hcmc.exposed.transaction.Transaction
import studio.hcmc.exposed.transaction.suspendedTransaction
import studio.hcmc.kotlin.protocol.io.ErrorDataTransferObject
import java.util.*

val invalidTransactionId = object : ErrorDataTransferObject() {
    override val httpStatusCode = 400
}

val ApplicationRequest.transactionId: UUID get() = runCatching {
    UUID.fromString(header(call.application.attributes[transactionIdHeaderNameKey]))
}.getOrElse {
    throw invalidTransactionId
}

inline fun <reified T : Any> Route.postTransaction(
    noinline body: suspend PipelineContext<Unit, ApplicationCall>.(T, Transaction) -> Unit
): Route {
    return post<T> { resource ->
        val transactionId = call.request.transactionId
        withContext(Dispatchers.Transaction) {
            suspendedTransaction(context = coroutineContext) {
                body(resource, this)
                awaitTransactionState(transactionId)
            }
        }
    }
}

inline fun <reified T : Any, reified R : Any> Route.postTransaction(
    noinline body: suspend PipelineContext<Unit, ApplicationCall>.(T, R, Transaction) -> Unit
): Route {
    return postTransaction<T> { resource, transaction ->
        body(resource, call.receive(), transaction)
    }
}