package ru.souz.backend.memory.hindsight

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import ru.souz.backend.storage.postgres.PostgresHistoryMemoryRepository

internal const val HISTORY_MEMORY_MAX_ATTEMPTS = 12

/** A history message this close in time to an identical message of a completed turn is context only. */
internal const val HISTORY_MEMORY_TURN_COPY_WINDOW_HOURS = 24

internal class HistoryMemoryWorker(
    private val repository: PostgresHistoryMemoryRepository,
    private val memory: HindsightConversationMemoryRuntime,
) {
    private val logger = LoggerFactory.getLogger(HistoryMemoryWorker::class.java)

    fun start(scope: CoroutineScope) = scope.launch {
        while (isActive) {
            try {
                if (processNext()) continue
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Exception) {
                logger.warn("History memory poll failed category={}", error.javaClass.simpleName)
            }
            delay(5_000)
        }
    }

    internal suspend fun processNext(): Boolean {
        val fragment = repository.claim() ?: return false
        try {
            coroutineScope {
                val heartbeat = launch {
                    while (isActive) {
                        delay(30_000)
                        check(repository.renew(fragment)) { "History memory lease lost" }
                    }
                }
                try {
                    val documents = repository.documents(fragment)
                    if (documents.isNotEmpty()) memory.captureHistory(fragment.userId, fragment.chatId, documents)
                    check(repository.complete(fragment)) { "History memory lease lost" }
                    logger.info("History memory captured chatId={} fragmentId={} attempt={} documents={}",
                        fragment.chatId, fragment.id, fragment.attempts, documents.size)
                } finally {
                    heartbeat.cancelAndJoin()
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            val category = if (error is HindsightHttpFailure) "http_${error.statusCode}" else error.javaClass.simpleName
            if (fragment.attempts >= HISTORY_MEMORY_MAX_ATTEMPTS) {
                repository.fail(fragment)
                logger.error("History memory failed chatId={} fragmentId={} attempts={} category={}",
                    fragment.chatId, fragment.id, fragment.attempts, category)
            } else {
                repository.retry(fragment)
                logger.warn("History memory retry chatId={} fragmentId={} attempt={} category={}",
                    fragment.chatId, fragment.id, fragment.attempts, category)
            }
        }
        return true
    }
}
