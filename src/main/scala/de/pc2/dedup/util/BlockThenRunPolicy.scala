package de.pc2.dedup.util
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
 
class BlockThenRunPolicy
        extends RejectedExecutionHandler {

  override def rejectedExecution(
             task: Runnable,
             executor: ThreadPoolExecutor) {           

            var queue = executor.getQueue()
            var taskSent = false

            while (!taskSent) {
                if (executor.isShutdown()) {
                    throw new RejectedExecutionException(
                        "ThreadPoolExecutor has shutdown while attempting to offer a new task.")
                }

                try {
                    // offer the task to the queue, for a blocking-timeout
                    if (queue.offer(task, 1, TimeUnit.SECONDS)) {
                        taskSent = true;
                    }
                }
                catch {
                  case e: InterruptedException =>
                    // we need to go back to the offer call...
                }

            } // end of while for InterruptedException 

        } // end of method rejectExecution

        // --------------------------------------------------

    } // end of inner private class BlockThenRunPolicy