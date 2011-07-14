package de.pc2.dedup.util

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

/*
 class SimpleSlidingAverage {
    private:
        int window_size_;

        tbb::spin_mutex mutex_;

        tbb::concurrent_queue<uint64_t> queue_;

        tbb::atomic<uint64_t> sum_;
    public:
        /**
         *
         * @param window_size > 0
         * @return
         */
        explicit SimpleSlidingAverage(int window_size);

        bool Add(uint64_t value);
        double GetAverage();
};
 */

class SimpleSlidingAverage(windowSize: Int) {
  val sum = new AtomicLong()
  val queue = new ConcurrentLinkedQueue[Long]()

  def add(value: Long) {
    this synchronized {
      sum.addAndGet(value)
      if (queue.size > windowSize) {
        val old_value = queue.poll()
        sum.addAndGet(-1 * old_value)
      }
      queue.add(value)
    }
  }

  def getAverage(): Double = {
    this synchronized {
      if (queue.isEmpty) {
        0.0
      } else {
        1.0 * sum.get / queue.size
      }
    }
  }
}