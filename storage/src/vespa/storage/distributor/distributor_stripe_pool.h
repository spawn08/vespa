// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include <vespa/fastos/thread.h>
#include <vespa/vespalib/util/time.h>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <vector>

namespace storage::distributor {

class DistributorStripeThread;
class TickableStripe;

/**
 * Management and coordination of a pool of distributor stripe threads.
 *
 * Aside from handling the threads themselves, the pool crucially offers a well-defined
 * thread synchronization/coordination API meant for ensuring all stripe threads are in
 * a well defined state before accessing them:
 *
 *   - park_all_threads() returns once ALL threads are in a "parked" state where they
 *     may not race with any operations performed on them by the caller. In essence, this
 *     acts as if a (very large) mutex is held by the caller that prevents the stripe
 *     from doing anything of its own volition. Must be followed by:
 *   - unpark_all_threads() returns once ALL threads have been confirmed released from
 *     a previously parked state. Must be called after park_all_threads().
 *
 * Neither park_all_threads() or unpark_all_threads() may be called prior to calling start().
 *
 * It's possible to set stripe thread tick-specific options (wait duration, ticks before
 * wait) both before and after start() is called. The options will be propagated to any
 * running stripe threads in a thread-safe, lock-free manner.
 */
class DistributorStripePool {
    using StripeVector       = std::vector<std::unique_ptr<DistributorStripeThread>>;
    using NativeThreadVector = std::vector<FastOS_ThreadInterface*>;

    FastOS_ThreadPool       _thread_pool;
    StripeVector            _stripes;
    NativeThreadVector      _threads;
    std::mutex              _mutex;
    std::condition_variable _parker_cond;
    size_t                  _parked_threads; // Must be protected by _park_mutex
    vespalib::duration      _bootstrap_tick_wait_duration;
    uint32_t                _bootstrap_ticks_before_wait;
    bool                    _stopped;

    friend class DistributorStripeThread;
public:
    DistributorStripePool();
    ~DistributorStripePool();

    // Set up the stripe pool with a 1-1 relationship between the provided
    // stripes and running threads. Can only be called once per pool.
    //
    // Precondition: stripes.size() > 0
    void start(const std::vector<TickableStripe*>& stripes);
    void stop_and_join();

    void park_all_threads() noexcept;
    void unpark_all_threads() noexcept;

    [[nodiscard]] const DistributorStripeThread& stripe(size_t idx) const noexcept {
        return *_stripes[idx];
    }
    [[nodiscard]] DistributorStripeThread& stripe(size_t idx) noexcept {
        return *_stripes[idx];
    }
    [[nodiscard]] size_t stripe_count() const noexcept { return _stripes.size(); }

    // Applies to all threads. May be called both before and after start(). Thread safe.
    void set_tick_wait_duration(vespalib::duration new_tick_wait_duration) noexcept;
    void set_ticks_before_wait(uint32_t new_ticks_before_wait) noexcept;
private:
    void park_thread_until_released(DistributorStripeThread& thread) noexcept;
};

}
