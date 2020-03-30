#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>

namespace stackfiledb
{
    class Semaphore {
    public:
        explicit Semaphore(int count = 0) : count_(count) {
        }

        void Signal() {
            std::unique_lock<std::mutex> lock(mutex_);
            ++count_;
            if (count_ == 1)
                cv_.notify_one();
        }

        void Wait() {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [=] { return count_ > 0; });
            --count_;
        }

    private:
        std::mutex mutex_;
        std::condition_variable cv_;
        int count_;
    };

    class SemWait
    {
    public:
        explicit SemWait(Semaphore* sem)
            : sem_(sem) {
            sem_->Wait();
        }

        ~SemWait() {
            if (sem_) {
                sem_->Signal();
            }
        }

        SemWait() = delete;
        SemWait(const SemWait&) = delete;
        SemWait& operator = (const SemWait&) = delete;
        SemWait(const SemWait&&) = delete;
        SemWait& operator = (const SemWait&&) = delete;

    private:
        Semaphore* sem_ = nullptr;
    };

} //stackfiledb