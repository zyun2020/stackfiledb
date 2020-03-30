#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>

#include "db/store_writer.h"
#include "db/stack_bucket.h"

namespace stackfiledb
{
    class SemWriter {
    public:
        explicit SemWriter(int count = 1) : count_(count), batch_(1) {
        }

        void Signal(StackBucket* bucket, StoreWriter* store_writer) {
            std::unique_lock<std::mutex> lock(mutex_);
            ++count_;
            bucket->ReleaseStoreWriter(store_writer);
            cv_.notify_one();
        }

        StoreWriter* Wait(StackBucket* bucket) {
            std::unique_lock<std::mutex> lock(mutex_);
            StoreWriter* writer = bucket->GetStoreWriter();
            while (count_ == 0 || writer == nullptr) {
                if (count_ > 0) {
                    cv_.notify_one();
                }
                cv_.wait(lock);
            }
            --count_;
            assert(writer != nullptr);
            return writer;
        }

        void SignalBatch(StackBucket* bucket, StoreWriter* store_writer) {
            std::unique_lock<std::mutex> lock(mutex_);
            bucket->ReleaseStoreWriter(store_writer);
            cv_.notify_one();
        }

        StoreWriter* WaitBatch(StackBucket* bucket) {
            std::unique_lock<std::mutex> lock(mutex_);
            StoreWriter* writer = bucket->GetStoreWriter();
            while (writer == nullptr) {
                cv_.wait(lock);
            }
            assert(writer != nullptr);
            return writer;
        }

        void SignalBatch() {
            std::unique_lock<std::mutex> lock(mutex_);
            ++batch_;
            cv_.notify_one();
        }

        void WaitBatch() {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [=] { return batch_ > 0; });
            --batch_;
        }
    private:
        std::mutex mutex_;
        std::condition_variable cv_;
        int count_;
        int batch_;
    };

    class WriterWait
    {
    public:
        explicit WriterWait(SemWriter* sem, StackBucket* bucket)
            : sem_(sem),
            bucket_(bucket){
            store_writer_ = sem_->Wait(bucket);
        }

        ~WriterWait() {
            if (sem_) {
                sem_->Signal(bucket_, store_writer_);
            }
        }

        WriterWait() = delete;
        WriterWait(const WriterWait&) = delete;
        WriterWait& operator = (const WriterWait&) = delete;
        WriterWait(const WriterWait&&) = delete;
        WriterWait& operator = (const WriterWait&&) = delete;

        StoreWriter* Writer() const { return store_writer_; }
    private:
        SemWriter* const sem_ = nullptr;
        StoreWriter* store_writer_;
        StackBucket* const bucket_;
    };

} //stackfiledb