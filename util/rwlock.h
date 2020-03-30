#pragma once

#if defined(_WIN32)
#include <windows.h>
#else
#include <pthread.h>
#endif

namespace stackfiledb
{

#if defined(_WIN32)
        class RWLock
        {
        public:
            RWLock() {
                /* Initialize the semaphore that acts as the write lock. */
                write_semaphore_ = CreateSemaphoreW(nullptr, 1, 1, nullptr);
               
                /* Initialize the critical section protecting the reader count. */
                InitializeCriticalSection(&num_readers_lock_);

                /* Initialize the reader count. */
                num_readers_ = 0;
            }
            ~RWLock() {
                DeleteCriticalSection(&num_readers_lock_);
                CloseHandle(write_semaphore_);
            }

            RWLock(const RWLock&) = delete;
            RWLock& operator=(const RWLock&) = delete;

            RWLock(const RWLock&&) = delete;
            RWLock& operator=(const RWLock&&) = delete;

            void lock_read() {
                /* Acquire the lock that protects the reader count. */
                EnterCriticalSection(&num_readers_lock_);

                /* Increase the reader count, and lock for write if this is the first
                 * reader.
                 */
                if (++num_readers_ == 1) {
                    WaitForSingleObject(write_semaphore_, INFINITE);
                }

                /* Release the lock that protects the reader count. */
                LeaveCriticalSection(&num_readers_lock_);
            }

            void unlock_read() {
                EnterCriticalSection(&num_readers_lock_);

                if (--num_readers_ == 0) {
                    ReleaseSemaphore(write_semaphore_, 1, nullptr);
                }

                LeaveCriticalSection(&num_readers_lock_);
            }

            void lock_write() {
                WaitForSingleObject(write_semaphore_, INFINITE);
            }

            void unlock_write() {
                ReleaseSemaphore(write_semaphore_, 1, nullptr);
            }

        private:
            unsigned int num_readers_;
            CRITICAL_SECTION num_readers_lock_;
            HANDLE write_semaphore_;
        };
#else
    class RWLock
    {
    public:
        RWLock() {
            pthread_rwlock_init(&rwlock_, nullptr);
        }
        ~RWLock() {
            pthread_rwlock_destroy(&rwlock_);
        }

        RWLock(const RWLock&) = delete;
        RWLock& operator=(const RWLock&) = delete;

        RWLock(const RWLock&&) = delete;
        RWLock& operator=(const RWLock&&) = delete;

        void lock_read() {
            pthread_rwlock_rdlock(&rwlock_);
        }

        void unlock_read() {
            pthread_rwlock_unlock(&rwlock_);
        }

        void lock_write() {
            pthread_rwlock_wrlock(&rwlock_);
        }

        void unlock_write() {
            pthread_rwlock_unlock(&rwlock_);
        }

    private:
        pthread_rwlock_t rwlock_;
    };
#endif

    class ReaderLock
    {
    public:
        explicit ReaderLock(RWLock* rwLock)
            : rwlock_(rwLock) {
            rwlock_->lock_read();
        }

        ~ReaderLock() {
            if (rwlock_) {
                rwlock_->unlock_read();
            }
        }

        ReaderLock() = delete;
        ReaderLock(const ReaderLock&) = delete;
        ReaderLock& operator = (const ReaderLock&) = delete;
        ReaderLock(const ReaderLock&&) = delete;
        ReaderLock& operator = (const ReaderLock&&) = delete;

    private:
        RWLock* rwlock_ = nullptr;
    };

    class WriterLock
    {
    public:
        explicit WriterLock(RWLock* rwLock)
            : rwlock_(rwLock) {
            rwlock_->lock_write();
        }

        ~WriterLock() {
            if (rwlock_) {
                rwlock_->unlock_write();
            }
        }

        WriterLock() = delete;
        WriterLock(const WriterLock&) = delete;
        WriterLock& operator = (const WriterLock&) = delete;
        WriterLock(const WriterLock&&) = delete;
        WriterLock& operator = (const WriterLock&&) = delete;

    private:
        RWLock* rwlock_ = nullptr;
    };

} //stackfiledb