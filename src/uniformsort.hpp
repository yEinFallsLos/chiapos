// Copyright 2018 Chia Network Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SRC_CPP_UNIFORMSORT_HPP_
#define SRC_CPP_UNIFORMSORT_HPP_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>

#include "./disk.hpp"
#include "./util.hpp"

#include <liburing.h>

#define QUEUE_DEPTH 8
#define MAX_BUFFERS 64

namespace UniformSort {

    inline int64_t const BUF_SIZE = 262144;

    inline int64_t const WINDOW_SIZE = BUF_SIZE * QUEUE_DEPTH;

    template<typename T, typename OP>
    T manipulate_bit(std::atomic<T> &a, unsigned n, OP bit_op)
    {
        static_assert(std::is_integral<T>::value, "atomic type not integral");

        T val = a.load();
        while (!a.compare_exchange_weak(val, bit_op(val, n)));

        return val;
    }

    auto set_bit = [](auto val, unsigned n) { return val | (1 << n); };
    auto clr_bit = [](auto val, unsigned n) { return val & ~(1 << n); };
    auto tgl_bit = [](auto val, unsigned n) { return val ^ (1 << n); };


    inline void SortToMemory(
        FileDisk &input_disk,
        uint64_t const input_disk_begin,
        uint8_t *const memory,
        uint32_t const entry_len,
        uint64_t const num_entries,
        uint32_t const bits_begin)
    {
        struct io_uring ring;
        io_uring_queue_init(QUEUE_DEPTH, &ring, 0);

        uint32_t start_byte = bits_begin / 8;
        uint8_t mask = ((1 << (8 - (bits_begin % 8))) - 1);

        __kernel_timespec timeout;
        timeout.tv_sec = 1;
        
        uint64_t const rounded_entries = Util::RoundSize(num_entries);
        auto const swap_space = std::make_unique<uint8_t[]>(entry_len);
        auto const buffer = std::make_unique<uint8_t[]>(BUF_SIZE);
        uint64_t bucket_length = 0;

        // make sure the disk is open
        input_disk.Open();

        // The number of buckets needed (the smallest power of 2 greater than 2 * num_entries).
        while ((1ULL << bucket_length) < 2 * num_entries) bucket_length++;
        bitfield is_used(rounded_entries);

        int fd = input_disk.fd;
        uint64_t completed_entries = 0;
        uint64_t queued_entries = 0;

        // create buffer pool (max 64)
        std::atomic<uint64_t> free_buffers{0xFFFFFFFFFFFFFFFFL};
        uint8_t** bufferPool;
        
        bufferPool = new uint8_t*[MAX_BUFFERS];
        for (uint32_t i = 0 ; i < MAX_BUFFERS; i++) {
            bufferPool[i] = new uint8_t[BUF_SIZE];
        }


        do {
            struct io_uring_cqe *cqe;
            struct io_uring_sqe *sqe;
            uint32_t queued = 0;

            // If there are open CQE slots, queue them up
            while ((QUEUE_DEPTH > (MAX_BUFFERS - __builtin_popcountll(free_buffers.load())))
                     && queued_entries < num_entries 
                     && (sqe = io_uring_get_sqe(&ring)) != NULL) {
                // read either the size of the buffer or the number of entries remaining
                uint32_t entries_to_read = std::min((uint64_t)BUF_SIZE / entry_len, num_entries - queued_entries);
                // get a free buffer
                int free_buffer_idx = -1;
                while (free_buffer_idx < 0) {
                    free_buffer_idx = __builtin_ffsll(free_buffers.load()) - 1;
                    if (free_buffer_idx >= 0) {
                        manipulate_bit(free_buffers, free_buffer_idx, clr_bit);
                    } else {
                        std::cout << "no buffers... waiting for more..." << std::endl;
                    }
                }
                auto const buffer = bufferPool[free_buffer_idx];
                //std::cout << "Requesting: " << entries_to_read * entry_len << " bytes, offset = " << input_disk_begin + (queued_entries * entry_len) << std::endl;
                io_uring_prep_read(sqe, fd, buffer, entries_to_read * entry_len, input_disk_begin + (queued_entries * entry_len));
                // annotate the request with the buffer and number of entries.
                // top 32 bits = entries, bottom 32 = index
                uint64_t user_data = entries_to_read;
                user_data <<= 32;
                user_data |= free_buffer_idx;
                io_uring_sqe_set_data(sqe, (void*) user_data);
                // increment the number of queued entries
                queued_entries += entries_to_read;
                queued++;
            }

            // submit the read request(s)
            if (queued > 0) {
                int submitted = io_uring_submit(&ring);
            }

            int ret = io_uring_wait_cqe_timeout(&ring, &cqe, &timeout);
            // If there were entries in the CQ (wait up to timeout), process them. Otherwise loop
            if (ret == 0) {
                // buffer index is in lower 32 bits
                int index = cqe->user_data & 0xFFFFFFFF;
                // entry_count is in upper 32 bits
                uint32_t  entry_count = (cqe->user_data >> 32) & 0xFFFFFFFF;

                // amount read should equal requested
                assert(cqe->res == entry_count * entry_len);
                uint8_t* buffer = bufferPool[index];

                // free the cqe
                io_uring_cqe_seen(&ring, cqe);

                for (uint64_t entry_num = 0; entry_num < entry_count; entry_num++) {
                    // First unique bits in the entry give the expected position of it in the sorted array.
                    // We take 'bucket_length' bits starting with the first unique one.
                    uint64_t buf_ofs = entry_num * entry_len;
                    uint64_t idx = Util::ExtractNum(buffer + buf_ofs, entry_len, bits_begin, bucket_length);
                    uint64_t mem_ofs = idx * entry_len;

                    // As long as position is occupied by a previous entry...
                    while (is_used.get(idx) && idx < rounded_entries) {
                        if (Util::MemCmpBits(
                            memory + mem_ofs, buffer + buf_ofs, entry_len, start_byte, mask) > 0) {
                                // Swap memory and buffer using temporary space
                                memcpy(swap_space.get(), memory + mem_ofs, entry_len);
                                memcpy(memory + mem_ofs, buffer + buf_ofs, entry_len);
                                memcpy(buffer + buf_ofs, swap_space.get(), entry_len);
                            }
                            idx++;
                            mem_ofs += entry_len;
                    }

                    memcpy(memory + mem_ofs, buffer + buf_ofs, entry_len);
                    is_used.set(idx);
                    completed_entries++;
                }

                manipulate_bit(free_buffers, index, set_bit);
            }
        } while (completed_entries < num_entries);

        uint64_t entries_written = 0;
        uint64_t entries_ofs = 0;
        // Search the memory buffer for occupied entries.
        for (uint64_t idx = 0, mem_ofs = 0; entries_written < num_entries && idx < rounded_entries;
              idx++, mem_ofs += entry_len) {
             if (is_used.get(idx)) {
                // We've found an entry.
                // write the stored entry itself.
                memcpy(
                    memory + entries_ofs,
                     memory + mem_ofs,
                    entry_len);
                entries_written++;
                entries_ofs += entry_len;
            }
        }
        io_uring_queue_exit(&ring);

        // free buffers
        for (uint32_t i = 0 ; i < MAX_BUFFERS; i++) {
            delete[] bufferPool[i];
        }
        delete[] bufferPool;

        assert(entries_written == num_entries);
    }

}

#endif  // SRC_CPP_UNIFORMSORT_HPP_
