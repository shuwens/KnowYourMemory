#include "conn.hpp"
#include "cxxopts.hpp"
#include "endpoint.hpp"
#include "ring_buffer/magic_buffer.hpp"
#include <atomic> // Add this for std::atomic
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <infiniband/verbs.h>
#include <iostream>
#include <ostream>
#include <string>
#include <thread>

kym::endpoint::Options opts = {
    .qp_attr =
        {
            .cap =
                {
                    .max_send_wr = 1,
                    .max_recv_wr = 1,
                    .max_send_sge = 1,
                    .max_recv_sge = 1,
                    .max_inline_data = 8,
                },
            .qp_type = IBV_QPT_RC,
        },
    .responder_resources = 5,
    .initiator_depth = 5,
    .retry_count = 8,
    .rnr_retry_count = 0,
    .native_qp = false,
    .inline_recv = 0,
};

cxxopts::ParseResult parse(int argc, char *argv[]) {
  cxxopts::Options options(argv[0], "sendrecv");
  try {
    options.add_options()("client", "Whether to as client only",
                          cxxopts::value<bool>())(
        "server", "Whether to as server only", cxxopts::value<bool>())(
        "i,address", "IP address to connect to", cxxopts::value<std::string>())(
        "s,size", "Size of message to exchange",
        cxxopts::value<int>()->default_value("60"));

    auto result = options.parse(argc, argv);
    if (!result.count("address")) {
      std::cerr << "Specify an address" << std::endl;
      std::cerr << options.help({""}) << std::endl;
      exit(1);
    }

    return result;
  } catch (const cxxopts::OptionException &e) {
    std::cerr << "error parsing options: " << e.what() << std::endl;
    std::cerr << options.help({""}) << std::endl;
    exit(1);
  }
}

struct cinfo {
  uint32_t generic_key;
  uint64_t generic_addr;
  uint32_t magic_key;
  uint64_t magic_addr;
};

int main(int argc, char *argv[]) {
  std::cout << "#### Testing write performance to ringbuffer####" << std::endl;

  auto flags = parse(argc, argv);
  std::string ip = flags["address"].as<std::string>();

  bool is_server = flags["server"].as<bool>();
  bool is_client = flags["client"].as<bool>();

  if (is_server) {
    auto ln_s = kym::endpoint::Listen(ip, 8987);
    if (!ln_s.ok()) {
      std::cerr << "Error listening" << ln_s.status() << std::endl;
      return 1;
    }
    auto ln = ln_s.value();

    // Allocate a page of normal heap memory
    int size = 4 * 1024 * 1024;
    void *generic = malloc(size);
    memset(generic, 0xAA, size); // Initialize memory with pattern
    struct ibv_mr *generic_mr =
        ibv_reg_mr(ln->GetPd(), generic, size,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

    // Allocate "magic" buffer
    auto magic_s = kym::ringbuffer::GetMagicBuffer(size);
    if (!magic_s.ok()) {
      std::cerr << "error allocating magic buffer " << magic_s.status()
                << std::endl;
      return 1;
    }
    void *magic = magic_s.value();
    memset(magic, 0xAA, 2 * size); // Initialize magic buffer with pattern
    struct ibv_mr *magic_mr =
        ibv_reg_mr(ln->GetPd(), magic, 2 * size,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

    struct cinfo ci;
    ci.generic_addr = (uint64_t)generic;
    ci.generic_key = generic_mr->lkey;
    ci.magic_addr = (uint64_t)magic;
    ci.magic_key = magic_mr->lkey;

    opts.private_data = &ci;
    opts.private_data_len = sizeof(ci);

    std::cout
        << "Server: Memory regions registered and filled with pattern 0xAA"
        << std::endl;
    std::cout << "  Generic buffer at 0x" << std::hex << ci.generic_addr
              << std::dec << std::endl;
    std::cout << "  Magic buffer at 0x" << std::hex << ci.magic_addr << std::dec
              << std::endl;

    auto ep_s = ln->Accept(opts);
    if (!ep_s.ok()) {
      std::cerr << "Error accepting connection " << ep_s.status() << std::endl;
      return 1;
    }
    kym::endpoint::Endpoint *ep = ep_s.value();
    std::cout << "Server: Client connected" << std::endl;

    // IMMEDIATELY post the receive buffer for the end signal
    void *wait = malloc(4);
    memset(wait, 0, 4);
    struct ibv_mr *wait_mr =
        ibv_reg_mr(ln->GetPd(), wait, 4, IBV_ACCESS_LOCAL_WRITE);

    auto recv_stat = ep->PostRecv(1, wait_mr->lkey, wait, 4);
    if (!recv_stat.ok()) {
      std::cerr << "Error posting receive " << recv_stat << std::endl;
      return 1;
    }
    std::cout << "Server: Posted receive buffer for end signal" << std::endl;

    // Define memory regions to monitor based on client's test areas
    struct MemoryRegion {
      const char *name;
      void *buffer;
      size_t offset;
      uint64_t last_change_time;
      int changes;
    };

    // Create regions to monitor - these match exactly what the client writes to
    MemoryRegion regions[] = {
        {"Generic Middle", generic, 2 * 1024 * 1024, 0, 0},
        {"Magic Middle", magic, 2 * 1024 * 1024, 0, 0},
        {"Magic Wrap", magic, 4 * 1024 * 1024 - 32, 0, 0}};

    // Store snapshots and print initial state
    std::vector<std::vector<unsigned char>> initial_state;

    std::cout << "Server: Initial memory state:" << std::endl;
    for (const auto &region : regions) {
      std::vector<unsigned char> snapshot(64);
      std::cout << "  " << region.name << " at offset " << region.offset << ":"
                << std::endl;

      if (region.name == std::string("Magic Wrap")) {
        // Handle wrap-around for the magic buffer
        for (int i = 0; i < 64; i++) {
          size_t idx = (region.offset + i) % (2 * size);
          snapshot[i] = ((unsigned char *)region.buffer)[idx];

          if (i % 16 == 0) {
            std::cout << "    ";
          }
          printf("%02x ", snapshot[i]);
          if ((i + 1) % 16 == 0) {
            std::cout << std::endl;
          }
        }
      } else {
        // Regular memory access
        unsigned char *ptr = (unsigned char *)region.buffer + region.offset;
        memcpy(snapshot.data(), ptr, 64);

        for (int i = 0; i < 64; i++) {
          if (i % 16 == 0) {
            std::cout << "    ";
          }
          printf("%02x ", ptr[i]);
          if ((i + 1) % 16 == 0) {
            std::cout << std::endl;
          }
        }
      }

      initial_state.push_back(snapshot);
    }

    // Create atomic flag for thread coordination
    std::atomic<bool> stop_monitoring{false};

    // Define memory monitor thread
    std::thread monitor_thread([&regions, &initial_state, &stop_monitoring,
                                generic, magic, size]() {
      // Create copies of the snapshots for the thread
      std::vector<std::vector<unsigned char>> snapshots = initial_state;

      std::cout << "Monitor thread: Started monitoring memory regions"
                << std::endl;

      auto start_time = std::chrono::high_resolution_clock::now();
      const int MAX_DUMPS_PER_REGION =
          10; // Limit the number of dumps per region

      while (!stop_monitoring.load()) {
        auto now = std::chrono::high_resolution_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - start_time)
                              .count();

        // Check each region for changes
        for (int r = 0; r < 3; r++) {
          auto &region = regions[r];
          auto &snapshot = snapshots[r];
          bool changed = false;

          if (region.name == std::string("Magic Wrap")) {
            // Handle wrap-around for the magic buffer
            for (int i = 0; i < 64; i++) {
              size_t idx = (region.offset + i) % (2 * size);
              unsigned char current = ((unsigned char *)magic)[idx];

              if (current != snapshot[i]) {
                changed = true;
                snapshot[i] = current; // Update snapshot
              }
            }
          } else {
            // Regular memory access
            unsigned char *ptr = (unsigned char *)region.buffer + region.offset;
            for (int i = 0; i < 64; i++) {
              if (ptr[i] != snapshot[i]) {
                changed = true;
                snapshot[i] = ptr[i]; // Update snapshot
              }
            }
          }

          // If changed, print the new state
          if (changed) {
            region.changes++;
            region.last_change_time = elapsed_ms;

            if (region.changes <= MAX_DUMPS_PER_REGION) {
              std::cout << "\nCHANGE #" << region.changes << " in "
                        << region.name << " at " << elapsed_ms
                        << "ms:" << std::endl;

              if (region.name == std::string("Magic Wrap")) {
                for (int i = 0; i < 64; i++) {
                  if (i % 16 == 0) {
                    std::cout << "    ";
                  }
                  size_t idx = (region.offset + i) % (2 * size);
                  printf("%02x ", ((unsigned char *)magic)[idx]);
                  if ((i + 1) % 16 == 0) {
                    std::cout << std::endl;
                  }
                }
              } else {
                unsigned char *ptr =
                    (unsigned char *)region.buffer + region.offset;
                for (int i = 0; i < 64; i++) {
                  if (i % 16 == 0) {
                    std::cout << "    ";
                  }
                  printf("%02x ", ptr[i]);
                  if ((i + 1) % 16 == 0) {
                    std::cout << std::endl;
                  }
                }
              }
            } else if (region.changes == MAX_DUMPS_PER_REGION + 1) {
              std::cout << "Reached maximum dump limit for " << region.name
                        << ", will stop showing dumps for this region"
                        << std::endl;
            }
          }
        }

        // Slight delay to reduce CPU usage
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }

      // Print final change counts
      std::cout << "Monitor thread: Final change counts:" << std::endl;
      for (const auto &region : regions) {
        std::cout << "  " << region.name << ": " << region.changes
                  << " changes detected"
                  << " (last at " << region.last_change_time << "ms)"
                  << std::endl;
      }
    });

    std::cout << "Server: Waiting for client to complete tests and signal..."
              << std::endl;

    // Wait for the end signal
    auto wc_s = ep->PollRecvCq();
    if (!wc_s.ok()) {
      std::cerr << "Error polling receive completion queue: " << wc_s.status()
                << std::endl;
    } else {
      std::cout << "Server: Received end signal from client" << std::endl;
      // Check what the end signal contains
      std::cout << "End signal content: ";
      for (int i = 0; i < 4; ++i) {
        printf("0x%02x ", ((unsigned char *)wait)[i]);
      }
      std::cout << std::endl;
    }

    // Stop the monitoring thread
    stop_monitoring.store(true);
    monitor_thread.join();

    // Print monitoring summary again
    std::cout << "Server: Monitoring summary" << std::endl;
    for (const auto &region : regions) {
      std::cout << "  " << region.name << ": " << region.changes
                << " changes detected"
                << " (last at " << region.last_change_time << "ms)"
                << std::endl;
    }

    // Clean up resources
    ibv_dereg_mr(wait_mr);
    free(wait);

    ibv_dereg_mr(magic_mr);
    kym::ringbuffer::FreeMagicBuffer(magic, size);

    ibv_dereg_mr(generic_mr);
    free(generic);

    auto stat = ep->Close();
    if (!stat.ok()) {
      std::cerr << "Error closing endpoint " << stat << std::endl;
      return 1;
    }
    stat = ln->Close();
    if (!stat.ok()) {
      std::cerr << "Error closing listener " << stat << std::endl;
      return 1;
    }

    std::cout << "Server: Resources cleaned up and exiting" << std::endl;
  }

  if (is_client) {
    auto ep_s = kym::endpoint::Dial(ip, 8987, opts);
    if (!ep_s.ok()) {
      std::cerr << "Error dialing " << ep_s.status() << std::endl;
      return 1;
    }
    kym::endpoint::Endpoint *ep = ep_s.value();
    std::cout << "Client: Connected to server" << std::endl;

    struct cinfo *ci;
    ep->GetConnectionInfo((void **)&ci);
    std::cout << "Client: Received memory region info from server" << std::endl;
    std::cout << "  Generic buffer at 0x" << std::hex << ci->generic_addr
              << std::dec << std::endl;
    std::cout << "  Magic buffer at 0x" << std::hex << ci->magic_addr
              << std::dec << std::endl;

    int size = 64;
    int n = 1000; // Reduced from 1,000,000 to make testing faster but still
                  // meaningful

    // Allocate send buffer and fill with recognizable pattern
    void *send = malloc(size);
    memset(send, 0x42, size); // Fill with 'B' character (0x42)
    struct ibv_mr *send_mr =
        ibv_reg_mr(ep->GetPd(), send, size, IBV_ACCESS_LOCAL_WRITE);

    // Sleep a bit to ensure server is ready to monitor
    std::cout
        << "Client: Sleeping for 2 seconds to ensure server is monitoring..."
        << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Test latency for generic mr
    std::cout << "Client: Starting generic buffer write test..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < n; i++) {
      if (i % 100 == 0) {
        // Change first byte periodically to help server detect each write
        ((char *)send)[0] = 0x42 + (i / 100) % 10;
      }

      auto stat =
          ep->PostWrite(i, send_mr->lkey, send, size,
                        ci->generic_addr + 2 * 1024 * 1024, ci->generic_key);
      if (!stat.ok()) {
        std::cerr << "Error writing to generic buffer: " << stat << std::endl;
        return 1;
      }
      auto wc_s = ep->PollSendCq();
      if (!wc_s.ok()) {
        std::cerr << "Error polling send CQ for generic write: "
                  << wc_s.status() << std::endl;
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    double dur =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count() /
        1000.0;
    std::cout << "Client: Generic buffer test complete" << std::endl;
    std::cout << "Mean write latency generic mr msg size " << size
              << " bytes: " << dur / (double)n << " µs" << std::endl;

    // Sleep between tests to help server distinguish them
    std::cout << "Client: Sleeping for 1 second between tests..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Modify send buffer to help server detect new test
    ((char *)send)[0] = 0x55; // Change first byte to 'U'

    // Test latency for magic mr
    std::cout << "Client: Starting magic buffer middle write test..."
              << std::endl;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < n; i++) {
      if (i % 100 == 0) {
        // Change first byte periodically to help server detect each write
        ((char *)send)[0] = 0x55 + (i / 100) % 10;
      }

      auto stat =
          ep->PostWrite(i, send_mr->lkey, send, size,
                        ci->magic_addr + 2 * 1024 * 1024, ci->magic_key);
      if (!stat.ok()) {
        std::cerr << "Error writing to magic buffer middle: " << stat
                  << std::endl;
        return 1;
      }
      auto wc_s = ep->PollSendCq();
      if (!wc_s.ok()) {
        std::cerr << "Error polling send CQ for magic middle write: "
                  << wc_s.status() << std::endl;
      }
    }
    end = std::chrono::high_resolution_clock::now();
    dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count() /
          1000.0;
    std::cout << "Client: Magic buffer middle test complete" << std::endl;
    std::cout << "Mean write latency magic mr msg size " << size
              << " bytes: " << dur / (double)n << " µs" << std::endl;

    // Sleep between tests to help server distinguish them
    std::cout << "Client: Sleeping for 1 second between tests..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Modify send buffer again for the final test
    ((char *)send)[0] = 0x66; // Change first byte to 'f'

    // Test latency for magic mr over end of buffer
    std::cout << "Client: Starting magic buffer wrap write test..."
              << std::endl;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < n; i++) {
      if (i % 100 == 0) {
        // Change first byte periodically to help server detect each write
        ((char *)send)[0] = 0x66 + (i / 100) % 10;
      }

      auto stat =
          ep->PostWrite(i, send_mr->lkey, send, size,
                        ci->magic_addr + 4 * 1024 * 1024 - 32, ci->magic_key);
      if (!stat.ok()) {
        std::cerr << "Error writing to magic buffer wrap: " << stat
                  << std::endl;
        return 1;
      }
      auto wc_s = ep->PollSendCq();
      if (!wc_s.ok()) {
        std::cerr << "Error polling send CQ for magic wrap write: "
                  << wc_s.status() << std::endl;
      }
    }
    end = std::chrono::high_resolution_clock::now();
    dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count() /
          1000.0;
    std::cout << "Client: Magic buffer wrap test complete" << std::endl;
    std::cout << "Mean write latency magic mr msg size " << size << " bytes"
              << std::endl;
    std::cout << "Writing over the end of the buffer" << std::endl;
    std::cout << dur / (double)n << " µs" << std::endl;

    // Sleep before sending end signal
    std::cout << "Client: Sleeping for 1 second before sending end signal..."
              << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Send end signal
    std::cout << "Client: Sending end signal to server..." << std::endl;
    auto stat = ep->PostImmidate(1, 4);
    if (!stat.ok()) {
      std::cerr << "Error sending end signal: " << stat << std::endl;
      return 1;
    }
    auto wc_s = ep->PollSendCq();
    if (!wc_s.ok()) {
      std::cerr << "Error polling send CQ for end signal: " << wc_s.status()
                << std::endl;
    }

    std::cout << "Client: Clean up and exit" << std::endl;

    // Clean up resources
    ibv_dereg_mr(send_mr);
    free(send);

    stat = ep->Close();
    if (!stat.ok()) {
      std::cerr << "Error closing endpoint " << stat << std::endl;
      return 1;
    }
  }

  return 0;
}
