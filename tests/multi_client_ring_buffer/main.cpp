#include "conn.hpp"
#include "cxxopts.hpp"
#include "endpoint.hpp"
#include "ring_buffer/magic_buffer.hpp"
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <infiniband/verbs.h>
#include <iostream>
#include <map>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

// Default QP Options - optimized for multiple connections
kym::endpoint::Options opts = {
    .qp_attr =
        {
            .cap =
                {
                    .max_send_wr = 128, // Increased for multiple clients
                    .max_recv_wr = 128, // Increased for multiple clients
                    .max_send_sge = 1,
                    .max_recv_sge = 1,
                    .max_inline_data = 8,
                },
            .qp_type = IBV_QPT_RC,
        },
    .responder_resources = 16, // Increased for multiple clients
    .initiator_depth = 16,     // Increased for multiple clients
    .retry_count = 8,
    .rnr_retry_count = 7, // Set to max (infinite) for better reliability
    .native_qp = false,
    .inline_recv = 0,
};

cxxopts::ParseResult parse(int argc, char *argv[]) {
  cxxopts::Options options(argv[0], "multi-client-ringbuffer");
  try {
    options.add_options()("client", "Whether to act as client only",
                          cxxopts::value<bool>())(
        "server", "Whether to act as server only", cxxopts::value<bool>())(
        "i,address", "IP address to connect to", cxxopts::value<std::string>())(
        "s,size", "Size of message to exchange",
        cxxopts::value<int>()->default_value("60"))(
        "c,clients", "Number of expected clients (server only)",
        cxxopts::value<int>()->default_value("1"))(
        "n,iterations", "Number of write iterations",
        cxxopts::value<int>()->default_value("1000"))(
        "client-id", "Client identifier (client only)",
        cxxopts::value<int>()->default_value("0"));

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

// Structure for connection info
struct cinfo {
  uint32_t generic_key;
  uint64_t generic_addr;
  uint32_t magic_key;
  uint64_t magic_addr;
  uint32_t client_id; // Added client identifier
};

int main(int argc, char *argv[]) {
  std::cout << "#### Multi-Client Ring Buffer Performance Test ####"
            << std::endl;

  auto flags = parse(argc, argv);
  std::string ip = flags["address"].as<std::string>();

  bool is_server = flags["server"].as<bool>();
  bool is_client = flags["client"].as<bool>();
  int expected_clients = flags["clients"].as<int>();
  int iterations = flags["iterations"].as<int>();
  int client_id = flags["client-id"].as<int>();
  int size = flags["size"].as<int>();

  if (is_server) {
    // ======================= SERVER CODE =======================
    auto ln_s = kym::endpoint::Listen(ip, 8987);
    if (!ln_s.ok()) {
      std::cerr << "Error listening" << ln_s.status() << std::endl;
      return 1;
    }
    auto ln = ln_s.value();

    std::cout << "Server: Listening on " << ip << ", port 8987" << std::endl;
    std::cout << "Server: Expecting " << expected_clients << " clients"
              << std::endl;

    // Allocate a page of normal heap memory - larger for multiple clients
    int buffer_size = 4 * 1024 * 1024;
    void *generic = malloc(buffer_size);
    memset(generic, 0xAA, buffer_size);
    struct ibv_mr *generic_mr =
        ibv_reg_mr(ln->GetPd(), generic, buffer_size,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

    // Allocate "magic" buffer
    auto magic_s = kym::ringbuffer::GetMagicBuffer(buffer_size);
    if (!magic_s.ok()) {
      std::cerr << "error allocating magic buffer " << magic_s.status()
                << std::endl;
      return 1;
    }
    void *magic = magic_s.value();
    memset(magic, 0xAA, 2 * buffer_size);
    struct ibv_mr *magic_mr =
        ibv_reg_mr(ln->GetPd(), magic, 2 * buffer_size,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

    std::cout
        << "Server: Memory regions registered and filled with pattern 0xAA"
        << std::endl;
    std::cout << "  Generic buffer at 0x" << std::hex << (uint64_t)generic
              << std::dec << " size: " << buffer_size << " bytes" << std::endl;
    std::cout << "  Magic buffer at 0x" << std::hex << (uint64_t)magic
              << std::dec << " size: " << 2 * buffer_size << " bytes"
              << std::endl;

    // Define memory regions to monitor based on client's test areas
    struct ClientMonitoringInfo {
      std::string name;
      void *buffer;
      size_t offset;
      uint64_t last_change_time;
      int changes;
    };

    // Store endpoints for each client
    std::vector<kym::endpoint::Endpoint *> client_endpoints;
    std::vector<void *> wait_buffers;
    std::vector<ibv_mr *> wait_mrs;

    // Per-client monitoring info with different offsets for each client
    std::map<int, std::vector<ClientMonitoringInfo>> client_regions;

    // Create a mutex for thread-safe operation
    std::mutex monitor_mutex;

    // Accept connections from all expected clients
    for (int c = 0; c < expected_clients; c++) {
      std::cout << "Server: Waiting for client " << (c + 1) << "/"
                << expected_clients << "..." << std::endl;

      struct cinfo ci;
      ci.generic_addr = (uint64_t)generic;
      ci.generic_key = generic_mr->lkey;
      ci.magic_addr = (uint64_t)magic;
      ci.magic_key = magic_mr->lkey;
      ci.client_id = c; // Assign client ID

      opts.private_data = &ci;
      opts.private_data_len = sizeof(ci);

      auto ep_s = ln->Accept(opts);
      if (!ep_s.ok()) {
        std::cerr << "Error accepting connection " << ep_s.status()
                  << std::endl;
        return 1;
      }

      kym::endpoint::Endpoint *ep = ep_s.value();
      client_endpoints.push_back(ep);

      std::cout << "Server: Client " << c << " connected" << std::endl;

      // Post the receive buffer for the end signal
      void *wait = malloc(4);
      memset(wait, 0, 4);
      struct ibv_mr *wait_mr =
          ibv_reg_mr(ln->GetPd(), wait, 4, IBV_ACCESS_LOCAL_WRITE);
      wait_buffers.push_back(wait);
      wait_mrs.push_back(wait_mr);

      auto recv_stat = ep->PostRecv(c + 1, wait_mr->lkey, wait, 4);
      if (!recv_stat.ok()) {
        std::cerr << "Error posting receive " << recv_stat << std::endl;
        return 1;
      }

      // Calculate specific offsets for this client
      // Each client gets a dedicated section of the buffers
      int client_section_size = buffer_size / expected_clients;
      int client_section_start = c * client_section_size;

      // Create monitoring regions for this client
      std::vector<ClientMonitoringInfo> regions = {
          {"Client" + std::to_string(c) + " Generic", generic,
           client_section_start + client_section_size / 2, 0, 0},
          {"Client" + std::to_string(c) + " Magic", magic,
           client_section_start + client_section_size / 2, 0, 0},
          {"Client" + std::to_string(c) + " Magic Wrap", magic,
           client_section_start + client_section_size - 32, 0, 0}};

      client_regions[c] = regions;

      std::cout << "Server: Client " << c
                << " monitoring regions created:" << std::endl;
      for (const auto &region : regions) {
        std::cout << "  " << region.name << " at offset " << region.offset
                  << std::endl;
      }
    }

    std::cout << "Server: All " << expected_clients << " clients connected"
              << std::endl;

    // Create atomic flag for thread coordination
    std::atomic<bool> stop_monitoring{false};

    // Define memory monitor thread
    std::thread monitor_thread([&client_regions, &stop_monitoring, generic,
                                magic, buffer_size, &monitor_mutex]() {
      // Store snapshots of each region
      std::map<int, std::vector<std::vector<unsigned char>>> snapshots;

      // Initialize snapshots
      {
        std::lock_guard<std::mutex> lock(monitor_mutex);
        for (const auto &client_pair : client_regions) {
          int c = client_pair.first;
          const auto &regions = client_pair.second;
          std::vector<std::vector<unsigned char>> client_snapshots;

          for (const auto &region : regions) {
            std::vector<unsigned char> snapshot(64);

            if (region.name.find("Magic Wrap") != std::string::npos) {
              // Handle wrap-around for the magic buffer
              for (int i = 0; i < 64; i++) {
                size_t idx = (region.offset + i) % (2 * buffer_size);
                snapshot[i] = ((unsigned char *)region.buffer)[idx];
              }
            } else {
              // Regular memory access
              unsigned char *ptr =
                  (unsigned char *)region.buffer + region.offset;
              memcpy(snapshot.data(), ptr, 64);
            }

            client_snapshots.push_back(snapshot);
          }

          snapshots[c] = client_snapshots;
        }
      }

      std::cout << "Monitor thread: Started monitoring memory regions for "
                << client_regions.size() << " clients" << std::endl;

      auto start_time = std::chrono::high_resolution_clock::now();
      const int MAX_DUMPS_PER_REGION = 10;

      while (!stop_monitoring.load()) {
        auto now = std::chrono::high_resolution_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - start_time)
                              .count();

        // Check each client's regions for changes
        {
          std::lock_guard<std::mutex> lock(monitor_mutex);
          for (auto &client_pair : client_regions) {
            int c = client_pair.first;
            auto &regions = client_pair.second;
            auto &client_snapshots = snapshots[c];

            for (size_t r = 0; r < regions.size(); r++) {
              auto &region = regions[r];
              auto &snapshot = client_snapshots[r];
              bool changed = false;

              if (region.name.find("Magic Wrap") != std::string::npos) {
                // Handle wrap-around for the magic buffer
                for (int i = 0; i < 64; i++) {
                  size_t idx = (region.offset + i) % (2 * buffer_size);
                  unsigned char current = ((unsigned char *)magic)[idx];

                  if (current != snapshot[i]) {
                    changed = true;
                    snapshot[i] = current;
                  }
                }
              } else {
                // Regular memory access
                unsigned char *ptr =
                    (unsigned char *)region.buffer + region.offset;
                for (int i = 0; i < 64; i++) {
                  if (ptr[i] != snapshot[i]) {
                    changed = true;
                    snapshot[i] = ptr[i];
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

                  if (region.name.find("Magic Wrap") != std::string::npos) {
                    for (int i = 0; i < 64; i++) {
                      if (i % 16 == 0) {
                        std::cout << "    ";
                      }
                      size_t idx = (region.offset + i) % (2 * buffer_size);
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
          }
        }

        // Slight delay to reduce CPU usage
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }

      // Print final change counts
      std::cout << "Monitor thread: Final change counts:" << std::endl;
      {
        std::lock_guard<std::mutex> lock(monitor_mutex);
        for (const auto &client_pair : client_regions) {
          int c = client_pair.first;
          const auto &regions = client_pair.second;

          std::cout << "Client " << c << ":" << std::endl;
          for (const auto &region : regions) {
            std::cout << "  " << region.name << ": " << region.changes
                      << " changes detected (last at "
                      << region.last_change_time << "ms)" << std::endl;
          }
        }
      }
    });

    // Wait for all clients to complete and signal
    std::vector<bool> clients_finished(expected_clients, false);
    int clients_done = 0;

    std::cout
        << "Server: Waiting for all clients to complete tests and signal..."
        << std::endl;

    while (clients_done < expected_clients) {
      for (int c = 0; c < expected_clients; c++) {
        if (clients_finished[c])
          continue;

        // Poll with timeout handling
        bool got_completion = false;
        auto wc_s = client_endpoints[c]->PollRecvCq();

        // Check if we got a valid completion
        if (wc_s.ok()) {
          clients_finished[c] = true;
          clients_done++;

          std::cout << "Server: Client " << c << " sent end signal"
                    << std::endl;
          // Check what the end signal contains
          std::cout << "End signal content from Client " << c << ": ";
          for (int i = 0; i < 4; ++i) {
            printf("0x%02x ", ((unsigned char *)wait_buffers[c])[i]);
          }
          std::cout << std::endl;
        }
      }

      // If we're still waiting for clients, sleep a bit
      if (clients_done < expected_clients) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }

    std::cout << "Server: All clients have completed" << std::endl;

    // Stop the monitoring thread
    stop_monitoring.store(true);
    monitor_thread.join();

    // Clean up resources
    for (int c = 0; c < expected_clients; c++) {
      ibv_dereg_mr(wait_mrs[c]);
      free(wait_buffers[c]);

      auto stat = client_endpoints[c]->Close();
      if (!stat.ok()) {
        std::cerr << "Error closing endpoint for client " << c << ": " << stat
                  << std::endl;
      }
    }

    ibv_dereg_mr(magic_mr);
    kym::ringbuffer::FreeMagicBuffer(magic, buffer_size);

    ibv_dereg_mr(generic_mr);
    free(generic);

    auto stat = ln->Close();
    if (!stat.ok()) {
      std::cerr << "Error closing listener " << stat << std::endl;
      return 1;
    }

    std::cout << "Server: Resources cleaned up and exiting" << std::endl;
  }

  if (is_client) {
    // ======================= CLIENT CODE =======================
    std::cout << "Client " << client_id << ": Connecting to server at " << ip
              << std::endl;

    auto ep_s = kym::endpoint::Dial(ip, 8987, opts);
    if (!ep_s.ok()) {
      std::cerr << "Error dialing " << ep_s.status() << std::endl;
      return 1;
    }
    kym::endpoint::Endpoint *ep = ep_s.value();
    std::cout << "Client " << client_id << ": Connected to server" << std::endl;

    struct cinfo *ci;
    ep->GetConnectionInfo((void **)&ci);
    std::cout << "Client " << client_id
              << ": Received memory region info from server" << std::endl;
    std::cout << "  Generic buffer at 0x" << std::hex << ci->generic_addr
              << std::dec << std::endl;
    std::cout << "  Magic buffer at 0x" << std::hex << ci->magic_addr
              << std::dec << std::endl;
    std::cout << "  Assigned client ID: " << ci->client_id << std::endl;

    // Calculate our section in the buffer based on client ID
    int buffer_size = 4 * 1024 * 1024;
    int expected_clients = 8; // Assume maximum of 8 clients
    int client_section_size = buffer_size / expected_clients;
    int client_section_start = ci->client_id * client_section_size;

    // Allocate send buffer and fill with recognizable pattern
    void *send = malloc(size);
    memset(send, 0x40 + client_id,
           size); // Fill with different pattern per client
    struct ibv_mr *send_mr =
        ibv_reg_mr(ep->GetPd(), send, size, IBV_ACCESS_LOCAL_WRITE);

    // Sleep a bit to ensure server is ready to monitor
    std::cout << "Client " << client_id
              << ": Sleeping for 2 seconds to ensure server is monitoring..."
              << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Calculate our specific offsets
    uint64_t generic_offset = client_section_start + client_section_size / 2;
    uint64_t magic_offset = client_section_start + client_section_size / 2;
    uint64_t magic_wrap_offset =
        client_section_start + client_section_size - 32;

    // Test latency for generic mr
    std::cout << "Client " << client_id
              << ": Starting generic buffer write test..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
      if (i % 100 == 0) {
        // Change first byte periodically to help server detect each write
        ((char *)send)[0] = 0x40 + client_id + (i / 100) % 10;
      }

      auto stat =
          ep->PostWrite(i, send_mr->lkey, send, size,
                        ci->generic_addr + generic_offset, ci->generic_key);
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
    std::cout << "Client " << client_id << ": Generic buffer test complete"
              << std::endl;
    std::cout << "Mean write latency generic mr msg size " << size
              << " bytes: " << dur / (double)iterations << " µs" << std::endl;

    // Sleep between tests to help server distinguish them
    std::cout << "Client " << client_id
              << ": Sleeping for 1 second between tests..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Modify send buffer to help server detect new test
    ((char *)send)[0] = 0x50 + client_id;

    // Test latency for magic mr
    std::cout << "Client " << client_id
              << ": Starting magic buffer middle write test..." << std::endl;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
      if (i % 100 == 0) {
        // Change first byte periodically to help server detect each write
        ((char *)send)[0] = 0x50 + client_id + (i / 100) % 10;
      }

      auto stat = ep->PostWrite(i, send_mr->lkey, send, size,
                                ci->magic_addr + magic_offset, ci->magic_key);
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
    std::cout << "Client " << client_id << ": Magic buffer middle test complete"
              << std::endl;
    std::cout << "Mean write latency magic mr msg size " << size
              << " bytes: " << dur / (double)iterations << " µs" << std::endl;

    // Sleep between tests to help server distinguish them
    std::cout << "Client " << client_id
              << ": Sleeping for 1 second between tests..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Modify send buffer again for the final test
    ((char *)send)[0] = 0x60 + client_id;

    // Test latency for magic mr over end of buffer
    std::cout << "Client " << client_id
              << ": Starting magic buffer wrap write test..." << std::endl;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
      if (i % 100 == 0) {
        // Change first byte periodically to help server detect each write
        ((char *)send)[0] = 0x60 + client_id + (i / 100) % 10;
      }

      auto stat =
          ep->PostWrite(i, send_mr->lkey, send, size,
                        ci->magic_addr + magic_wrap_offset, ci->magic_key);
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
    std::cout << "Client " << client_id << ": Magic buffer wrap test complete"
              << std::endl;
    std::cout << "Mean write latency magic mr msg size " << size
              << " bytes: " << dur / (double)iterations << " µs" << std::endl;

    // Sleep before sending end signal
    std::cout << "Client " << client_id
              << ": Sleeping for 1 second before sending end signal..."
              << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Send end signal to server
    std::cout << "Client " << client_id << ": Sending end signal to server..."
              << std::endl;

    // Convert 4-byte data to 32-bit immediate value for PostImmidate
    unsigned char signal[4] = {0xAA, (unsigned char)client_id, 0xBB, 0xCC};
    uint32_t immediate_value =
        (signal[0] << 24) | (signal[1] << 16) | (signal[2] << 8) | signal[3];

    auto stat = ep->PostImmidate(1, immediate_value);
    if (!stat.ok()) {
      std::cerr << "Error sending end signal: " << stat << std::endl;
      return 1;
    }
    auto wc_s = ep->PollSendCq();
    if (!wc_s.ok()) {
      std::cerr << "Error polling send CQ for end signal: " << wc_s.status()
                << std::endl;
    }

    std::cout << "Client " << client_id << ": Clean up and exit" << std::endl;

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
