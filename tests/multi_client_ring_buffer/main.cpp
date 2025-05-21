#include "conn.hpp"
#include "cxxopts.hpp"
#include "endpoint.hpp"
#include "ring_buffer/magic_buffer.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <infiniband/verbs.h>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Improved QP Options for multiple clients
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
        cxxopts::value<int>()->default_value("64"))(
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

// Helper to print buffer contents
void print_buffer(const char *label, void *ptr, size_t size) {
  unsigned char *buf = (unsigned char *)ptr;
  std::cout << label << " (first " << std::min(size, (size_t)64)
            << " bytes):" << std::endl;

  for (size_t i = 0; i < std::min(size, (size_t)64); i++) {
    if (i % 16 == 0)
      std::cout << "  ";
    printf("%02x ", buf[i]);
    if ((i + 1) % 16 == 0)
      std::cout << std::endl;
  }
  if (size > 64)
    std::cout << "  ... (truncated)" << std::endl;
  else if (size % 16 != 0)
    std::cout << std::endl;
}

// Structure for connection info
struct cinfo {
  uint32_t generic_key;
  uint64_t generic_addr;
  uint32_t magic_key;
  uint64_t magic_addr;
  uint32_t client_id;   // Added client identifier
  uint32_t buffer_size; // Total buffer size
};

// Client data structure
struct ClientInfo {
  kym::endpoint::Endpoint *endpoint;
  void *recv_buffer;
  ibv_mr *recv_mr;
  int client_id;
  bool finished;
  std::string client_name;
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
  int msg_size = flags["size"].as<int>();

  if (is_server) {
    // ======================= SERVER CODE =======================
    std::cout << "Server: Starting on " << ip << std::endl;
    std::cout << "Server: Expecting " << expected_clients << " clients"
              << std::endl;

    auto ln_s = kym::endpoint::Listen(ip, 9999);
    if (!ln_s.ok()) {
      std::cerr << "Error listening: " << ln_s.status() << std::endl;
      return 1;
    }
    auto ln = ln_s.value();

    // Allocate generic buffer - shared by all clients
    int buffer_size = 4 * 1024 * 1024; // 4MB
    void *generic = malloc(buffer_size);
    memset(generic, 0xAA, buffer_size);
    struct ibv_mr *generic_mr =
        ibv_reg_mr(ln->GetPd(), generic, buffer_size,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!generic_mr) {
      std::cerr << "Failed to register generic MR: " << strerror(errno)
                << std::endl;
      return 1;
    }

    // Allocate magic buffer - shared by all clients
    auto magic_s = kym::ringbuffer::GetMagicBuffer(buffer_size);
    if (!magic_s.ok()) {
      std::cerr << "Error allocating magic buffer: " << magic_s.status()
                << std::endl;
      return 1;
    }
    void *magic = magic_s.value();
    memset(magic, 0xAA, 2 * buffer_size);
    struct ibv_mr *magic_mr =
        ibv_reg_mr(ln->GetPd(), magic, 2 * buffer_size,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!magic_mr) {
      std::cerr << "Failed to register magic MR: " << strerror(errno)
                << std::endl;
      return 1;
    }

    std::cout << "Server: Memory regions registered:" << std::endl;
    std::cout << "  Generic buffer at 0x" << std::hex << (uint64_t)generic
              << std::dec << " size: " << buffer_size << " bytes" << std::endl;
    std::cout << "  Magic buffer at 0x" << std::hex << (uint64_t)magic
              << std::dec << " size: " << 2 * buffer_size << " bytes"
              << std::endl;

    // Print initial buffer content
    print_buffer("Server: Initial generic buffer", generic, 64);
    print_buffer("Server: Initial magic buffer", magic, 64);

    // Store client information
    std::vector<ClientInfo> clients;

    // Set up monitoring
    std::atomic<bool> stop_monitor{false};
    std::mutex monitor_mutex;

    // Track total changes
    struct BufferStats {
      int total_changes;
      std::map<int, int> changes_per_client;
    };

    BufferStats generic_stats = {0, {}};
    BufferStats magic_stats = {0, {}};

    // Start monitoring thread
    std::thread monitor_thread([&generic_stats, &magic_stats, &monitor_mutex,
                                &stop_monitor, generic, magic, buffer_size,
                                expected_clients]() {
      std::cout << "Monitor: Started monitoring buffers" << std::endl;

      // Create snapshots for comparison
      unsigned char *generic_snapshot = new unsigned char[buffer_size];
      unsigned char *magic_snapshot = new unsigned char[2 * buffer_size];

      memcpy(generic_snapshot, generic, buffer_size);
      memcpy(magic_snapshot, magic, 2 * buffer_size);

      auto start_time = std::chrono::high_resolution_clock::now();

      // Per-client monitoring regions (calculated based on expected clients)
      int section_size = buffer_size / expected_clients;
      std::vector<std::pair<size_t, size_t>> client_regions;

      for (int c = 0; c < expected_clients; c++) {
        size_t start = c * section_size;
        size_t end = (c + 1) * section_size;
        client_regions.push_back(std::make_pair(start, end));

        std::cout << "Monitor: Client " << c << " region: offset " << start
                  << " to " << end << std::endl;

        // Initialize change counters
        generic_stats.changes_per_client[c] = 0;
        magic_stats.changes_per_client[c] = 0;
      }

      const int MAX_CHANGES_TO_PRINT =
          5; // Limit printed changes to avoid flooding console

      while (!stop_monitor.load()) {
        auto now = std::chrono::high_resolution_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - start_time)
                              .count();

        // Check generic buffer for changes
        for (int c = 0; c < expected_clients; c++) {
          size_t start = client_regions[c].first;
          size_t end = client_regions[c].second;

          for (size_t i = start; i < end; i++) {
            if (((unsigned char *)generic)[i] != generic_snapshot[i]) {
              // Found a change
              generic_stats.total_changes++;
              generic_stats.changes_per_client[c]++;

              // Update snapshot
              generic_snapshot[i] = ((unsigned char *)generic)[i];

              // Print first few changes
              if (generic_stats.changes_per_client[c] <= MAX_CHANGES_TO_PRINT) {
                std::lock_guard<std::mutex> lock(monitor_mutex);
                std::cout << "Monitor: Generic buffer change for client " << c
                          << " at offset " << i << " (t=" << elapsed_ms << "ms)"
                          << std::endl;

                // Print surrounding bytes
                size_t print_start = (i >= 16) ? i - 16 : 0;
                size_t print_len = std::min((size_t)32, end - print_start);

                std::cout << "  Changed region:" << std::endl << "  ";
                for (size_t j = 0; j < print_len; j++) {
                  printf("%02x ", ((unsigned char *)generic)[print_start + j]);
                  if ((j + 1) % 16 == 0 && j < print_len - 1)
                    std::cout << std::endl << "  ";
                }
                std::cout << std::endl;
              }

              // Only process one change at a time to avoid flooding
              break;
            }
          }
        }

        // Check magic buffer for changes
        for (int c = 0; c < expected_clients; c++) {
          size_t start = client_regions[c].first;
          size_t end = client_regions[c].second;

          for (size_t i = start; i < end; i++) {
            if (((unsigned char *)magic)[i] != magic_snapshot[i]) {
              // Found a change
              magic_stats.total_changes++;
              magic_stats.changes_per_client[c]++;

              // Update snapshot
              magic_snapshot[i] = ((unsigned char *)magic)[i];

              // Print first few changes
              if (magic_stats.changes_per_client[c] <= MAX_CHANGES_TO_PRINT) {
                std::lock_guard<std::mutex> lock(monitor_mutex);
                std::cout << "Monitor: Magic buffer change for client " << c
                          << " at offset " << i << " (t=" << elapsed_ms << "ms)"
                          << std::endl;

                // Print surrounding bytes
                size_t print_start = (i >= 16) ? i - 16 : 0;
                size_t print_len = std::min((size_t)32, end - print_start);

                std::cout << "  Changed region:" << std::endl << "  ";
                for (size_t j = 0; j < print_len; j++) {
                  printf("%02x ", ((unsigned char *)magic)[print_start + j]);
                  if ((j + 1) % 16 == 0 && j < print_len - 1)
                    std::cout << std::endl << "  ";
                }
                std::cout << std::endl;
              }

              // Only process one change at a time to avoid flooding
              break;
            }
          }
        }

        // Sleep to reduce CPU usage
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }

      // Print final statistics
      {
        std::lock_guard<std::mutex> lock(monitor_mutex);
        std::cout << "Monitor: Monitoring complete" << std::endl;
        std::cout << "Generic buffer: " << generic_stats.total_changes
                  << " total changes" << std::endl;
        std::cout << "Magic buffer: " << magic_stats.total_changes
                  << " total changes" << std::endl;

        std::cout << "Changes per client:" << std::endl;
        for (int c = 0; c < expected_clients; c++) {
          std::cout << "  Client " << c << ": "
                    << generic_stats.changes_per_client[c] << " generic, "
                    << magic_stats.changes_per_client[c] << " magic"
                    << std::endl;
        }
      }

      delete[] generic_snapshot;
      delete[] magic_snapshot;
    });

    // Accept client connections
    for (int c = 0; c < expected_clients; c++) {
      std::cout << "Server: Waiting for client " << c + 1 << "/"
                << expected_clients << "..." << std::endl;

      // Prepare connection info
      struct cinfo ci;
      ci.generic_addr = (uint64_t)generic;
      ci.generic_key = generic_mr->lkey;
      ci.magic_addr = (uint64_t)magic;
      ci.magic_key = magic_mr->lkey;
      ci.client_id = c;
      ci.buffer_size = buffer_size;

      opts.private_data = &ci;
      opts.private_data_len = sizeof(ci);

      auto ep_s = ln->Accept(opts);
      if (!ep_s.ok()) {
        std::cerr << "Error accepting connection: " << ep_s.status()
                  << std::endl;
        continue; // Try next client instead of failing
      }

      // Create receive buffer for client signals
      void *recv_buf = malloc(msg_size);
      memset(recv_buf, 0, msg_size);
      struct ibv_mr *recv_mr =
          ibv_reg_mr(ln->GetPd(), recv_buf, msg_size, IBV_ACCESS_LOCAL_WRITE);

      // Post receive for client completion signal
      auto post_stat =
          ep_s.value()->PostRecv(c + 1, recv_mr->lkey, recv_buf, msg_size);
      if (!post_stat.ok()) {
        std::cerr << "Error posting receive for client " << c << ": "
                  << post_stat << std::endl;
        free(recv_buf);
        ibv_dereg_mr(recv_mr);
        ep_s.value()->Close();
        continue; // Try next client
      }

      // Add client to our list
      ClientInfo client;
      client.endpoint = ep_s.value();
      client.recv_buffer = recv_buf;
      client.recv_mr = recv_mr;
      client.client_id = c;
      client.finished = false;
      client.client_name = "Client " + std::to_string(c);

      clients.push_back(client);

      std::cout << "Server: " << client.client_name << " connected"
                << std::endl;
    }

    std::cout << "Server: All " << clients.size() << " clients connected"
              << std::endl;

    // Wait for all clients to complete
    int clients_done = 0;

    while (clients_done < clients.size()) {
      for (size_t c = 0; c < clients.size(); c++) {
        if (clients[c].finished)
          continue;

        // Check if client has sent completion signal (non-blocking poll)
        auto wc_s = clients[c].endpoint->PollRecvCq();
        if (wc_s.ok()) {
          clients[c].finished = true;
          clients_done++;

          std::cout << "Server: " << clients[c].client_name << " completed test"
                    << std::endl;
          print_buffer("  Signal content", clients[c].recv_buffer, msg_size);
        }
      }

      // Sleep a bit to reduce CPU usage
      if (clients_done < clients.size()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }

    std::cout << "Server: All clients completed their tests" << std::endl;

    // Stop monitoring and wait for thread to exit
    stop_monitor.store(true);
    monitor_thread.join();

    // Print final buffer state
    print_buffer("Server: Final generic buffer", generic, 64);
    print_buffer("Server: Final magic buffer", magic, 64);

    // Clean up client resources
    for (auto &client : clients) {
      ibv_dereg_mr(client.recv_mr);
      free(client.recv_buffer);
      client.endpoint->Close();
    }

    // Clean up shared resources
    ibv_dereg_mr(magic_mr);
    kym::ringbuffer::FreeMagicBuffer(magic, buffer_size);
    ibv_dereg_mr(generic_mr);
    free(generic);

    ln->Close();
    std::cout << "Server: Cleanup complete, exiting" << std::endl;
  }

  if (is_client) {
    // ======================= CLIENT CODE =======================
    std::cout << "Client " << client_id << ": Connecting to server at " << ip
              << std::endl;

    auto ep_s = kym::endpoint::Dial(ip, 9999, opts);
    if (!ep_s.ok()) {
      std::cerr << "Error connecting to server: " << ep_s.status() << std::endl;
      return 1;
    }
    kym::endpoint::Endpoint *ep = ep_s.value();
    std::cout << "Client " << client_id << ": Connected to server" << std::endl;

    // Get connection info from server
    struct cinfo *ci;
    ep->GetConnectionInfo((void **)&ci);

    std::cout << "Client " << client_id
              << ": Received buffer info from server:" << std::endl;
    std::cout << "  Generic buffer at 0x" << std::hex << ci->generic_addr
              << std::dec << ", key: " << ci->generic_key << std::endl;
    std::cout << "  Magic buffer at 0x" << std::hex << ci->magic_addr
              << std::dec << ", key: " << ci->magic_key << std::endl;
    std::cout << "  Assigned client ID: " << ci->client_id << std::endl;
    std::cout << "  Buffer size: " << ci->buffer_size << std::endl;

    // Calculate our section in the shared buffer
    int buffer_size = ci->buffer_size;
    int client_section_size = buffer_size / 8; // Assume maximum 8 clients
    int client_section_start = ci->client_id * client_section_size;

    // Calculate specific offsets for our tests
    uint64_t generic_offset = client_section_start + client_section_size / 2;
    uint64_t magic_offset = client_section_start + client_section_size / 2;
    uint64_t magic_wrap_offset =
        client_section_start + client_section_size - 32;

    std::cout << "Client " << client_id << ": Using offsets:" << std::endl;
    std::cout << "  Generic test: offset " << generic_offset << std::endl;
    std::cout << "  Magic test: offset " << magic_offset << std::endl;
    std::cout << "  Magic wrap test: offset " << magic_wrap_offset << std::endl;

    // Prepare send buffer with client-specific pattern
    void *send_buf = malloc(msg_size);
    for (int i = 0; i < msg_size; i++) {
      ((char *)send_buf)[i] =
          0x40 + client_id + (i % 26); // Client-specific pattern
    }

    struct ibv_mr *send_mr =
        ibv_reg_mr(ep->GetPd(), send_buf, msg_size, IBV_ACCESS_LOCAL_WRITE);
    if (!send_mr) {
      std::cerr << "Failed to register send buffer MR: " << strerror(errno)
                << std::endl;
      return 1;
    }

    std::cout << "Client " << client_id << ": Send buffer prepared"
              << std::endl;
    print_buffer("Client: Send buffer content", send_buf, msg_size);

    // Sleep to ensure server is monitoring
    std::cout << "Client " << client_id
              << ": Waiting 2 seconds before starting tests..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // First test: Generic buffer
    std::cout << "Client " << client_id << ": Starting generic buffer test ("
              << iterations << " iterations)..." << std::endl;

    auto start = std::chrono::high_resolution_clock::now();
    int successful_writes = 0;

    for (int i = 0; i < iterations; i++) {
      // Change first byte to indicate iteration
      ((char *)send_buf)[0] = 0x40 + client_id + (i % 10);

      // Post RDMA write
      auto write_stat =
          ep->PostWrite(i, send_mr->lkey, send_buf, msg_size,
                        ci->generic_addr + generic_offset, ci->generic_key);
      if (!write_stat.ok()) {
        std::cerr << "Error posting generic write #" << i << ": " << write_stat
                  << std::endl;
        continue;
      }

      // Wait for completion
      auto wc_s = ep->PollSendCq();
      if (!wc_s.ok()) {
        std::cerr << "Error polling send CQ for generic write #" << i << ": "
                  << wc_s.status() << std::endl;
      } else {
        successful_writes++;
      }

      // Progress updates
      if (i % 100 == 0 || i == iterations - 1) {
        std::cout << "Client " << client_id
                  << ": Generic test progress: " << i + 1 << "/" << iterations
                  << std::endl;
      }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();

    std::cout << "Client " << client_id << ": Generic buffer test complete"
              << std::endl;
    std::cout << "  " << successful_writes << "/" << iterations
              << " writes succeeded" << std::endl;
    if (successful_writes > 0) {
      std::cout << "  Average latency: " << duration / (double)successful_writes
                << " µs" << std::endl;
    }

    // Sleep between tests
    std::cout << "Client " << client_id
              << ": Waiting 1 second before next test..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Second test: Magic buffer
    std::cout << "Client " << client_id << ": Starting magic buffer test ("
              << iterations << " iterations)..." << std::endl;

    start = std::chrono::high_resolution_clock::now();
    successful_writes = 0;

    for (int i = 0; i < iterations; i++) {
      // Change first byte to indicate iteration
      ((char *)send_buf)[0] = 0x50 + client_id + (i % 10);

      // Post RDMA write
      auto write_stat =
          ep->PostWrite(i, send_mr->lkey, send_buf, msg_size,
                        ci->magic_addr + magic_offset, ci->magic_key);
      if (!write_stat.ok()) {
        std::cerr << "Error posting magic write #" << i << ": " << write_stat
                  << std::endl;
        continue;
      }

      // Wait for completion
      auto wc_s = ep->PollSendCq();
      if (!wc_s.ok()) {
        std::cerr << "Error polling send CQ for magic write #" << i << ": "
                  << wc_s.status() << std::endl;
      } else {
        successful_writes++;
      }

      // Progress updates
      if (i % 100 == 0 || i == iterations - 1) {
        std::cout << "Client " << client_id
                  << ": Magic test progress: " << i + 1 << "/" << iterations
                  << std::endl;
      }
    }

    end = std::chrono::high_resolution_clock::now();
    duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();

    std::cout << "Client " << client_id << ": Magic buffer test complete"
              << std::endl;
    std::cout << "  " << successful_writes << "/" << iterations
              << " writes succeeded" << std::endl;
    if (successful_writes > 0) {
      std::cout << "  Average latency: " << duration / (double)successful_writes
                << " µs" << std::endl;
    }

    // Sleep between tests
    std::cout << "Client " << client_id
              << ": Waiting 1 second before next test..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Third test: Magic buffer wrap
    std::cout << "Client " << client_id << ": Starting magic buffer wrap test ("
              << iterations << " iterations)..." << std::endl;

    start = std::chrono::high_resolution_clock::now();
    successful_writes = 0;

    for (int i = 0; i < iterations; i++) {
      // Change first byte to indicate iteration
      ((char *)send_buf)[0] = 0x60 + client_id + (i % 10);

      // Post RDMA write
      auto write_stat =
          ep->PostWrite(i, send_mr->lkey, send_buf, msg_size,
                        ci->magic_addr + magic_wrap_offset, ci->magic_key);
      if (!write_stat.ok()) {
        std::cerr << "Error posting magic wrap write #" << i << ": "
                  << write_stat << std::endl;
        continue;
      }

      // Wait for completion
      auto wc_s = ep->PollSendCq();
      if (!wc_s.ok()) {
        std::cerr << "Error polling send CQ for magic wrap write #" << i << ": "
                  << wc_s.status() << std::endl;
      } else {
        successful_writes++;
      }

      // Progress updates
      if (i % 100 == 0 || i == iterations - 1) {
        std::cout << "Client " << client_id
                  << ": Magic wrap test progress: " << i + 1 << "/"
                  << iterations << std::endl;
      }
    }

    end = std::chrono::high_resolution_clock::now();
    duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();

    std::cout << "Client " << client_id << ": Magic buffer wrap test complete"
              << std::endl;
    std::cout << "  " << successful_writes << "/" << iterations
              << " writes succeeded" << std::endl;
    if (successful_writes > 0) {
      std::cout << "  Average latency: " << duration / (double)successful_writes
                << " µs" << std::endl;
    }

    // Send completion signal to server
    std::cout << "Client " << client_id
              << ": Sending completion signal to server..." << std::endl;

    // Prepare completion message with client ID
    memset(send_buf, 0xFF, msg_size);
    const char *done_msg = "DONE";
    memcpy(send_buf, done_msg, strlen(done_msg));
    ((char *)send_buf)[4] = (char)client_id; // Embed client ID

    auto send_stat = ep->PostSend(1, send_mr->lkey, send_buf, msg_size);
    if (!send_stat.ok()) {
      std::cerr << "Error sending completion signal: " << send_stat
                << std::endl;
    } else {
      auto wc_s = ep->PollSendCq();
      if (!wc_s.ok()) {
        std::cerr << "Error polling send CQ for completion signal: "
                  << wc_s.status() << std::endl;
      } else {
        std::cout << "Client " << client_id
                  << ": Completion signal sent successfully" << std::endl;
      }
    }

    // Clean up
    ibv_dereg_mr(send_mr);
    free(send_buf);
    ep->Close();

    std::cout << "Client " << client_id << ": Cleanup complete, exiting"
              << std::endl;
  }

  return 0;
}
