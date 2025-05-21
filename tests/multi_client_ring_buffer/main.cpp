#include "conn.hpp"
#include "cxxopts.hpp"
#include "endpoint.hpp"
#include "ring_buffer/magic_buffer.hpp"
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <infiniband/verbs.h>
#include <iostream>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// Global for signal handling
std::atomic<bool> g_running{true};

// Structure to hold message information
struct MessageInfo {
  uint32_t client_id;   // Client that sent the message
  uint32_t msg_counter; // Message sequence number
  char msg_type; // Type of message (e.g., 'A'=generic, 'B'=magic, 'C'=wrap)
  uint64_t timestamp; // When the message was sent
};

// Signal handler
void signal_handler(int signum) {
  std::cout << "Caught signal " << signum << ", cleaning up..." << std::endl;
  g_running.store(false);
}

// Optimized QP options
kym::endpoint::Options opts = {
    .qp_attr =
        {
            .cap =
                {
                    .max_send_wr = 128,
                    .max_recv_wr = 128,
                    .max_send_sge = 1,
                    .max_recv_sge = 1,
                    .max_inline_data = 8,
                },
            .qp_type = IBV_QPT_RC,
        },
    .responder_resources = 16,
    .initiator_depth = 16,
    .retry_count = 8,
    .rnr_retry_count = 7,
    .native_qp = false,
    .inline_recv = 0,
};

cxxopts::ParseResult parse(int argc, char *argv[]) {
  cxxopts::Options options(argv[0], "message-counting-ringbuffer");
  try {
    options.add_options()("client", "Act as client", cxxopts::value<bool>())(
        "server", "Act as server", cxxopts::value<bool>())(
        "i,address", "IP address to connect to", cxxopts::value<std::string>())(
        "s,size", "Size of message to exchange",
        cxxopts::value<int>()->default_value("128"))(
        "c,clients", "Number of expected clients (server only)",
        cxxopts::value<int>()->default_value("1"))(
        "n,iterations", "Number of messages to send",
        cxxopts::value<int>()->default_value("100"))(
        "client-id", "Client identifier (0-based, client only)",
        cxxopts::value<int>()->default_value("0"))(
        "msg-prefix", "Message prefix for this client",
        cxxopts::value<std::string>()->default_value(""))(
        "v,verbose", "Verbose output",
        cxxopts::value<bool>()->default_value("false"))("h,help",
                                                        "Print usage");

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
      std::cout << options.help() << std::endl;
      exit(0);
    }

    if (!result.count("address")) {
      std::cerr << "Specify an address with -i or --address" << std::endl;
      std::cerr << options.help() << std::endl;
      exit(1);
    }

    return result;
  } catch (const cxxopts::OptionException &e) {
    std::cerr << "Error parsing options: " << e.what() << std::endl;
    std::cerr << options.help() << std::endl;
    exit(1);
  }
}

// Print buffer contents
void print_buffer(const char *label, const void *ptr, size_t size) {
  const unsigned char *buf = static_cast<const unsigned char *>(ptr);
  std::cout << label << " (first " << std::min(size, (size_t)64)
            << " bytes):" << std::endl;

  for (size_t i = 0; i < std::min(size, (size_t)64); i++) {
    if (i % 16 == 0)
      std::cout << "  ";
    printf("%02x ", buf[i]);
    if ((i + 1) % 16 == 0)
      std::cout << std::endl;
  }
  if (size > 64) {
    std::cout << "  ... (truncated)" << std::endl;
  } else if (size % 16 != 0) {
    std::cout << std::endl;
  }

  // Also print as ASCII if possible
  std::cout << "  ASCII: \"";
  for (size_t i = 0; i < std::min(size, (size_t)64); i++) {
    char c = buf[i];
    if (isprint(c)) {
      std::cout << c;
    } else {
      std::cout << ".";
    }
  }
  std::cout << "\"" << std::endl;
}

// Connection info structure
struct cinfo {
  uint32_t generic_key;
  uint64_t generic_addr;
  uint32_t magic_key;
  uint64_t magic_addr;
  uint32_t client_id;
  uint32_t buffer_size;
  uint32_t num_clients;
  uint32_t msg_size;
};

// Client information
struct ClientInfo {
  kym::endpoint::Endpoint *endpoint;
  void *recv_buffer;
  ibv_mr *recv_mr;
  int client_id;
  bool finished;
  std::string name;
  uint32_t messages_sent;
  uint32_t messages_received;
};

// Message counter for monitoring
struct MessageCounter {
  std::mutex mutex;
  uint64_t total_messages;
  std::map<int, uint64_t> messages_per_client;
  std::map<int, std::set<uint32_t>> unique_msg_ids;
  std::map<int, uint64_t> first_msg_time;
  std::map<int, uint64_t> last_msg_time;

  MessageCounter() : total_messages(0) {}

  void record_message(int client_id, uint32_t msg_id, uint64_t timestamp) {
    std::lock_guard<std::mutex> lock(mutex);

    // Record total count
    total_messages++;

    // Record per-client count
    messages_per_client[client_id]++;

    // Track unique message IDs
    unique_msg_ids[client_id].insert(msg_id);

    // Track timing
    if (first_msg_time.find(client_id) == first_msg_time.end()) {
      first_msg_time[client_id] = timestamp;
    }
    last_msg_time[client_id] = timestamp;
  }

  void print_stats() {
    std::lock_guard<std::mutex> lock(mutex);

    std::cout << "====== Message Statistics ======" << std::endl;
    std::cout << "Total messages detected: " << total_messages << std::endl;
    std::cout << "Messages per client:" << std::endl;

    for (const auto &pair : messages_per_client) {
      int client_id = pair.first;
      uint64_t msg_count = pair.second;
      uint64_t unique_count = unique_msg_ids[client_id].size();

      std::cout << "  Client " << client_id << ": " << msg_count << " messages";

      // Check for duplicates
      if (unique_count != msg_count) {
        std::cout << " (" << unique_count << " unique, "
                  << (msg_count - unique_count) << " duplicates)";
      }

      // Calculate rate if timing info available
      if (first_msg_time.find(client_id) != first_msg_time.end() &&
          last_msg_time.find(client_id) != last_msg_time.end()) {
        uint64_t duration_ms =
            last_msg_time[client_id] - first_msg_time[client_id];
        if (duration_ms > 0) {
          double rate = (double)msg_count / (duration_ms / 1000.0);
          std::cout << ", rate: " << rate << " msgs/sec";
        }
      }

      std::cout << std::endl;
    }

    std::cout << "================================" << std::endl;
  }
};

int main(int argc, char *argv[]) {
  // Set up signal handler
  signal(SIGINT, signal_handler);

  std::cout << "#### Message Counting Ring Buffer ####" << std::endl;

  auto flags = parse(argc, argv);
  std::string ip = flags["address"].as<std::string>();
  bool verbose = flags["verbose"].as<bool>();

  bool is_server = flags["server"].as<bool>();
  bool is_client = flags["client"].as<bool>();
  int expected_clients = flags["clients"].as<int>();
  int iterations = flags["iterations"].as<int>();
  int client_id = flags["client-id"].as<int>();
  int msg_size = flags["size"].as<int>();
  std::string msg_prefix = flags["msg-prefix"].as<std::string>();

  // Use a default prefix if none provided
  if (msg_prefix.empty()) {
    msg_prefix = "Client" + std::to_string(client_id) + "-";
  }

  // Validate options
  if (!is_server && !is_client) {
    std::cerr << "Error: Must specify either --client or --server" << std::endl;
    return 1;
  }

  if (is_server && is_client) {
    std::cerr << "Error: Cannot be both client and server" << std::endl;
    return 1;
  }

  // ======================= SERVER CODE =======================
  if (is_server) {
    std::cout << "Server: Starting on " << ip << std::endl;
    std::cout << "Server: Expecting " << expected_clients << " clients"
              << std::endl;

    auto ln_s = kym::endpoint::Listen(ip, 9999);
    if (!ln_s.ok()) {
      std::cerr << "Error listening: " << ln_s.status().message() << std::endl;
      return 1;
    }
    auto ln = ln_s.value();

    // Allocate buffer
    int buffer_size = 8 * 1024 * 1024; // 8MB buffer
    void *buffer = malloc(buffer_size);
    if (!buffer) {
      std::cerr << "Error allocating buffer: " << strerror(errno) << std::endl;
      return 1;
    }
    memset(buffer, 0, buffer_size); // Initialize with zeroes

    // Register for RDMA use
    struct ibv_mr *buffer_mr =
        ibv_reg_mr(ln->GetPd(), buffer, buffer_size,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!buffer_mr) {
      std::cerr << "Error registering buffer: " << strerror(errno) << std::endl;
      free(buffer);
      return 1;
    }

    std::cout << "Server: Buffer registered at 0x" << std::hex
              << (uint64_t)buffer << std::dec << " size: " << buffer_size
              << " bytes" << std::endl;

    // Setup for monitoring
    std::atomic<bool> stop_monitor{false};
    std::mutex monitor_mutex;

    // Message counting
    MessageCounter message_counter;

    // Client tracking
    std::vector<ClientInfo> clients;

    // Start monitoring thread
    std::thread monitor_thread([&message_counter, &stop_monitor, buffer,
                                buffer_size, expected_clients, verbose]() {
      std::cout << "Monitor: Started monitoring messages" << std::endl;

      // Take snapshots of initial buffer state
      std::vector<char> snapshot(buffer_size, 0);
      memcpy(snapshot.data(), buffer, buffer_size);

      auto start_time = std::chrono::high_resolution_clock::now();

      // Calculate each client's region
      std::vector<std::pair<size_t, size_t>> client_regions;
      int client_section = buffer_size / expected_clients;

      for (int c = 0; c < expected_clients; c++) {
        size_t start = c * client_section;
        size_t end = (c == expected_clients - 1) ? buffer_size
                                                 : (c + 1) * client_section;
        client_regions.push_back(std::make_pair(start, end));

        std::cout << "Monitor: Client " << c << " region: offset " << start
                  << " to " << end << std::endl;
      }

      // Define message header layout
      struct MessageHeader {
        uint32_t client_id;
        uint32_t msg_id;
        uint64_t timestamp;
        char msg_type;
      };

      // Monitoring loop
      while (!stop_monitor.load() && g_running.load()) {
        auto now = std::chrono::high_resolution_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - start_time)
                              .count();

        // Check each client's region
        for (int c = 0; c < expected_clients; c++) {
          size_t start = client_regions[c].first;
          size_t end = client_regions[c].second;

          // Scan the client's region for changes
          for (size_t pos = start; pos + sizeof(MessageHeader) <= end;
               pos += 128) {
            // Check if this position has changed
            if (memcmp(&snapshot[pos], (char *)buffer + pos,
                       sizeof(MessageHeader)) != 0) {
              // Found a potential message
              MessageHeader *header = (MessageHeader *)((char *)buffer + pos);

              // Validate header
              if (header->client_id < expected_clients &&
                  header->msg_id < 1000000 && // Sanity check
                  (header->msg_type == 'A' || header->msg_type == 'B' ||
                   header->msg_type == 'C')) {

                // We found a valid message - update our snapshot
                memcpy(&snapshot[pos], (char *)buffer + pos,
                       sizeof(MessageHeader));

                // Count the message
                message_counter.record_message(header->client_id,
                                               header->msg_id, elapsed_ms);

                if (verbose) {
                  std::cout << "Monitor: Found message at offset " << pos
                            << " from client " << header->client_id
                            << " id=" << header->msg_id
                            << " type=" << header->msg_type
                            << " time=" << header->timestamp << std::endl;

                  // Print message content
                  char *msg_data = (char *)buffer + pos + sizeof(MessageHeader);
                  size_t avail_len =
                      std::min((size_t)64, end - pos - sizeof(MessageHeader));
                  std::cout << "  Message content: \"";
                  for (size_t i = 0; i < avail_len; i++) {
                    char c = msg_data[i];
                    if (c == 0)
                      break; // End of string
                    std::cout << (isprint(c) ? c : '.');
                  }
                  std::cout << "\"" << std::endl;
                }

                // Skip ahead to avoid re-processing the same message
                pos += 127; // +1 will be added by loop increment
              }
            }
          }
        }

        // Sleep to reduce CPU usage
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }

      // Print final statistics
      message_counter.print_stats();
    });

    // Accept connections from clients
    for (int c = 0; c < expected_clients; c++) {
      std::cout << "Server: Waiting for client " << (c + 1) << "/"
                << expected_clients << "..." << std::endl;

      // Setup connection info for this client
      struct cinfo ci;
      ci.generic_addr = (uint64_t)buffer;
      ci.generic_key = buffer_mr->lkey;
      ci.magic_addr = 0; // No magic buffer in this implementation
      ci.magic_key = 0;
      ci.client_id = c;
      ci.buffer_size = buffer_size;
      ci.num_clients = expected_clients;
      ci.msg_size = msg_size;

      opts.private_data = &ci;
      opts.private_data_len = sizeof(ci);

      // Wait for connection
      auto ep_s = ln->Accept(opts);
      if (!ep_s.ok()) {
        std::cerr << "Error accepting client " << c << ": "
                  << ep_s.status().message() << std::endl;
        continue;
      }

      // Prepare to receive completion signal
      void *recv_buf = malloc(msg_size);
      if (!recv_buf) {
        std::cerr << "Failed to allocate receive buffer for client " << c
                  << std::endl;
        ep_s.value()->Close();
        continue;
      }

      memset(recv_buf, 0, msg_size);

      // Register receive buffer
      struct ibv_mr *recv_mr =
          ibv_reg_mr(ln->GetPd(), recv_buf, msg_size, IBV_ACCESS_LOCAL_WRITE);
      if (!recv_mr) {
        std::cerr << "Failed to register receive MR for client " << c << ": "
                  << strerror(errno) << std::endl;
        free(recv_buf);
        ep_s.value()->Close();
        continue;
      }

      // Post receive for completion signal
      auto post_stat =
          ep_s.value()->PostRecv(c + 1, recv_mr->lkey, recv_buf, msg_size);
      if (!post_stat.ok()) {
        std::cerr << "Error posting receive for client " << c << ": "
                  << post_stat.message() << std::endl;
        ibv_dereg_mr(recv_mr);
        free(recv_buf);
        ep_s.value()->Close();
        continue;
      }

      // Add client to our list
      ClientInfo client;
      client.endpoint = ep_s.value();
      client.recv_buffer = recv_buf;
      client.recv_mr = recv_mr;
      client.client_id = c;
      client.finished = false;
      client.name = "Client " + std::to_string(c);
      client.messages_sent = 0;
      client.messages_received = 0;

      clients.push_back(client);

      std::cout << "Server: " << client.name << " connected" << std::endl;
    }

    std::cout << "Server: All " << clients.size() << " clients connected"
              << std::endl;

    // Print buffer state after all clients connect
    print_buffer("Server: Initial buffer state", buffer, 64);

    // Wait for clients to complete
    int clients_done = 0;

    while (clients_done < (int)clients.size() && g_running.load()) {
      for (size_t c = 0; c < clients.size(); c++) {
        if (clients[c].finished)
          continue;

        // Try to poll for completion - this will block
        auto wc_s = clients[c].endpoint->PollRecvCq();
        if (wc_s.ok()) {
          clients[c].finished = true;
          clients_done++;

          std::cout << "Server: " << clients[c].name << " completed test"
                    << std::endl;
          print_buffer("  Signal content", clients[c].recv_buffer, msg_size);

          // Parse message count from signal
          struct {
            char signature[8];
            uint32_t client_id;
            uint32_t msg_count;
          } *signal;

          signal = (decltype(signal))clients[c].recv_buffer;

          std::cout << "  Client " << signal->client_id << " reports sending "
                    << signal->msg_count << " messages" << std::endl;

          clients[c].messages_sent = signal->msg_count;
        }
      }

      // If we're still waiting, sleep briefly
      if (clients_done < (int)clients.size()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }

    if (g_running.load()) {
      std::cout << "Server: All clients completed their tests" << std::endl;
    } else {
      std::cout << "Server: Interrupted, " << clients_done << "/"
                << clients.size() << " clients completed" << std::endl;
    }

    // Stop monitoring
    stop_monitor.store(true);
    monitor_thread.join();

    // Print buffer state after clients finish
    print_buffer("Server: Final buffer state", buffer, 64);

    // Compare reported vs detected message counts
    std::cout << "====== Message Count Verification ======" << std::endl;
    std::cout << "Client-reported message counts:" << std::endl;
    uint32_t total_reported = 0;

    for (const auto &client : clients) {
      std::cout << "  " << client.name << ": " << client.messages_sent
                << std::endl;
      total_reported += client.messages_sent;
    }

    std::cout << "Total reported messages: " << total_reported << std::endl;
    std::cout << "Total detected messages: " << message_counter.total_messages
              << std::endl;

    if (total_reported != message_counter.total_messages) {
      std::cout << "WARNING: Message count mismatch! Difference: "
                << (total_reported > message_counter.total_messages
                        ? total_reported - message_counter.total_messages
                        : message_counter.total_messages - total_reported)
                << std::endl;
    } else {
      std::cout << "SUCCESS: Message counts match perfectly!" << std::endl;
    }
    std::cout << "========================================" << std::endl;

    // Clean up client resources
    for (auto &client : clients) {
      ibv_dereg_mr(client.recv_mr);
      free(client.recv_buffer);
      client.endpoint->Close();
    }

    // Clean up shared resources
    ibv_dereg_mr(buffer_mr);
    free(buffer);

    ln->Close();

    std::cout << "Server: Cleanup complete, exiting" << std::endl;
  }

  // ======================= CLIENT CODE =======================
  if (is_client) {
    std::cout << "Client " << client_id << ": Connecting to server at " << ip
              << std::endl;

    auto ep_s = kym::endpoint::Dial(ip, 9999, opts);
    if (!ep_s.ok()) {
      std::cerr << "Error connecting to server: " << ep_s.status().message()
                << std::endl;
      return 1;
    }
    kym::endpoint::Endpoint *ep = ep_s.value();
    std::cout << "Client " << client_id << ": Connected to server" << std::endl;

    // Get connection info from server
    struct cinfo *ci;
    ep->GetConnectionInfo((void **)&ci);

    // Print connection info
    std::cout << "Client " << client_id
              << ": Received connection info:" << std::endl;
    std::cout << "  Buffer: addr=0x" << std::hex << ci->generic_addr
              << ", key=" << std::dec << ci->generic_key << std::endl;
    std::cout << "  Assigned client ID: " << ci->client_id << std::endl;
    std::cout << "  Buffer size: " << ci->buffer_size << " bytes" << std::endl;
    std::cout << "  Number of clients: " << ci->num_clients << std::endl;
    std::cout << "  Message size: " << ci->msg_size << " bytes" << std::endl;

    // Verify received client ID matches expected ID
    if ((uint32_t)ci->client_id != (uint32_t)client_id) {
      std::cout << "Warning: Assigned client ID " << ci->client_id
                << " differs from requested ID " << client_id << std::endl;
      // Use the server-assigned ID
      client_id = ci->client_id;
      std::cout << "Using server-assigned client ID: " << client_id
                << std::endl;
    }

    // Calculate our section in the shared buffer
    int buffer_size = ci->buffer_size;
    int client_section_size = buffer_size / ci->num_clients;
    int client_section_start = client_id * client_section_size;

    std::cout << "Client " << client_id << ": Buffer section:" << std::endl;
    std::cout << "  Section start: " << client_section_start << std::endl;
    std::cout << "  Section size: " << client_section_size << std::endl;

    // Prepare message buffer
    char *msg_buf = new char[ci->msg_size];
    memset(msg_buf, 0, ci->msg_size);

    // Register the buffer for RDMA
    struct ibv_mr *msg_mr =
        ibv_reg_mr(ep->GetPd(), msg_buf, ci->msg_size, IBV_ACCESS_LOCAL_WRITE);
    if (!msg_mr) {
      std::cerr << "Failed to register message buffer: " << strerror(errno)
                << std::endl;
      delete[] msg_buf;
      ep->Close();
      return 1;
    }

    std::cout << "Client " << client_id << ": Message buffer registered"
              << std::endl;

    // Define message header layout
    struct MessageHeader {
      uint32_t client_id;
      uint32_t msg_id;
      uint64_t timestamp;
      char msg_type;
    };

    // Ensure message size is large enough for header and some content
    if (ci->msg_size < sizeof(MessageHeader) + 32) {
      std::cerr << "Error: Message size too small, need at least "
                << (sizeof(MessageHeader) + 32) << " bytes" << std::endl;
      ibv_dereg_mr(msg_mr);
      delete[] msg_buf;
      ep->Close();
      return 1;
    }

    // Wait for server to be ready
    std::cout << "Client " << client_id
              << ": Waiting 2 seconds before starting..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Start sending messages
    std::cout << "Client " << client_id << ": Sending " << iterations
              << " messages with prefix '" << msg_prefix << "'" << std::endl;

    auto start_time = std::chrono::high_resolution_clock::now();
    int successful_msgs = 0;

    for (int i = 0; i < iterations && g_running.load(); i++) {
      // Prepare message with header
      MessageHeader *header = reinterpret_cast<MessageHeader *>(msg_buf);
      header->client_id = client_id;
      header->msg_id = i;
      header->timestamp =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count();
      header->msg_type = 'A' + (i % 3); // 'A', 'B', or 'C'

      // Add message content
      char *content = msg_buf + sizeof(MessageHeader);
      snprintf(content, ci->msg_size - sizeof(MessageHeader), "%s-MSG%d-%c",
               msg_prefix.c_str(), i, header->msg_type);

      // Calculate target offset - spread messages through our section
      uint64_t offset = client_section_start +
                        (i * 128) % (client_section_size - ci->msg_size);

      if (verbose) {
        std::cout << "Client " << client_id << ": Writing message " << i
                  << " to offset " << offset << std::endl;
      }

      // Perform RDMA write
      auto write_stat =
          ep->PostWrite(i, msg_mr->lkey, msg_buf, ci->msg_size,
                        ci->generic_addr + offset, ci->generic_key);

      if (!write_stat.ok()) {
        std::cerr << "Error posting write for message " << i << ": "
                  << write_stat.message() << std::endl;
        continue;
      }

      // Wait for completion
      auto wc_s = ep->PollSendCq();
      if (!wc_s.ok()) {
        std::cerr << "Error completing write for message " << i << ": "
                  << wc_s.status().message() << std::endl;
      } else {
        successful_msgs++;
      }

      // Progress updates
      if (i % 10 == 0 || i == iterations - 1) {
        std::cout << "Client " << client_id << ": Progress: " << i + 1 << "/"
                  << iterations << " (" << (i + 1) * 100 / iterations << "%)"
                  << std::endl;
      }

      // Brief delay between messages
      if (i % 10 == 0 && i > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           end_time - start_time)
                           .count();

    std::cout << "Client " << client_id << ": Message sending complete"
              << std::endl;
    std::cout << "  Successful writes: " << successful_msgs << "/" << iterations
              << " (" << successful_msgs * 100 / iterations << "%)"
              << std::endl;

    if (successful_msgs > 0 && duration_ms > 0) {
      double rate = (double)successful_msgs / (duration_ms / 1000.0);
      std::cout << "  Message rate: " << rate << " msgs/sec" << std::endl;
      std::cout << "  Average latency: "
                << duration_ms / (double)successful_msgs << " ms" << std::endl;
    }

    // Send completion signal to server
    std::cout << "Client " << client_id
              << ": Sending completion signal with message count..."
              << std::endl;

    // Clear buffer and fill with completion message
    memset(msg_buf, 0xFF, ci->msg_size);

    // Signal structure
    struct {
      char signature[8];
      uint32_t client_id;
      uint32_t msg_count;
    } *signal;

    signal = (decltype(signal))msg_buf;
    // Copy the signature
    char signatureStr[9] = "COMPLETE";
    memcpy(signal->signature, signatureStr, 8);
    signal->client_id = client_id;
    signal->msg_count = successful_msgs;

    // Add descriptive text after the header
    char *desc = msg_buf + sizeof(*signal);
    snprintf(desc, ci->msg_size - sizeof(*signal), "Client %d sent %d messages",
             client_id, successful_msgs);

    // Use PostSend for signal
    auto send_stat = ep->PostSend(1, msg_mr->lkey, msg_buf, ci->msg_size);
    if (!send_stat.ok()) {
      std::cerr << "Error sending completion signal: " << send_stat.message()
                << std::endl;
    } else {
      auto wc_s = ep->PollSendCq();
      if (!wc_s.ok()) {
        std::cerr << "Error completing send of completion signal: "
                  << wc_s.status().message() << std::endl;
      } else {
        std::cout << "Client " << client_id
                  << ": Completion signal sent successfully" << std::endl;
      }
    }

    // Clean up resources
    ibv_dereg_mr(msg_mr);
    delete[] msg_buf;
    ep->Close();

    std::cout << "Client " << client_id << ": Cleanup complete, exiting"
              << std::endl;
  }

  return 0;
}
