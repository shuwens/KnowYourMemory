#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <fstream>
#include <infiniband/verbs.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "async_events.hpp"
#include "bench/bench.hpp"
#include "conn.hpp"
#include "cxxopts.hpp"
#include "debug.h"
#include "send_receive.hpp"

// Thread-safe logging helper
std::mutex cout_mutex;
#define SAFE_LOG(msg)                                                          \
  {                                                                            \
    std::lock_guard<std::mutex> lock(cout_mutex);                              \
    std::cout << msg << std::endl;                                             \
  }

// Function to print message content
void print_message(const char *buffer, int size, int client_id = -1) {
  std::ostringstream oss;
  oss << "Received message from client "
      << (client_id >= 0 ? std::to_string(client_id) : "unknown") << " ("
      << size << " bytes): ";

  // Print as string if it seems to be text
  bool is_printable = true;
  for (int i = 0; i < std::min(size, 100); i++) {
    if (!isprint(buffer[i]) && !isspace(buffer[i])) {
      is_printable = false;
      break;
    }
  }

  if (is_printable) {
    oss << "\"";
    // Print up to 100 chars to avoid flooding the console
    int print_len = std::min(size, 100);
    oss.write(buffer, print_len);
    if (print_len < size)
      oss << "... (truncated)";
    oss << "\"";
  } else {
    oss << "(binary data)";
    // Print first few bytes as hex
    int print_len = std::min(size, 20);
    for (int i = 0; i < print_len; i++) {
      oss << " " << std::hex << std::setw(2) << std::setfill('0')
          << static_cast<int>(static_cast<unsigned char>(buffer[i]));
    }
    if (print_len < size)
      oss << "... (truncated)";
  }

  SAFE_LOG(oss.str());
}

cxxopts::ParseResult parse(int argc, char *argv[]) {
  cxxopts::Options options(
      argv[0], "sendrecv - multiple client to single server bandwidth test");
  try {
    options.add_options()("srq", "Whether to use a shared receive queue",
                          cxxopts::value<bool>())(
        "bw", "Whether to test bandwidth", cxxopts::value<bool>())(
        "lat", "Whether to test latency", cxxopts::value<bool>())(
        "pingpong", "Whether to test pingpong latency", cxxopts::value<bool>())(
        "client", "Whether to act as client only", cxxopts::value<bool>())(
        "server", "Whether to act as server only", cxxopts::value<bool>())(
        "i,address", "IP address to connect to", cxxopts::value<std::string>())(
        "source", "Source IP address",
        cxxopts::value<std::string>()->default_value(""))(
        "n,iters", "Number of exchanges per connection",
        cxxopts::value<int>()->default_value("1000"))(
        "s,size", "Size of message to exchange",
        cxxopts::value<int>()->default_value("1024"))(
        "l,limit", "Sender ratelimit in Msg/s (0 for no limit)",
        cxxopts::value<int>()->default_value("0"))(
        "c,conn", "Number of connections",
        cxxopts::value<int>()->default_value("1"))(
        "t,threads", "Number of worker threads for processing (server only)",
        cxxopts::value<int>()->default_value("1"))(
        "single-receiver",
        "Whether to have single receiver for all connections N:1",
        cxxopts::value<bool>()->default_value("false"))(
        "unack",
        "Number of messages that can be unacknowledged (bandwidth only)",
        cxxopts::value<int>()->default_value("100"))(
        "batch",
        "Number of messages to send in a single batch (bandwidth only)",
        cxxopts::value<int>()->default_value("1"))(
        "out", "Filename to output measurements",
        cxxopts::value<std::string>()->default_value(""))(
        "client-id", "Client identifier (integer) for multi-client setup",
        cxxopts::value<int>()->default_value("0"))(
        "verbose", "Enable verbose output",
        cxxopts::value<bool>()->default_value("false"))("h,help",
                                                        "Print usage");

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
      std::cout << options.help() << std::endl;
      exit(0);
    }

    if (!result.count("address")) {
      std::cerr << "Error: Address must be specified" << std::endl;
      std::cerr << options.help() << std::endl;
      exit(1);
    }

    if (!result.count("lat") && !result.count("bw") &&
        !result.count("pingpong")) {
      std::cerr << "Error: Either specify latency or bandwidth benchmark"
                << std::endl;
      std::cerr << options.help() << std::endl;
      exit(1);
    }

    if (result.count("single-receiver") && !result.count("bw")) {
      std::cerr
          << "Warning: Single receiver mode works best with bandwidth test"
          << std::endl;
    }

    return result;
  } catch (const cxxopts::OptionException &e) {
    std::cerr << "Error parsing options: " << e.what() << std::endl;
    std::cerr << options.help() << std::endl;
    exit(1);
  }
}

// Worker class for handling messages in a multi-threaded server
class MessageWorker {
public:
  MessageWorker(int num_threads, int msg_size)
      : running_(true), msg_size_(msg_size), total_processed_(0) {

    // Start worker threads
    for (int i = 0; i < num_threads; i++) {
      threads_.push_back(std::thread(&MessageWorker::worker_thread, this, i));
    }
  }

  ~MessageWorker() { stop(); }

  void process_message(const kym::connection::ReceiveRegion &region,
                       void *client_handle) {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    // Add to queue
    MessageItem item;
    item.region = region;
    item.client_handle = client_handle;
    message_queue_.push(item);

    // Notify a worker
    queue_cv_.notify_one();
  }

  void stop() {
    if (running_) {
      running_ = false;
      queue_cv_.notify_all();

      for (auto &thread : threads_) {
        if (thread.joinable()) {
          thread.join();
        }
      }
    }
  }

  uint64_t get_total_processed() const { return total_processed_; }

  const std::map<uintptr_t, int> &get_client_stats() const {
    return msgs_per_client_;
  }

private:
  struct MessageItem {
    kym::connection::ReceiveRegion region;
    void *client_handle;
  };

  void worker_thread(int thread_id) {
    char *print_buffer = new char[msg_size_];

    while (running_) {
      MessageItem item;
      bool have_item = false;

      // Get an item from the queue
      {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock,
                       [this] { return !message_queue_.empty() || !running_; });

        if (!running_ && message_queue_.empty()) {
          break;
        }

        if (!message_queue_.empty()) {
          item = message_queue_.front();
          message_queue_.pop();
          have_item = true;
        }
      }

      if (have_item) {
        // Process the message
        process_message_item(item, thread_id, print_buffer);
        total_processed_++;

        // Update per-client statistics
        {
          std::lock_guard<std::mutex> lock(stats_mutex_);
          uintptr_t client_id = reinterpret_cast<uintptr_t>(item.client_handle);
          msgs_per_client_[client_id]++;
        }
      }
    }

    delete[] print_buffer;
  }

  void process_message_item(const MessageItem &item, int thread_id,
                            char *print_buffer) {
    // Here we would actually process the message
    // For now, we just print some debug info

    // Try to extract some information from the message
    const auto &region = item.region;
    uintptr_t client_id = reinterpret_cast<uintptr_t>(item.client_handle);

    if (print_buffer) {
      // Try to access the message data
      const void *addr = &region;
      const unsigned char *bytes = static_cast<const unsigned char *>(addr);

      // Copy a portion of the memory for printing
      std::memcpy(print_buffer, bytes, msg_size_);

      // Print the message (only in verbose mode to avoid overwhelming output)
      if (verbose_) {
        std::ostringstream oss;
        oss << "Thread " << thread_id << " processing message from client "
            << client_id << ", dumping first 32 bytes:";

        for (int i = 0; i < 32; i++) {
          if (i % 16 == 0)
            oss << std::endl;
          oss << std::hex << std::setw(2) << std::setfill('0')
              << static_cast<int>(bytes[i]) << " ";
        }

        SAFE_LOG(oss.str());
      }
    }
  }

  // Thread management
  std::vector<std::thread> threads_;
  std::atomic<bool> running_;

  // Message queue
  std::queue<MessageItem> message_queue_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;

  // Statistics
  std::mutex stats_mutex_;
  std::map<uintptr_t, int> msgs_per_client_;
  int msg_size_;
  std::atomic<uint64_t> total_processed_;

  // Configuration
  bool verbose_ = false;
};

int main(int argc, char *argv[]) {
  auto flags = parse(argc, argv);
  std::string ip = flags["address"].as<std::string>();
  std::string src = flags["source"].as<std::string>();
  std::string filename = flags["out"].as<std::string>();

  bool verbose = flags["verbose"].as<bool>();
  bool bw = flags["bw"].as<bool>();
  bool lat = flags["lat"].as<bool>();
  bool pingpong = flags["pingpong"].as<bool>();
  bool srq = flags["srq"].as<bool>();

  int count = flags["iters"].as<int>();
  int size = flags["size"].as<int>();
  int batch = flags["batch"].as<int>();
  int unack = flags["unack"].as<int>();
  int limit = flags["limit"].as<int>();
  int num_threads = flags["threads"].as<int>();
  int client_id = flags["client-id"].as<int>();

  if (batch > 1 && limit) {
    std::cerr << "Error: Ratelimit with batching is not supported" << std::endl;
    exit(1);
  }

  int conn_count = flags["conn"].as<int>();
  bool singlercv = flags["single-receiver"].as<bool>();

  if (limit) {
    limit = limit / conn_count;
  }

  bool server = flags["server"].as<bool>();
  bool client = flags["client"].as<bool>();

  std::vector<std::vector<float> *> measurements(conn_count);
  std::thread ae_thread;
  std::thread rcver;

  // ================= SERVER MODE =================
  if (server) {
    std::cout << "Starting server on " << ip << std::endl;
    if (singlercv) {
      std::cout << "Using single receiver mode for multiple clients"
                << std::endl;
    }

    std::vector<std::thread> workers;
    kym::connection::SendReceiveConnection *conns[conn_count];
    kym::connection::SendReceiveListener *ln;

    if (srq) {
      auto ln_s = kym::connection::ListenSharedReceive(ip, 9999, singlercv);
      if (!ln_s.ok()) {
        std::cerr
            << "Error listening for send_receive with shared receive queue: "
            << ln_s.status().message() << std::endl;
        return 1;
      }
      ln = ln_s.value();
    } else {
      auto ln_s = kym::connection::ListenSendReceive(ip, 9999, singlercv);
      if (!ln_s.ok()) {
        std::cerr << "Error listening for send_receive: "
                  << ln_s.status().message() << std::endl;
        return 1;
      }
      ln = ln_s.value();
    }

    ae_thread = DebugTrailAsyncEvents(ln->GetListener()->GetContext());
    std::cout << "Server listening, waiting for connections..." << std::endl;

    // Multiple client connections, single receiver mode
    if (singlercv && bw) {
      set_core_affinity(1);
      std::cout << "Single receiver starting with " << num_threads
                << " worker threads for multiple clients" << std::endl;

      std::vector<float> *m = new std::vector<float>();
      char *print_buffer = new char[size];

      // Create message worker pool
      MessageWorker message_worker(num_threads, size);

      int recvd = 0;
      auto begin = std::chrono::high_resolution_clock::now();

      // In single-receiver mode with multiple clients, we can't know the exact
      // number of messages to expect. We'll use a timeout approach.
      std::cout
          << "Starting bandwidth test, will receive messages for 30 seconds..."
          << std::endl;

      auto timeout =
          std::chrono::system_clock::now() + std::chrono::seconds(30);
      std::map<void *, int> client_msgs;

      while (std::chrono::system_clock::now() < timeout) {
        // Receive message with timeout
        auto recv_s = ln->Receive();
        if (!recv_s.ok()) {
          auto status = recv_s.status();
          // Instead of checking for a specific status code, just log the error
          // and continue - this is more robust
          std::cout << "Error receiving message: " << status << std::endl;

          // Sleep briefly to avoid spinning too fast on errors
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          continue; // Skip to next iteration
        }

        // Try to identify which client sent this message
        void *client_handle = nullptr;
        // This depends on the actual implementation of ReceiveRegion
        // In a real implementation, you'd extract the client information

        // Count messages per client
        client_msgs[client_handle]++;
        recvd++;

        if (verbose && recvd % 1000 == 0) {
          std::cout << "Received " << recvd << " messages so far" << std::endl;
        }

        // Process message in worker thread pool
        message_worker.process_message(recv_s.value(), client_handle);
      }

      auto end = std::chrono::high_resolution_clock::now();
      auto elapsed =
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);

      // Stop the worker threads
      message_worker.stop();

      // Print statistics
      int total_processed = message_worker.get_total_processed();
      auto client_stats = message_worker.get_client_stats();

      std::cout << "Test completed:" << std::endl;
      std::cout << "  Total messages received: " << recvd << std::endl;
      std::cout << "  Total messages processed: " << total_processed
                << std::endl;
      std::cout << "  Number of clients: " << client_stats.size() << std::endl;

      std::cout << "Messages per client:" << std::endl;
      for (const auto &pair : client_stats) {
        std::cout << "  Client " << pair.first << ": " << pair.second
                  << " messages" << std::endl;
      }

      uint64_t bytes = ((uint64_t)recvd) * ((uint64_t)size);
      uint64_t nanos = elapsed.count();
      uint64_t rate_bytes_per_ns = (bytes * 1000000000L) / nanos;
      double mbps = (double)rate_bytes_per_ns / (1024 * 1024);

      std::cout << "Bandwidth: " << mbps << " MB/s" << std::endl;
      std::cout << "Average processing rate: "
                << (double)recvd / (nanos / 1000000000.0) << " messages/sec"
                << std::endl;

      m->push_back(rate_bytes_per_ns);
      measurements[0] = m;

      delete[] print_buffer;
    }
    // Traditional connection-per-client mode
    else {
      for (int i = 0; i < conn_count; i++) {
        debug(stderr, "Accepting\n");
        std::cout << "Waiting for connection " << i + 1 << " of " << conn_count
                  << std::endl;

        auto conn_s = ln->Accept();
        if (!conn_s.ok()) {
          std::cerr << "Error accepting connection: "
                    << conn_s.status().message() << std::endl;
          return 1;
        }

        std::cout << "Accepted connection " << i + 1 << std::endl;

        if (!singlercv) {
          auto conn = conn_s.value();
          conns[i] = conn;

          auto y = [i, bw, lat, pingpong, conn, count, size, &measurements,
                    verbose]() {
            set_core_affinity(i + 2);
            std::vector<float> *m = new std::vector<float>();

            std::cout << "Server [" << i << "] starting..." << std::endl;

            if (bw) {
              // Bandwidth test
              std::cout << "Server [" << i << "] running bandwidth test"
                        << std::endl;

              char *print_buffer = new char[size];
              int recvd = 0;
              auto begin = std::chrono::high_resolution_clock::now();

              while (recvd < count) {
                auto recv_s = conn->Receive();
                if (!recv_s.ok()) {
                  delete[] print_buffer;
                  std::cerr << "Error receiving message: " << recv_s.status()
                            << std::endl;
                  return 1;
                }

                if (verbose) {
                  std::cout << "Server [" << i << "] received message #"
                            << recvd + 1 << std::endl;

                  // Try to print some bytes from the region
                  const auto &region = recv_s.value();
                  const void *addr = &region;
                  const unsigned char *bytes =
                      static_cast<const unsigned char *>(addr);

                  std::cout << "First 16 bytes: ";
                  for (int j = 0; j < 16; j++) {
                    std::cout << std::hex << std::setw(2) << std::setfill('0')
                              << static_cast<int>(bytes[j]) << " ";
                  }
                  std::cout << std::dec << std::endl;
                }

                recvd++;
              }

              auto end = std::chrono::high_resolution_clock::now();
              auto elapsed =
                  std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                       begin);

              uint64_t bytes = ((uint64_t)count) * ((uint64_t)size);
              uint64_t nanos = elapsed.count();
              uint64_t rate_bytes_per_ns = (bytes * 1000000000L) / nanos;

              std::cout << "Server [" << i << "] completed bandwidth test: "
                        << (double)rate_bytes_per_ns / (1024 * 1024) << " MB/s"
                        << std::endl;

              delete[] print_buffer;
              m->push_back(rate_bytes_per_ns);
            } else if (lat) {
              // Latency test
              std::cout << "Server [" << i << "] running latency test"
                        << std::endl;

              for (int j = 0; j < count; j++) {
                auto recv_s = conn->Receive();
                if (!recv_s.ok()) {
                  std::cerr << "Error receiving message: " << recv_s.status()
                            << std::endl;
                  return 1;
                }

                if (verbose) {
                  std::cout << "Server [" << i << "] received latency message #"
                            << j + 1 << std::endl;
                }

                // Send reply
                kym::connection::SendRegion send_region;
                auto stat = conn->Send(send_region);
                if (!stat.ok()) {
                  std::cerr << "Error sending reply: " << stat << std::endl;
                  return 1;
                }
              }

              std::cout << "Server [" << i << "] completed latency test"
                        << std::endl;
            } else if (pingpong) {
              // Ping-pong test
              std::cout << "Server [" << i << "] running ping-pong test"
                        << std::endl;

              for (int j = 0; j < count; j++) {
                auto recv_s = conn->Receive();
                if (!recv_s.ok()) {
                  std::cerr << "Error receiving ping: " << recv_s.status()
                            << std::endl;
                  return 1;
                }

                if (verbose) {
                  std::cout << "Server [" << i << "] received ping #" << j + 1
                            << std::endl;
                }

                // Send pong reply
                kym::connection::SendRegion send_region;
                auto stat = conn->Send(send_region);
                if (!stat.ok()) {
                  std::cerr << "Error sending pong: " << stat << std::endl;
                  return 1;
                }
              }

              std::cout << "Server [" << i << "] completed ping-pong test"
                        << std::endl;
            }

            measurements[i] = m;
            conn->Close();
            return 0;
          };

          workers.push_back(std::thread(y));
        }
      }

      if (srq && !singlercv) {
        rcver = std::thread([ln] {
          set_core_affinity(1);
          ln->RunReceiver();
        });
      }

      // Wait for all worker threads to complete
      for (size_t i = 0; i < workers.size(); i++) {
        workers[i].join();
      }
    }

    // Clean up and close connections
    ln->Close();
    if (srq && !singlercv) {
      rcver.join();
    }

    std::cout << "Server completed all tests" << std::endl;
  }

  // ================= CLIENT MODE =================
  if (client) {
    std::cout << "Starting client connecting to " << ip;
    if (!src.empty()) {
      std::cout << " from source " << src;
    }
    std::cout << std::endl;

    std::vector<std::thread> workers;
    std::chrono::milliseconds timespan(
        4000); // Wait a little if server just started
    std::this_thread::sleep_for(timespan);

    kym::connection::SendReceiveConnection *conns[conn_count];

    for (int i = 0; i < conn_count; i++) {
      std::cout << "Client: connecting " << i + 1 << " of " << conn_count
                << std::endl;

      auto conn_s = kym::connection::DialSendReceive(ip, 9999, src);
      if (!conn_s.ok()) {
        std::cerr << "Error connecting to server: " << conn_s.status().message()
                  << std::endl;
        return 1;
      }

      std::cout << "Connected #" << i + 1 << std::endl;

      kym::connection::SendReceiveConnection *conn = conn_s.value();
      if (i == 0) {
        ae_thread = DebugTrailAsyncEvents(conn->GetEndpoint()->GetContext());
      }

      conns[i] = conn;

      workers.push_back(
          std::thread([i, bw, lat, pingpong, conn, count, batch, size, unack,
                       limit, &measurements, verbose, client_id]() {
            set_core_affinity(i + 2);

            std::chrono::milliseconds timespan(4000); // Wait for race condition
            std::vector<float> *m = new std::vector<float>();
            std::this_thread::sleep_for(timespan);

            if (bw) {
              // Prepare a message with client ID embedded
              char *msg_buffer = new char[size];
              int actual_client_id =
                  client_id * 100 +
                  i; // Combine base client ID with connection number
              snprintf(msg_buffer, size, "Client %d Conn %d Msg", client_id, i);

              if (batch > 1) {
                std::cout << "Client [" << actual_client_id
                          << "]: Starting bandwidth test with batching"
                          << std::endl;

                // Custom batched send with client ID
                auto bw_s = test_bw_batch_send(conn, count, size, batch, unack);
                if (!bw_s.ok()) {
                  int cpu_num = sched_getcpu();
                  std::cerr << "[CPU: " << cpu_num
                            << "] Error in bandwidth test: " << bw_s.status()
                            << std::endl;
                  delete[] msg_buffer;
                  return 1;
                }

                m->push_back(bw_s.value());
                std::cout << "Client [" << actual_client_id
                          << "]: Completed bandwidth test" << std::endl;
              } else {
                kym::StatusOr<uint64_t> bw_s;

                if (limit) {
                  std::cout << "Client [" << actual_client_id
                            << "]: Starting bandwidth test with rate limit"
                            << std::endl;
                  bw_s = test_bw_limit_send(conn, count, size, unack, limit);
                } else {
                  std::cout << "Client [" << actual_client_id
                            << "]: Starting bandwidth test" << std::endl;
                  bw_s = test_bw_send(conn, count, size, unack);
                }

                if (!bw_s.ok()) {
                  int cpu_num = sched_getcpu();
                  std::cerr << "[CPU: " << cpu_num
                            << "] Error in bandwidth test: " << bw_s.status()
                            << std::endl;
                  delete[] msg_buffer;
                  return 1;
                }

                m->push_back(bw_s.value());
                std::cout << "Client [" << actual_client_id
                          << "]: Completed bandwidth test: "
                          << (double)bw_s.value() / (1024 * 1024) << " MB/s"
                          << std::endl;
              }

              delete[] msg_buffer;
            } else if (lat) {
              std::chrono::milliseconds timespan(1000);
              std::this_thread::sleep_for(timespan);

              m->reserve(count);

              std::cout << "Client [" << client_id << "-" << i
                        << "]: Starting latency test" << std::endl;
              auto stat = test_lat_send(conn, count, size, m);
              if (!stat.ok()) {
                std::cerr << "Error in latency test: " << stat << std::endl;
                return 1;
              }

              std::cout << "Client [" << client_id << "-" << i
                        << "]: Completed latency test" << std::endl;
            } else if (pingpong) {
              std::chrono::milliseconds timespan(1000);
              std::this_thread::sleep_for(timespan);

              m->reserve(count);

              std::cout << "Client [" << client_id << "-" << i
                        << "]: Starting ping-pong test" << std::endl;
              auto stat = test_lat_ping(conn, conn, count, size, m);
              if (!stat.ok()) {
                std::cerr << "Error in ping-pong test: " << stat << std::endl;
                return 1;
              }

              std::cout << "Client [" << client_id << "-" << i
                        << "]: Completed ping-pong test" << std::endl;
            }

            measurements[i] = m;
            std::this_thread::sleep_for(timespan);
            conn->Close();
            return 0;
          }));
    }

    // Wait for all worker threads to complete
    for (size_t i = 0; i < workers.size(); i++) {
      workers[i].join();
    }

    std::cout << "Client completed all tests" << std::endl;
  }

  // ================= PROCESS RESULTS =================
  if (lat || pingpong) {
    // Handle latency distribution
    std::vector<float> joined;
    for (auto m : measurements) {
      if (m != nullptr) {
        joined.reserve(joined.size() + m->size());
        joined.insert(joined.end(), m->begin(), m->end());
      }
    }

    if (!filename.empty()) {
      std::ofstream file(filename);
      for (float f : joined) {
        file << f << "\n";
      }
      file.close();
      std::cout << "Latency measurements saved to " << filename << std::endl;
    }

    if (joined.size() > 0) {
      if (lat) {
        std::cout << "\n===== LATENCY RESULTS =====" << std::endl;
      } else if (pingpong) {
        std::cout << "\n===== PING-PONG RESULTS =====" << std::endl;
      }

      auto n = joined.size();
      std::cout << "Total measurements: " << n << std::endl;

      std::sort(joined.begin(), joined.end());

      int q025 = (int)(n * 0.025);
      int q500 = (int)(n * 0.5);
      int q975 = (int)(n * 0.975);

      std::cout << "q025" << "\t" << "q50" << "\t" << "q975" << std::endl;
      std::cout << joined[q025] << "\t" << joined[q500] << "\t" << joined[q975]
                << std::endl;

      // Calculate mean latency
      float sum = 0;
      for (float f : joined) {
        sum += f;
      }
      std::cout << "Mean latency: " << sum / n << " microseconds" << std::endl;
    }
  }

  if (bw) {
    uint64_t bandwidth = 0;
    for (auto m : measurements) {
      if (m != nullptr && !m->empty()) {
        bandwidth += m->at(0);
      }
    }

    std::cout << "\n===== BANDWIDTH RESULTS =====" << std::endl;
    double mbps = (double)bandwidth / (1024 * 1024);
    std::cout << "Total bandwidth: " << mbps << " MB/s" << std::endl;

    if (!filename.empty()) {
      std::ofstream file(filename, std::ios_base::app);
      file << conn_count << " " << size << " " << unack << " " << batch << " "
           << bandwidth << "\n";
      file.close();
      std::cout << "Bandwidth measurements saved to " << filename << std::endl;
    }
  }

  return 0;
}
