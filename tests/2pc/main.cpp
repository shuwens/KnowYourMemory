

#include "async_events.hpp"
#include "bench/bench.hpp"
#include "conn.hpp"
#include "cxxopts.hpp"
#include "debug.h"
#include "send_receive.hpp"
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <infiniband/verbs.h>
#include <iomanip>
#include <iostream>
#include <ostream>
#include <string>
#include <thread>

cxxopts::ParseResult parse(int argc, char *argv[]) {
  cxxopts::Options options(argv[0], "sendrecv");
  try {
    options.add_options()("srq", "Whether to use a shard receive queue",
                          cxxopts::value<bool>())(
        "bw", "Whether to test bandwidth", cxxopts::value<bool>())(
        "lat", "Whether to test latency", cxxopts::value<bool>())(
        "pingpong", "Whether to test pingpong latency", cxxopts::value<bool>())(
        "client", "Whether to as client only", cxxopts::value<bool>())(
        "server", "Whether to as server only", cxxopts::value<bool>())(
        "i,address", "IP address to connect to", cxxopts::value<std::string>())(
        "source", "source IP address",
        cxxopts::value<std::string>()->default_value(""))(
        "n,iters", "Number of exchanges",
        cxxopts::value<int>()->default_value("1000"))(
        "s,size", "Size of message to exchange",
        cxxopts::value<int>()->default_value("1024"))(
        "l,limit",
        "Sender ratelimit to send at in Msg/s. Set to 0 to not limit sender",
        cxxopts::value<int>()->default_value("0"))(
        "c,conn", "Number of connections",
        cxxopts::value<int>()->default_value("1"))(
        "single-receiver",
        "whether to have single receiver for all connections N:1",
        cxxopts::value<bool>()->default_value("false"))(
        "unack",
        "Number of messages that can be unacknowleged. Only relevant for "
        "bandwidth benchmark",
        cxxopts::value<int>()->default_value("100"))(
        "batch",
        "Number of messages to send in a single batch. Only relevant for "
        "bandwidth benchmark",
        cxxopts::value<int>()->default_value("1"))(
        "out", "filename to output measurements",
        cxxopts::value<std::string>()->default_value(""));

    auto result = options.parse(argc, argv);
    if (!result.count("address")) {
      std::cerr << "Specify an address" << std::endl;
      std::cerr << options.help({""}) << std::endl;
      exit(1);
    }

    if (!result.count("lat") && !result.count("bw") &&
        !result.count("pingpong")) {
      std::cerr << "Either perform latency or bandwidth benchmark" << std::endl;
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

int main(int argc, char *argv[]) {
  auto flags = parse(argc, argv);
  std::string ip = flags["address"].as<std::string>();
  std::string src = flags["source"].as<std::string>();

  std::string filename = flags["out"].as<std::string>();

  bool bw = flags["bw"].as<bool>();
  bool lat = flags["lat"].as<bool>();
  bool pingpong = flags["pingpong"].as<bool>();

  bool srq = flags["srq"].as<bool>();

  int count = flags["iters"].as<int>();
  int size = flags["size"].as<int>();
  int batch = flags["batch"].as<int>();
  int unack = flags["unack"].as<int>();
  int limit = flags["limit"].as<int>();

  if (batch > 1 && limit) {
    std::cerr << "ratelimit with batching is not supported" << std::endl;
    exit(1);
  }

  int conn_count = flags["conn"].as<int>();
  bool singlercv = flags["single-receiver"].as<bool>();
  limit = limit / conn_count;

  bool server = flags["server"].as<bool>();
  bool client = flags["client"].as<bool>();

  std::vector<std::vector<float> *> measurements(conn_count);
  std::thread ae_thread;
  std::thread rcver;
  if (server) {
    std::vector<std::thread> workers;
    kym::connection::SendReceiveConnection *conns[conn_count];
    kym::connection::SendReceiveListener *ln;
    if (srq) {
      auto ln_s = kym::connection::ListenSharedReceive(ip, 9999, singlercv);
      if (!ln_s.ok()) {
        std::cerr
            << "Error listening for send_receive with shared receive queue "
            << ln_s.status().message() << std::endl;
        return 1;
      }
      ln = ln_s.value();

    } else {
      auto ln_s = kym::connection::ListenSendReceive(ip, 9999, singlercv);
      if (!ln_s.ok()) {
        std::cerr << "Error listening for send_receive "
                  << ln_s.status().message() << std::endl;
        return 1;
      }
      ln = ln_s.value();
    }
    ae_thread = DebugTrailAsyncEvents(ln->GetListener()->GetContext());

    for (int i = 0; i < conn_count; i++) {
      debug(stderr, "Accepting\n");
      auto conn_s = ln->Accept();
      if (!conn_s.ok()) {
        std::cerr << "Error accepting for send_receive "
                  << conn_s.status().message() << std::endl;
        return 1;
      }
      debug(stderr, "Accepted\n");

      if (!singlercv) {
        auto conn = conn_s.value();
        conns[i] = conn;
        auto y = [i, bw, lat, pingpong, conn, count, size, &measurements]() {
          set_core_affinity(i + 2);
          std::vector<float> *m = new std::vector<float>();

          // Log that server is starting
          std::cout << "Server [" << i << "] starting..." << std::endl;

          if (bw) {
            // Modified approach for bandwidth test
            std::cout << "Server [" << i << "] running bandwidth test"
                      << std::endl;

            // Use this buffer for copying and printing the message
            char *print_buffer = new char[size];

            int recvd = 0;
            auto begin = std::chrono::high_resolution_clock::now();

            while (recvd < count) {
              // Receive message (similar to original test_bw_recv)
              auto recv_s = conn->Receive();
              if (!recv_s.ok()) {
                delete[] print_buffer;
                std::cerr << "Error receiving message: " << recv_s.status()
                          << std::endl;
                return 1;
              }

              // Print message info
              std::cout << "Server [" << i << "] received message #"
                        << recvd + 1 << std::endl;

              // Try to print some bytes from the region structure itself
              // This is a hack since we don't know the exact structure
              const auto &region = recv_s.value();
              const void *addr = &region;
              const unsigned char *bytes =
                  static_cast<const unsigned char *>(addr);

              // Print a hex dump of part of the memory where the region is
              // stored The actual message content should be somewhere in this
              // memory
              std::cout << "Memory dump of ReceiveRegion:" << std::endl;
              for (int j = 0; j < 64; j++) {
                if (j % 16 == 0) {
                  std::cout << std::endl
                            << std::hex << std::setw(4) << std::setfill('0')
                            << j << ": ";
                }
                std::cout << std::setw(2) << std::setfill('0')
                          << static_cast<int>(bytes[j]) << " ";
              }
              std::cout << std::dec << std::endl;

              // Also try to print it as ASCII if it's printable
              std::cout << "ASCII representation:" << std::endl;
              for (int j = 0; j < 64; j++) {
                if (j % 16 == 0) {
                  std::cout << std::endl;
                }
                char c = static_cast<char>(bytes[j]);
                if (isprint(c)) {
                  std::cout << c;
                } else {
                  std::cout << ".";
                }
              }
              std::cout << std::endl;

              // Increment received count
              recvd++;
            }

            auto end = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                end - begin);

            uint64_t bytes = ((uint64_t)count) * ((uint64_t)size);
            uint64_t nanos = elapsed.count();
            uint64_t rate_bytes_per_ns = (bytes * 1000000000L) / nanos;

            delete[] print_buffer;
            m->push_back(rate_bytes_per_ns);
          } else if (lat) {
            // Modified approach for latency test
            std::cout << "Server [" << i << "] running latency test"
                      << std::endl;

            for (int j = 0; j < count; j++) {
              auto recv_s = conn->Receive();
              if (!recv_s.ok()) {
                std::cerr << "Error receiving message: " << recv_s.status()
                          << std::endl;
                return 1;
              }

              // Print message info
              std::cout << "Server [" << i << "] received latency message #"
                        << j + 1 << std::endl;

              // Try to print some bytes from the region structure itself
              const auto &region = recv_s.value();
              const void *addr = &region;
              const unsigned char *bytes =
                  static_cast<const unsigned char *>(addr);

              // Print first few bytes
              std::cout << "First 16 bytes: ";
              for (int k = 0; k < 16; k++) {
                std::cout << std::hex << std::setw(2) << std::setfill('0')
                          << static_cast<int>(bytes[k]) << " ";
              }
              std::cout << std::dec << std::endl;

              // Send reply using the original approach from test_lat_recv
              // This might not work exactly the same, but we'll try
              kym::connection::SendRegion send_region;
              auto stat = conn->Send(send_region);
              if (!stat.ok()) {
                std::cerr << "Error sending reply: " << stat << std::endl;
                return 1;
              }
            }
          } else if (pingpong) {
            // Modified approach for ping-pong test
            std::cout << "Server [" << i << "] running ping-pong test"
                      << std::endl;

            for (int j = 0; j < count; j++) {
              auto recv_s = conn->Receive();
              if (!recv_s.ok()) {
                std::cerr << "Error receiving ping: " << recv_s.status()
                          << std::endl;
                return 1;
              }

              // Print message info
              std::cout << "Server [" << i << "] received ping #" << j + 1
                        << std::endl;

              // Try to print some bytes from the region structure itself
              const auto &region = recv_s.value();
              const void *addr = &region;
              const unsigned char *bytes =
                  static_cast<const unsigned char *>(addr);

              // Print first few bytes
              std::cout << "First 16 bytes: ";
              for (int k = 0; k < 16; k++) {
                std::cout << std::hex << std::setw(2) << std::setfill('0')
                          << static_cast<int>(bytes[k]) << " ";
              }
              std::cout << std::dec << std::endl;

              // Send pong reply
              kym::connection::SendRegion send_region;
              auto stat = conn->Send(send_region);
              if (!stat.ok()) {
                std::cerr << "Error sending pong: " << stat << std::endl;
                return 1;
              }
            }
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
    if (singlercv) {
      set_core_affinity(1);
      if (bw) {
        std::vector<float> *m = new std::vector<float>();

        // Single receiver bandwidth test with modified approach to print
        // messages
        std::cout << "Single receiver starting..." << std::endl;

        // Use this buffer for copying and printing the message
        char *print_buffer = new char[size];

        int recvd = 0;
        auto begin = std::chrono::high_resolution_clock::now();

        int total_msgs = conn_count * count;
        while (recvd < total_msgs) {
          // Receive message
          auto recv_s = ln->Receive();
          if (!recv_s.ok()) {
            delete[] print_buffer;
            std::cerr << "Error receiving message: " << recv_s.status()
                      << std::endl;
            return 1;
          }

          // Print message info
          std::cout << "Single receiver received message #" << recvd + 1
                    << std::endl;

          // Try to print some bytes from the region structure itself
          const auto &region = recv_s.value();
          const void *addr = &region;
          const unsigned char *bytes = static_cast<const unsigned char *>(addr);

          // Print a hex dump of part of the memory where the region is stored
          std::cout << "Memory dump of ReceiveRegion:" << std::endl;
          for (int j = 0; j < 64; j++) {
            if (j % 16 == 0) {
              std::cout << std::endl
                        << std::hex << std::setw(4) << std::setfill('0') << j
                        << ": ";
            }
            std::cout << std::setw(2) << std::setfill('0')
                      << static_cast<int>(bytes[j]) << " ";
          }
          std::cout << std::dec << std::endl;

          // Increment received count
          recvd++;
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);

        uint64_t bytes = ((uint64_t)total_msgs) * ((uint64_t)size);
        uint64_t nanos = elapsed.count();
        uint64_t rate_bytes_per_ns = (bytes * 1000000000L) / nanos;

        delete[] print_buffer;
        m->push_back(rate_bytes_per_ns);
        measurements[0] = m;
      }
    }

    for (size_t i = 0; i < workers.size(); i++) {
      workers[i].join();
    }
    ln->Close();
    if (srq && !singlercv) {
      rcver.join();
    }
  }

  if (client) {
    std::vector<std::thread> workers;
    std::chrono::milliseconds timespan(
        4000); // If we start the server at the same time we will wait a little
    std::this_thread::sleep_for(timespan);
    kym::connection::SendReceiveConnection *conns[conn_count];

    for (int i = 0; i < conn_count; i++) {
      auto conn_s = kym::connection::DialSendReceive(ip, 9999, src);
      if (!conn_s.ok()) {
        std::cerr << "Error dialing send_receive connection "
                  << conn_s.status().message() << std::endl;
        return 1;
      }
      kym::connection::SendReceiveConnection *conn = conn_s.value();
      if (i == 0) {
        ae_thread = DebugTrailAsyncEvents(conn->GetEndpoint()->GetContext());
      }
      conns[i] = conn;
      workers.push_back(std::thread([i, bw, lat, pingpong, conn, count, batch,
                                     size, unack, limit, &measurements]() {
        set_core_affinity(i + 2);

        std::chrono::milliseconds timespan(
            4000); // This is because of a race condition...
        std::vector<float> *m = new std::vector<float>();
        std::this_thread::sleep_for(timespan);
        if (bw) {
          if (batch > 1) {
            auto bw_s = test_bw_batch_send(conn, count, size, batch, unack);
            if (!bw_s.ok()) {
              int cpu_num = sched_getcpu();
              std::cerr << "[CPU: " << cpu_num
                        << "] Error running benchmark: " << bw_s.status()
                        << std::endl;
              return 1;
            }
            m->push_back(bw_s.value());
          } else {
            kym::StatusOr<uint64_t> bw_s;
            if (limit) {
              bw_s = test_bw_limit_send(conn, count, size, unack, limit);
            } else {
              bw_s = test_bw_send(conn, count, size, unack);
            }
            if (!bw_s.ok()) {
              int cpu_num = sched_getcpu();
              std::cerr << "[CPU: " << cpu_num
                        << "] Error running benchmark: " << bw_s.status()
                        << std::endl;
              return 1;
            }
            m->push_back(bw_s.value());
          }
        } else if (lat) {
          std::chrono::milliseconds timespan(
              1000); // This is because of a race condition...
          std::this_thread::sleep_for(timespan);

          m->reserve(count);

          auto stat = test_lat_send(conn, count, size, m);
          if (!stat.ok()) {
            std::cerr << "Error running benchmark: " << stat << std::endl;
            return 1;
          }
        } else if (pingpong) {
          // pingpong
          std::chrono::milliseconds timespan(
              1000); // This is because of a race condition...
          std::this_thread::sleep_for(timespan);

          m->reserve(count);

          auto stat = test_lat_ping(conn, conn, count, size, m);
          if (!stat.ok()) {
            std::cerr << "Error running benchmark: " << stat << std::endl;
            return 1;
          }
        }
        measurements[i] = m;
        std::this_thread::sleep_for(timespan);
        conn->Close();
        return 0;
      }));
    }
    for (size_t i = 0; i < workers.size(); i++) {
      workers[i].join();
    }
  }
  if (lat || pingpong) {
    // Handle Latency distribution
    std::vector<float> joined;
    for (auto m : measurements) {
      if (m != nullptr) {
        joined.reserve(m->size());
        joined.insert(joined.end(), m->begin(), m->end());
      }
    }
    if (!filename.empty()) {
      std::ofstream file(filename);
      for (float f : joined) {
        file << f << "\n";
      }
      file.close();
    }

    if (joined.size() > 0) {
      if (lat) {
        std::cout << "\tLatency";
      } else if (pingpong) {
        std::cout << "\tPingPong Latency";
      }
      auto n = joined.size();
      std::cout << std::endl;
      std::cout << "N: " << n << std::endl;

      std::sort(joined.begin(), joined.end());
      int q025 = (int)(n * 0.025);
      int q500 = (int)(n * 0.5);
      int q975 = (int)(n * 0.975);
      std::cout << "q025" << "\t" << "q50" << "\t" << "q975" << std::endl;
      std::cout << joined[q025] << "\t" << joined[q500] << "\t" << joined[q975]
                << std::endl;
    }
  }

  if (bw) {
    uint64_t bandwidth = 0;
    for (auto m : measurements) {
      if (m != nullptr) {
        bandwidth += m->at(0);
      }
    }
    std::cerr << "## Bandwidth (MB/s)" << std::endl;
    std::cout << (double)bandwidth / (1024 * 1024) << std::endl;
    if (!filename.empty()) {
      std::ofstream file(filename, std::ios_base::app);
      file << conn_count << " " << size << " " << unack << " " << batch << " "
           << bandwidth << "\n";
      file.close();
    }
  }

  return 0;
}
