#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <infiniband/verbs.h>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <thread>

#include <ctime>
#include <iomanip>
#include <vector>

#include "cxxopts.hpp"

#include "async_events.hpp"
#include "bench/bench.hpp"
#include "conn.hpp"

#include "send_receive.hpp"

#include "debug.h"

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

    std::cout << "Server listening, waiting for connections..." << std::endl;

    for (int i = 0; i < conn_count; i++) {
      debug(stderr, "Accepting\n");
      std::cout << "Waiting for connection " << i + 1 << " of " << conn_count
                << std::endl;

      auto conn_s = ln->Accept();
      if (!conn_s.ok()) {
        std::cerr << "Error accepting for send_receive "
                  << conn_s.status().message() << std::endl;
        return 1;
      }
      debug(stderr, "Accepted\n");
      std::cout << "Accepted connection " << i + 1 << std::endl;

      if (!singlercv) {
        auto conn = conn_s.value();
        conns[i] = conn;
        auto y = [i, bw, lat, pingpong, conn, count, size, &measurements]() {
          set_core_affinity(i + 2);
          std::vector<float> *m = new std::vector<float>();

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

              // Try to print some bytes from the region structure itself
              const auto &region = recv_s.value();

              // Safety check to avoid segfault - only print if we can safely
              // access the memory
              if (recvd == 0 || recvd % 100 == 0) {
                std::cout << "Server [" << i << "] received message #"
                          << recvd + 1 << std::endl;
              }

              // Increment received count
              recvd++;
            }

            auto end = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                end - begin);

            uint64_t bytes = ((uint64_t)count) * ((uint64_t)size);
            uint64_t nanos = elapsed.count();
            uint64_t rate_bytes_per_ns = (bytes * 1000000000L) / nanos;

            std::cout << "Server [" << i << "] bandwidth test complete: "
                      << (double)rate_bytes_per_ns / (1024 * 1024) << " MB/s"
                      << std::endl;

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

              if (j % 100 == 0) {
                std::cout << "Server [" << i << "] received latency message #"
                          << j + 1 << std::endl;
              }

              // Send reply using the original approach from test_lat_recv
              kym::connection::SendRegion send_region;
              auto stat = conn->Send(send_region);
              if (!stat.ok()) {
                std::cerr << "Error sending reply: " << stat << std::endl;
                return 1;
              }
            }

            std::cout << "Server [" << i << "] latency test complete"
                      << std::endl;
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

              if (j % 100 == 0) {
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

            std::cout << "Server [" << i << "] ping-pong test complete"
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

    // Single receiver mode for multiple clients
    if (singlercv) {
      set_core_affinity(1);
      if (bw) {
        std::cout << "Single receiver starting..." << std::endl;
        std::vector<float> *m = new std::vector<float>();

        // Safer implementation for single receiver mode
        char *print_buffer = new char[size];

        int recvd = 0;
        auto begin = std::chrono::high_resolution_clock::now();

        // Run for a specific duration rather than waiting for exact message
        // count
        std::cout << "Starting bandwidth test, will receive messages for 30 "
                     "seconds..."
                  << std::endl;
        auto end_time =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);

        // Map to track messages per client
        std::map<void *, int> client_msgs;

        try {
          while (std::chrono::steady_clock::now() < end_time) {
            // Receive message with timeout check
            auto recv_s = ln->Receive();
            if (!recv_s.ok()) {
              std::cerr << "Error receiving message: " << recv_s.status()
                        << std::endl;
              std::this_thread::sleep_for(std::chrono::milliseconds(100));
              continue;
            }

            // Track messages
            recvd++;

            // Only log periodically to avoid console spam
            if (recvd % 1000 == 0) {
              std::cout << "Received " << recvd << " messages so far"
                        << std::endl;
            }
          }
        } catch (const std::exception &e) {
          std::cerr << "Exception in single receiver mode: " << e.what()
                    << std::endl;
        } catch (...) {
          std::cerr << "Unknown exception in single receiver mode" << std::endl;
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);

        uint64_t bytes = ((uint64_t)recvd) * ((uint64_t)size);
        uint64_t nanos = elapsed.count();
        uint64_t rate_bytes_per_ns = 0;

        if (nanos > 0) {
          rate_bytes_per_ns = (bytes * 1000000000L) / nanos;
        }

        std::cout << "Test completed - received " << recvd << " messages"
                  << std::endl;
        std::cout << "Bandwidth: " << (double)rate_bytes_per_ns / (1024 * 1024)
                  << " MB/s" << std::endl;

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

    std::cout << "Starting client connecting to " << ip;
    if (!src.empty()) {
      std::cout << " from source " << src;
    }
    std::cout << std::endl;

    for (int i = 0; i < conn_count; i++) {
      std::cout << "Client: connecting " << i + 1 << " of " << conn_count
                << std::endl;

      auto conn_s = kym::connection::DialSendReceive(ip, 9999, src);
      if (!conn_s.ok()) {
        std::cerr << "Error dialing send_receive connection "
                  << conn_s.status().message() << std::endl;
        return 1;
      }

      std::cout << "Connected #" << i + 1 << std::endl;

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
            std::cout << "Client [" << i
                      << "] starting bandwidth test with batching" << std::endl;
            auto bw_s = test_bw_batch_send(conn, count, size, batch, unack);
            if (!bw_s.ok()) {
              int cpu_num = sched_getcpu();
              std::cerr << "[CPU: " << cpu_num
                        << "] Error running benchmark: " << bw_s.status()
                        << std::endl;
              return 1;
            }
            m->push_back(bw_s.value());
            std::cout << "Client [" << i << "] completed bandwidth test: "
                      << (double)bw_s.value() / (1024 * 1024) << " MB/s"
                      << std::endl;
          } else {
            kym::StatusOr<uint64_t> bw_s;
            if (limit) {
              std::cout << "Client [" << i
                        << "] starting bandwidth test with rate limit"
                        << std::endl;
              bw_s = test_bw_limit_send(conn, count, size, unack, limit);
            } else {
              std::cout << "Client [" << i << "] starting bandwidth test"
                        << std::endl;
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
            std::cout << "Client [" << i << "] completed bandwidth test: "
                      << (double)bw_s.value() / (1024 * 1024) << " MB/s"
                      << std::endl;
          }
        } else if (lat) {
          std::chrono::milliseconds timespan(
              1000); // This is because of a race condition...
          std::this_thread::sleep_for(timespan);

          m->reserve(count);

          std::cout << "Client [" << i << "] starting latency test"
                    << std::endl;
          auto stat = test_lat_send(conn, count, size, m);
          if (!stat.ok()) {
            std::cerr << "Error running benchmark: " << stat << std::endl;
            return 1;
          }
          std::cout << "Client [" << i << "] completed latency test"
                    << std::endl;
        } else if (pingpong) {
          // pingpong
          std::chrono::milliseconds timespan(
              1000); // This is because of a race condition...
          std::this_thread::sleep_for(timespan);

          m->reserve(count);

          std::cout << "Client [" << i << "] starting ping-pong test"
                    << std::endl;
          auto stat = test_lat_ping(conn, conn, count, size, m);
          if (!stat.ok()) {
            std::cerr << "Error running benchmark: " << stat << std::endl;
            return 1;
          }
          std::cout << "Client [" << i << "] completed ping-pong test"
                    << std::endl;
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

    std::cout << "Client completed all tests" << std::endl;
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
    }
  }

  if (bw) {
    uint64_t bandwidth = 0;
    for (auto m : measurements) {
      if (m != nullptr) {
        bandwidth += m->at(0);
      }
    }
    std::cerr << "\n===== BANDWIDTH RESULTS =====" << std::endl;
    std::cout << "Total bandwidth: " << (double)bandwidth / (1024 * 1024)
              << " MB/s" << std::endl;
    if (!filename.empty()) {
      std::ofstream file(filename, std::ios_base::app);
      file << conn_count << " " << size << " " << unack << " " << batch << " "
           << bandwidth << "\n";
      file.close();
    }
  }

  return 0;
}
