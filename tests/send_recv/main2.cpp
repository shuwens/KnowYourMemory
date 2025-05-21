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
        auto y = [i, bw, lat, conn, count, size, &measurements]() {
          set_core_affinity(i + 2);
          std::vector<float> *m = new std::vector<float>();
          if (bw) {
            auto bw_s = test_bw_recv(conn, count, size);
            if (!bw_s.ok()) {
              int cpu_num = sched_getcpu();
              std::cerr << "[CPU: " << cpu_num
                        << "] Error running benchmark: " << bw_s.status()
                        << std::endl;
              return 1;
            }
            m->push_back(bw_s.value());
          } else if (lat) {
            auto stat = test_lat_recv(conn, count);
            if (!stat.ok()) {
              std::cerr << "Error running benchmark: " << stat << std::endl;
              return 1;
            }
          } else {
            auto stat = test_lat_pong(conn, conn, count, size);
            if (!stat.ok()) {
              std::cerr << "Error running benchmark: " << stat << std::endl;
              return 1;
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
        auto bw_s = test_bw_recv(ln, conn_count * count, size);
        if (!bw_s.ok()) {
          int cpu_num = sched_getcpu();
          std::cerr << "[CPU: " << cpu_num
                    << "] Error running benchmark: " << bw_s.status()
                    << std::endl;
          return 1;
        }
        m->push_back(bw_s.value());
        measurements[0] = m;
      }
    }

    for (int i = 0; i < workers.size(); i++) {
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
        std::cerr << "Error dialing send_receive co_nection"
                  << conn_s.status().message() << std::endl;
        return 1;
      }
      kym::connection::SendReceiveConnection *conn = conn_s.value();
      if (i == 0) {
        ae_thread = DebugTrailAsyncEvents(conn->GetEndpoint()->GetContext());
      }
      conns[i] = conn;
      workers.push_back(std::thread([i, bw, lat, conn, count, batch, size,
                                     unack, limit, &measurements]() {
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
        } else {
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
    for (int i = 0; i < workers.size(); i++) {
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
