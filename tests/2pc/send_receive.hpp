
/*
 *
 *  conn.h
 *
 *  Common interfaces for rdma connections
 *
 */

#ifndef KNY_CONN_SEND_RECEIVE_H_
#define KNY_CONN_SEND_RECEIVE_H_

#include <map>
#include <stddef.h>
#include <string>
#include <vector>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#include "endpoint.hpp"
#include "error.hpp"
#include "receive_queue.hpp"
#include "shared_receive_queue.hpp"

#include "conn.hpp"
#include "mm.hpp"

namespace kym {
namespace connection {

class SendReceiveConnection : public Connection, public BatchSender {
public:
  SendReceiveConnection(endpoint::Endpoint *, endpoint::IReceiveQueue *,
                        bool rq_shared, memory::Allocator *);
  ~SendReceiveConnection();

  Status Close();

  StatusOr<SendRegion> GetMemoryRegion(size_t size);
  Status Send(std::vector<SendRegion> regions);
  StatusOr<uint64_t> SendAsync(std::vector<SendRegion> regions);
  Status Send(SendRegion region);
  StatusOr<uint64_t> SendAsync(SendRegion region);
  Status Free(SendRegion region);
  Status Wait(uint64_t id);

  StatusOr<ReceiveRegion> Receive();
  Status Free(ReceiveRegion);

  endpoint::Endpoint *GetEndpoint() { return this->ep_; };

private:
  memory::Allocator *allocator_;
  endpoint::Endpoint *ep_;

  uint64_t ackd_id_;
  uint64_t next_id_;

  endpoint::IReceiveQueue *rq_;
  bool rq_shared_;

  uint32_t next_post_id_;
  std::vector<uint32_t> to_post_;
  uint32_t max_to_post_;
};

StatusOr<SendReceiveConnection *> DialSendReceive(std::string ip, int port);
StatusOr<SendReceiveConnection *> DialSendReceive(std::string ip, int port,
                                                  std::string src);
StatusOr<SendReceiveConnection *> DialSendReceive(std::string ip, int port,
                                                  endpoint::IReceiveQueue *rq);
StatusOr<SendReceiveConnection *> DialSendReceive(std::string ip, int port,
                                                  std::string src,
                                                  endpoint::IReceiveQueue *rq);

class SendReceiveListener : public Receiver {
public:
  SendReceiveListener(endpoint::Listener *listener);
  SendReceiveListener(endpoint::Listener *listener,
                      endpoint::SharedReceiveQueue *srq);
  SendReceiveListener(endpoint::Listener *listener,
                      endpoint::SharedReceiveQueue *srq, bool single_receiver);
  ~SendReceiveListener();

  Status Close();

  // TODO(Fischi) Somhow signal end
  Status RunReceiver();

  StatusOr<SendReceiveConnection *> Accept();
  endpoint::Listener *GetListener() { return this->listener_; };

  StatusOr<ReceiveRegion> Receive();
  Status Free(ReceiveRegion);

private:
  endpoint::Listener *listener_;
  endpoint::SharedReceiveQueue *srq_;
  struct ibv_cq *rcv_cq_;

  std::map<uint32_t, endpoint::ReceiveQueue *> rqs_;

  bool single_receiver_;
};

StatusOr<SendReceiveListener *> ListenSendReceive(std::string ip, int port,
                                                  bool single_receiver);
StatusOr<SendReceiveListener *> ListenSendReceive(std::string ip, int port);
StatusOr<SendReceiveListener *> ListenSharedReceive(std::string ip, int port,
                                                    bool single_receiver);
StatusOr<SendReceiveListener *> ListenSharedReceive(std::string ip, int port);

} // namespace connection
} // namespace kym

#endif // KNY_CONN_SEND_RECEIVE_H_
