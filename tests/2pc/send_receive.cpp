#include "send_receive.hpp"


#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>
#include <vector>

#include "error.hpp"
#include "endpoint.hpp"
#include "conn.hpp"
#include "mm.hpp"
#include "mm/dumb_allocator.hpp"
#include "receive_queue.hpp"
#include "shared_receive_queue.hpp"

#include "debug.h"

namespace kym {
namespace connection {
namespace {

const uint64_t inflight = 512;
const uint64_t max_conn = 32;

endpoint::Options defaultOptions = {
  .pd = NULL,
  .qp_attr = {
    .qp_context = NULL,
    .send_cq = NULL,
    .recv_cq = NULL,
    .srq = NULL,
    .cap = {
      .max_send_wr = 2*inflight,
      .max_recv_wr = 2*inflight,
      .max_send_sge = 1,
      .max_recv_sge = 1,
      .max_inline_data = 0,
    },
    .qp_type = IBV_QPT_RC,
    .sq_sig_all = 0,
  },
  .use_srq = false,
  .private_data = NULL,
  .private_data_len = 0,
  .responder_resources = 0,
  .initiator_depth =  0,
  .flow_control = 0,
  .retry_count = 5,  
  .rnr_retry_count = 2, 
  .native_qp = false,
  .inline_recv = 0,
};


}

/*
 * Client Dial
 */

StatusOr<SendReceiveConnection *> DialSendReceive(std::string ip, int port){
  return DialSendReceive(ip, port, NULL);
}

StatusOr<SendReceiveConnection *> DialSendReceive(std::string ip, int port, std::string src){
  // Default to port 18515
  if (port == 0) {
    port = 18515;
  }

  auto opts = defaultOptions;
  if (!src.empty()){
    opts.src = src.c_str();
  }

  auto epStatus = kym::endpoint::Dial(ip, port, opts);
  if (!epStatus.ok()){
    return epStatus.status();
  }
  endpoint::Endpoint *ep = epStatus.value();

  auto allocator = new memory::DumbAllocator(ep->GetPd());

  //TODO(Fischi) parameterize
  auto rq_stat = endpoint::GetReceiveQueue(ep, 16*1024, inflight);
  if (!rq_stat.ok()){
    return rq_stat.status().Wrap("error creating receive queue while dialing");
  }
  endpoint::ReceiveQueue *rq = rq_stat.value();

  auto conn = new SendReceiveConnection(ep, rq, false, allocator);

  return conn;
}

StatusOr<SendReceiveConnection *> DialSendReceive(std::string ip, int port, endpoint::IReceiveQueue * rq){
  return DialSendReceive(ip, port, NULL, rq);
}
StatusOr<SendReceiveConnection *> DialSendReceive(std::string ip, int port, std::string src, endpoint::IReceiveQueue * rq){
  // Default to port 18515
  if (port == 0) {
    port = 18515;
  }

  auto opts = defaultOptions;
  if (!src.empty()){
    opts.src = src.c_str();
  }
  auto epStatus = kym::endpoint::Dial(ip, port, opts);
  if (!epStatus.ok()){
    return epStatus.status();
  }
  endpoint::Endpoint *ep = epStatus.value();

  auto allocator = new memory::DumbAllocator(ep->GetPd());

  auto conn = new SendReceiveConnection(ep, rq, true, allocator);
  return conn;
}

/* 
 * Server listener
 */
StatusOr<SendReceiveListener *> ListenSendReceive(std::string ip, int port, bool shared_rcv) {
  // Default to port 18515
  if (port == 0) {
    port = 18515;
  }
  
  auto lnStatus = endpoint::Listen(ip, port);
  if (!lnStatus.ok()){
    return lnStatus.status();
  }
  endpoint::Listener *ln = lnStatus.value();
  auto rcvLn = new SendReceiveListener(ln, nullptr, shared_rcv);
  return rcvLn;
}
StatusOr<SendReceiveListener *> ListenSendReceive(std::string ip, int port) {
  return ListenSendReceive(ip, port, false);
}

StatusOr<SendReceiveListener *> ListenSharedReceive(std::string ip, int port, bool shared_rcv) {
  // Default to port 18515
  if (port == 0) {
    port = 18515;
  }
  
  auto lnStatus = endpoint::Listen(ip, port);
  if (!lnStatus.ok()){
    return lnStatus.status();
  }
  endpoint::Listener *ln = lnStatus.value();

  //TODO(Fischi) parameterize
  auto srq_stat = endpoint::GetSharedReceiveQueue(ln->GetPd(), 16*1024, 2*inflight);
  if (!srq_stat.ok()){
    return srq_stat.status().Wrap("error creating shared receive queue");
  }
  endpoint::SharedReceiveQueue *srq = srq_stat.value();

  auto rcvLn = new SendReceiveListener(ln, srq, shared_rcv);
  return rcvLn;
}
StatusOr<SendReceiveListener *> ListenSharedReceive(std::string ip, int port) {
  return ListenSharedReceive(ip, port, false);
}

StatusOr<SendReceiveConnection *> SendReceiveListener::Accept(){
  kym::endpoint::Options opts = defaultOptions;
  if (this->srq_ != nullptr){
    opts.qp_attr.srq = this->srq_->GetSRQ();
  }
  if (this->single_receiver_) {
    if (this->rcv_cq_ == nullptr) {
      this->rcv_cq_ = ibv_create_cq(this->listener_->GetContext(), 1024, NULL, NULL, 0); // TODO(Fischi) not sure how big that should be
      debug(stderr, "new rcv_cq_: %p\n", this->rcv_cq_);
    }
    opts.qp_attr.recv_cq = this->rcv_cq_;
  }

  debug(stderr, "Accepting sl rcv_cq_: %p\n", this->rcv_cq_);
  auto epStatus = this->listener_->Accept(opts);
  if (!epStatus.ok()){
    return epStatus.status();
  }
  debug(stderr, "Accepted sl\n");
  endpoint::Endpoint *ep = epStatus.value();

  auto allocator = new memory::DumbAllocator(ep->GetPd());

  endpoint::IReceiveQueue *rq;
  if (this->srq_ == nullptr){
    auto rq_stat = endpoint::GetReceiveQueue(ep, 16*1024, inflight);
    if (!rq_stat.ok()){
      return rq_stat.status().Wrap("error creating receive queue while accepting");
    }
    auto trq = rq_stat.value();
    debug(stderr, "pushback rq %p, for qp %d\n",trq, ep->GetQpNum());
    if (this->single_receiver_) this->rqs_[ep->GetQpNum()] = trq;
    debug(stderr, "pushedback rq\n");
    rq = trq;
  } else {
    auto rq_stat = this->srq_->NewReceiver();
    if (!rq_stat.ok()){
      return rq_stat.status().Wrap("error creating shared receive queue while accepting");
    }
    rq = rq_stat.value();
  }

  auto conn = new SendReceiveConnection(ep, rq, this->srq_ != nullptr, allocator);

  debug(stderr, "new conn\n");
  return conn;

}
StatusOr<ReceiveRegion> SendReceiveListener::Receive(){
  struct ibv_wc wc;
  debug(stderr, "Polling on cq %p\n", this->rcv_cq_);

  int ret = 0;

  while(ret == 0){
    ret = ibv_poll_cq(this->rcv_cq_, 1, &wc);
    if (ret < 0){
      return Status(StatusCode::Internal, "error polling recv cq\n");
    }
  }
  

  debug(stderr, "Got wc with ctx %d for qp %d from %d with ret %d\n", wc.wr_id, wc.qp_num, wc.src_qp, ret);
  if (this->srq_ != nullptr){
    struct endpoint::mr mr = this->srq_->GetMR(wc.wr_id);
    ReceiveRegion reg;
    reg.context = wc.wr_id;
    reg.addr = mr.addr;
    reg.length = wc.byte_len;

    return reg;
  }
  
  auto rq = this->rqs_[wc.qp_num];

  debug(stderr, "Got rq %p\n", rq);
  struct endpoint::mr mr = rq->GetMR(wc.wr_id);
  ReceiveRegion reg;
  // In our case the wr_id is never larger than 32 bits. That means we can encode the qp num in the upper 32 bits 
  // so we will cast it to a 64 bit integer. shift it 32 bits and take the bitwise or to move the qp num in the upper 32 bits
  reg.context = (((uint64_t)wc.qp_num) << 32) | wc.wr_id;
  reg.addr = mr.addr;
  reg.length = wc.byte_len;
  return reg;
}

Status SendReceiveListener::Free(ReceiveRegion region){
  if (this->srq_ != nullptr){
    return this->srq_->PostMR(region.context);
  }

  // qp num is in upper 32 bits. So shift right
  uint32_t qp_num = region.context >> 32;
  debug(stderr, "Getting rq for qp %d", qp_num);
  auto rq = this->rqs_[qp_num];
  debug(stderr, "Got rq %p", rq);
  // wr_id is in lower 32 bits.
  debug(stderr, "Freeeing %p", rq);
  return rq->PostMR(region.context & 0xFFFFFFFF);
}

SendReceiveListener::SendReceiveListener(endpoint::Listener *listener) : listener_(listener), srq_(NULL), rcv_cq_(nullptr) {}
SendReceiveListener::SendReceiveListener(endpoint::Listener *listener, 
    endpoint::SharedReceiveQueue *srq ) : listener_(listener), srq_(srq), rcv_cq_(nullptr) {}
SendReceiveListener::SendReceiveListener(endpoint::Listener *listener, endpoint::SharedReceiveQueue *srq, 
    bool single_receiver ) : listener_(listener), srq_(srq), rcv_cq_(nullptr), single_receiver_(single_receiver) {}

SendReceiveListener::~SendReceiveListener(){
  delete this->listener_;
  delete this->srq_;
}
Status SendReceiveListener::Close() {
  if (this->srq_ != nullptr){
    this->srq_->Close();
  }
  return this->listener_->Close();
}
Status SendReceiveListener::RunReceiver(){
  return this->srq_->Run();
}
/*
 *SendReceiveConnection
 */
SendReceiveConnection::SendReceiveConnection(endpoint::Endpoint *ep, 
    endpoint::IReceiveQueue *rq, bool rq_shared, memory::Allocator *allocator){
  this->allocator_ = allocator;
  this->ep_ = ep;
  this->rq_ = rq;
  this->rq_shared_ = rq_shared;
  this->ackd_id_ = 0;
  this->next_id_ = 1;

  this->next_post_id_ = 0;
  this->max_to_post_ = inflight/4;
  this->to_post_.reserve(inflight/4);
}

SendReceiveConnection::~SendReceiveConnection(){
  delete this->allocator_;
  delete this->ep_;
  if (!this->rq_shared_){
    delete this->rq_;
  }
}
Status SendReceiveConnection::Close() {
  if (!this->rq_shared_){
    auto stat = this->rq_->Close();
    if (!stat.ok()){
      return stat.Wrap("error closing receive queue for SendReceiveConnection");
    }
  }
  return this->ep_->Close();
}

StatusOr<SendRegion> SendReceiveConnection::GetMemoryRegion(size_t size){
  auto mrStatus =  this->allocator_->Alloc(size);
  if (!mrStatus.ok()){
    return mrStatus.status();
  }
  auto mr = mrStatus.value();
  struct SendRegion reg = {0};
  reg.context = mr.context;
  reg.addr = mr.addr;
  reg.length = mr.length;
  reg.lkey = mr.lkey;
  return reg;
}

Status SendReceiveConnection::Send(std::vector<SendRegion> regions){
  auto id_stat = this->SendAsync(regions);
  if (!id_stat.ok()){
    return id_stat.status();
  }
  return this->Wait(id_stat.value());
}
StatusOr<uint64_t> SendReceiveConnection::SendAsync(std::vector<SendRegion> regions){
  int i = 0;
  int batch_size = regions.size();
  ibv_send_wr wrs[batch_size];
  ibv_sge sges[batch_size];

  uint64_t id = this->next_id_++;
  for ( auto region : regions){
    struct ibv_sge *sge = &sges[i];
    ibv_send_wr *wr = &wrs[i];
    bool last = (++i == batch_size);

    sge->addr = (uintptr_t)region.addr;
    sge->length = region.length;
    sge->lkey =  region.lkey;

    wr->wr_id = id;
    wr->next = last ? NULL : &(wrs[i]);
    wr->sg_list = sge;
    wr->num_sge = 1;
    wr->opcode = IBV_WR_SEND;

    wr->send_flags = last ? IBV_SEND_SIGNALED : 0;  
  }

  struct ibv_send_wr *bad;
  auto stat = this->ep_->PostSendRaw(&(wrs[0]), &bad);
  if (!stat.ok()){
    return stat;
  }
  return id;
}
Status SendReceiveConnection::Wait(uint64_t id){
  while (this->ackd_id_ < id){
    auto wcStatus = this->ep_->PollSendCq();
    if (!wcStatus.ok()){
      return wcStatus.status();
    }
    ibv_wc wc = wcStatus.value();
    this->ackd_id_ = wc.wr_id;
  }
  return Status();
}
Status SendReceiveConnection::Send(SendRegion region){
  auto id_stat = this->SendAsync(region);
  if (!id_stat.ok()){
    return id_stat.status();
  }
  return this->Wait(id_stat.value());
}
StatusOr<uint64_t> SendReceiveConnection::SendAsync(SendRegion region){
  uint64_t id = this->next_id_++;
  auto sendStatus = this->ep_->PostSend(id, region.lkey, region.addr, region.length);
  if (!sendStatus.ok()){
    return sendStatus;
  }
  return id;
}
Status SendReceiveConnection::Free(SendRegion region){
  memory::Region mr = memory::Region();
  mr.addr = region.addr;
  mr.length = region.length;
  mr.lkey = region.lkey;
  mr.context = region.context;
  return this->allocator_->Free(mr);
}

StatusOr<ReceiveRegion> SendReceiveConnection::Receive(){
  auto wcStatus = this->ep_->PollRecvCq();
  if (!wcStatus.ok()){
    return wcStatus.status().Wrap("error while polling recieve queue for receive");
  }

  struct ibv_wc wc = wcStatus.value();

  struct endpoint::mr mr = this->rq_->GetMR(wc.wr_id);
  ReceiveRegion reg;
  reg.context = wc.wr_id;
  reg.addr = mr.addr;
  reg.length = wc.byte_len;
  return reg;
}

Status SendReceiveConnection::Free(ReceiveRegion region){
  return this->rq_->PostMR(region.context);
}
}
}
