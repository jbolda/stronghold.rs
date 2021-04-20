// Copyright 2020-2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

mod protocol;

use crate::behaviour::{RequestId, EMPTY_QUEUE_SHRINK_THRESHOLD};
use futures::{channel::oneshot, future::BoxFuture, prelude::*, stream::FuturesUnordered};
use libp2p::{
    core::upgrade::{NegotiationError, UpgradeError},
    swarm::{
        protocols_handler::{KeepAlive, ProtocolsHandler, ProtocolsHandlerEvent, ProtocolsHandlerUpgrErr},
        SubstreamProtocol,
    },
};
pub use protocol::{MessageEvent, MessageProtocol, ProtocolSupport, RequestProtocol, ResponseProtocol};
use smallvec::SmallVec;
use std::{
    collections::VecDeque,
    io,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

#[doc(hidden)]
pub struct RequestResponseHandler<Req, Res>
where
    Req: MessageEvent,
    Res: MessageEvent,
{
    inbound_protocols: SmallVec<[MessageProtocol; 2]>,
    keep_alive_timeout: Duration,
    substream_timeout: Duration,
    keep_alive: KeepAlive,
    pending_error: Option<ProtocolsHandlerUpgrErr<io::Error>>,
    pending_events: VecDeque<RequestResponseHandlerEvent<Req, Res>>,
    outbound: VecDeque<RequestProtocol<Req, Res>>,
    inbound: FuturesUnordered<BoxFuture<'static, Result<((RequestId, Req), oneshot::Sender<Res>), oneshot::Canceled>>>,
    inbound_request_id: Arc<AtomicU64>,
}

impl<Req, Res> RequestResponseHandler<Req, Res>
where
    Req: MessageEvent,
    Res: MessageEvent,
{
    pub(super) fn new(
        inbound_protocols: SmallVec<[MessageProtocol; 2]>,
        keep_alive_timeout: Duration,
        substream_timeout: Duration,
        inbound_request_id: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inbound_protocols,
            keep_alive: KeepAlive::Yes,
            keep_alive_timeout,
            substream_timeout,
            outbound: VecDeque::new(),
            inbound: FuturesUnordered::new(),
            pending_events: VecDeque::new(),
            pending_error: None,
            inbound_request_id,
        }
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub enum RequestResponseHandlerEvent<Req, Res>
where
    Req: MessageEvent,
    Res: MessageEvent,
{
    Request {
        request_id: RequestId,
        request: Req,
        sender: oneshot::Sender<Res>,
    },
    Response {
        request_id: RequestId,
        response: Res,
    },
    ResponseSent(RequestId),
    ResponseOmission(RequestId),
    OutboundTimeout(RequestId),
    OutboundUnsupportedProtocols(RequestId),
    InboundTimeout(RequestId),
    InboundUnsupportedProtocols(RequestId),
}

impl<Req, Res> RequestResponseHandlerEvent<Req, Res>
where
    Req: MessageEvent,
    Res: MessageEvent,
{
    pub fn request_id(&self) -> &RequestId {
        match self {
            RequestResponseHandlerEvent::Request { request_id, .. }
            | RequestResponseHandlerEvent::Response { request_id, .. }
            | RequestResponseHandlerEvent::ResponseSent(request_id)
            | RequestResponseHandlerEvent::ResponseOmission(request_id)
            | RequestResponseHandlerEvent::OutboundTimeout(request_id)
            | RequestResponseHandlerEvent::OutboundUnsupportedProtocols(request_id)
            | RequestResponseHandlerEvent::InboundTimeout(request_id)
            | RequestResponseHandlerEvent::InboundUnsupportedProtocols(request_id) => request_id,
        }
    }
}

impl<Req, Res> ProtocolsHandler for RequestResponseHandler<Req, Res>
where
    Req: MessageEvent,
    Res: MessageEvent,
{
    type InEvent = RequestProtocol<Req, Res>;
    type OutEvent = RequestResponseHandlerEvent<Req, Res>;
    type Error = ProtocolsHandlerUpgrErr<io::Error>;
    type InboundProtocol = ResponseProtocol<Req, Res>;
    type OutboundProtocol = RequestProtocol<Req, Res>;
    type OutboundOpenInfo = RequestId;
    type InboundOpenInfo = RequestId;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        let (rq_send, rq_recv) = oneshot::channel();

        let (rs_send, rs_recv) = oneshot::channel();

        let request_id = RequestId::new(self.inbound_request_id.fetch_add(1, Ordering::Relaxed));

        let proto = ResponseProtocol {
            protocols: self.inbound_protocols.clone(),
            request_sender: rq_send,
            response_receiver: rs_recv,
            request_id,
        };

        self.inbound.push(rq_recv.map_ok(move |rq| (rq, rs_send)).boxed());

        SubstreamProtocol::new(proto, request_id).with_timeout(self.substream_timeout)
    }

    fn inject_fully_negotiated_inbound(&mut self, sent: bool, request_id: RequestId) {
        if sent {
            self.pending_events
                .push_back(RequestResponseHandlerEvent::ResponseSent(request_id))
        } else {
            self.pending_events
                .push_back(RequestResponseHandlerEvent::ResponseOmission(request_id))
        }
    }

    fn inject_fully_negotiated_outbound(&mut self, response: Res, request_id: RequestId) {
        self.pending_events
            .push_back(RequestResponseHandlerEvent::Response { request_id, response });
    }

    fn inject_event(&mut self, request: Self::InEvent) {
        self.keep_alive = KeepAlive::Yes;
        self.outbound.push_back(request);
    }

    fn inject_dial_upgrade_error(&mut self, info: RequestId, error: ProtocolsHandlerUpgrErr<io::Error>) {
        match error {
            ProtocolsHandlerUpgrErr::Timeout => {
                self.pending_events
                    .push_back(RequestResponseHandlerEvent::OutboundTimeout(info));
            }
            ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::Failed)) => {
                self.pending_events
                    .push_back(RequestResponseHandlerEvent::OutboundUnsupportedProtocols(info));
            }
            _ => {
                self.pending_error = Some(error);
            }
        }
    }

    fn inject_listen_upgrade_error(&mut self, info: RequestId, error: ProtocolsHandlerUpgrErr<io::Error>) {
        match error {
            ProtocolsHandlerUpgrErr::Timeout => self
                .pending_events
                .push_back(RequestResponseHandlerEvent::InboundTimeout(info)),
            ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::Failed)) => {
                self.pending_events
                    .push_back(RequestResponseHandlerEvent::InboundUnsupportedProtocols(info));
            }
            _ => {
                self.pending_error = Some(error);
            }
        }
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ProtocolsHandlerEvent<RequestProtocol<Req, Res>, RequestId, Self::OutEvent, Self::Error>> {
        if let Some(err) = self.pending_error.take() {
            return Poll::Ready(ProtocolsHandlerEvent::Close(err));
        }

        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(ProtocolsHandlerEvent::Custom(event));
        } else if self.pending_events.capacity() > EMPTY_QUEUE_SHRINK_THRESHOLD {
            self.pending_events.shrink_to_fit();
        }

        while let Poll::Ready(Some(result)) = self.inbound.poll_next_unpin(cx) {
            match result {
                Ok(((id, rq), rs_sender)) => {
                    self.keep_alive = KeepAlive::Yes;
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(RequestResponseHandlerEvent::Request {
                        request_id: id,
                        request: rq,
                        sender: rs_sender,
                    }));
                }
                Err(oneshot::Canceled) => {}
            }
        }

        if let Some(request) = self.outbound.pop_front() {
            let info = request.request_id;
            return Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(request, info).with_timeout(self.substream_timeout),
            });
        }

        debug_assert!(self.outbound.is_empty());

        if self.outbound.capacity() > EMPTY_QUEUE_SHRINK_THRESHOLD {
            self.outbound.shrink_to_fit();
        }

        if self.inbound.is_empty() && self.keep_alive.is_yes() {
            let until = Instant::now() + self.substream_timeout + self.keep_alive_timeout;
            self.keep_alive = KeepAlive::Until(until);
        }

        Poll::Pending
    }
}
