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

pub mod handler;

pub use handler::{MessageEvent, MessageProtocol, ProtocolSupport};

use futures::channel::oneshot;
use handler::{RequestProtocol, RequestResponseHandler, RequestResponseHandlerEvent};
use libp2p::{
    core::{connection::ConnectionId, ConnectedPoint, Multiaddr, PeerId},
    swarm::{DialPeerCondition, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, PollParameters},
};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc},
    task::{Context, Poll},
    time::Duration,
};

#[derive(Debug)]
pub enum RequestResponseMessage<TRequest, TResponse, TChannelResponse = TResponse> {
    Request {
        request_id: RequestId,
        request: TRequest,
        channel: ResponseChannel<TChannelResponse>,
    },
    Response {
        request_id: RequestId,
        response: TResponse,
    },
}

#[derive(Debug)]
pub enum RequestResponseEvent<TRequest, TResponse, TChannelResponse = TResponse> {
    Message {
        peer: PeerId,
        message: RequestResponseMessage<TRequest, TResponse, TChannelResponse>,
    },
    OutboundFailure {
        peer: PeerId,
        request_id: RequestId,
        error: OutboundFailure,
    },
    InboundFailure {
        peer: PeerId,
        request_id: RequestId,
        error: InboundFailure,
    },
    ResponseSent {
        peer: PeerId,
        request_id: RequestId,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum OutboundFailure {
    DialFailure,
    Timeout,
    ConnectionClosed,
    UnsupportedProtocols,
}

impl fmt::Display for OutboundFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutboundFailure::DialFailure => write!(f, "Failed to dial the requested peer"),
            OutboundFailure::Timeout => write!(f, "Timeout while waiting for a response"),
            OutboundFailure::ConnectionClosed => write!(f, "Connection was closed before a response was received"),
            OutboundFailure::UnsupportedProtocols => write!(f, "The remote supports none of the requested protocols"),
        }
    }
}

impl std::error::Error for OutboundFailure {}

#[derive(Debug, Clone, PartialEq)]
pub enum InboundFailure {
    Timeout,
    ConnectionClosed,
    UnsupportedProtocols,
    ResponseOmission,
}

impl fmt::Display for InboundFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InboundFailure::Timeout => write!(f, "Timeout while receiving request or sending response"),
            InboundFailure::ConnectionClosed => write!(f, "Connection was closed before a response could be sent"),
            InboundFailure::UnsupportedProtocols => write!(
                f,
                "The local peer supports none of the protocols requested by the remote"
            ),
            InboundFailure::ResponseOmission => write!(
                f,
                "The response channel was dropped without sending a response to the remote"
            ),
        }
    }
}

impl std::error::Error for InboundFailure {}

#[derive(Debug)]
pub struct ResponseChannel<TResponse> {
    request_id: RequestId,
    peer: PeerId,
    sender: oneshot::Sender<TResponse>,
}

impl<TResponse> ResponseChannel<TResponse> {
    pub fn is_open(&self) -> bool {
        !self.sender.is_canceled()
    }

    pub fn request_id(&self) -> RequestId {
        self.request_id
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct RequestId(u64);

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct RequestResponseConfig {
    request_timeout: Duration,
    connection_keep_alive: Duration,
}

impl Default for RequestResponseConfig {
    fn default() -> Self {
        Self {
            connection_keep_alive: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10),
        }
    }
}

impl RequestResponseConfig {
    pub fn set_connection_keep_alive(&mut self, v: Duration) -> &mut Self {
        self.connection_keep_alive = v;
        self
    }

    pub fn set_request_timeout(&mut self, v: Duration) -> &mut Self {
        self.request_timeout = v;
        self
    }
}

pub struct RequestResponse<Req, Res>
where
    Req: MessageEvent,
    Res: MessageEvent,
{
    inbound_protocols: SmallVec<[MessageProtocol; 2]>,
    outbound_protocols: SmallVec<[MessageProtocol; 2]>,
    next_request_id: RequestId,
    next_inbound_id: Arc<AtomicU64>,
    config: RequestResponseConfig,
    pending_events: VecDeque<NetworkBehaviourAction<RequestProtocol<Req, Res>, RequestResponseEvent<Req, Res>>>,
    connected: HashMap<PeerId, SmallVec<[Connection; 2]>>,
    addresses: HashMap<PeerId, SmallVec<[Multiaddr; 6]>>,
    pending_outbound_requests: HashMap<PeerId, SmallVec<[RequestProtocol<Req, Res>; 10]>>,
}

impl<Req, Res> RequestResponse<Req, Res>
where
    Req: MessageEvent,
    Res: MessageEvent,
{
    pub fn new<I>(protocols: I, cfg: RequestResponseConfig) -> Self
    where
        I: IntoIterator<Item = (MessageProtocol, ProtocolSupport)>,
    {
        let mut inbound_protocols = SmallVec::new();
        let mut outbound_protocols = SmallVec::new();
        for (p, s) in protocols {
            if s.inbound() {
                inbound_protocols.push(p.clone());
            }
            if s.outbound() {
                outbound_protocols.push(p.clone());
            }
        }
        RequestResponse {
            inbound_protocols,
            outbound_protocols,
            next_request_id: RequestId(1),
            next_inbound_id: Arc::new(AtomicU64::new(1)),
            config: cfg,
            pending_events: VecDeque::new(),
            connected: HashMap::new(),
            pending_outbound_requests: HashMap::new(),
            addresses: HashMap::new(),
        }
    }

    pub fn send_request(&mut self, peer: &PeerId, request: Req) -> RequestId {
        let request_id = self.next_request_id();
        let request = RequestProtocol {
            request_id,
            protocols: self.outbound_protocols.clone(),
            request,
            marker: PhantomData,
        };

        if let Some(request) = self.try_send_request(peer, request) {
            self.pending_events.push_back(NetworkBehaviourAction::DialPeer {
                peer_id: *peer,
                condition: DialPeerCondition::Disconnected,
            });
            self.pending_outbound_requests.entry(*peer).or_default().push(request);
        }

        request_id
    }

    pub fn send_response(&mut self, ch: ResponseChannel<Res>, rs: Res) -> Result<(), Res> {
        ch.sender.send(rs)
    }

    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) {
        self.addresses.entry(*peer).or_default().push(address);
    }

    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr) {
        let mut last = false;
        if let Some(addresses) = self.addresses.get_mut(peer) {
            addresses.retain(|a| a != address);
            last = addresses.is_empty();
        }
        if last {
            self.addresses.remove(peer);
        }
    }

    pub fn is_connected(&self, peer: &PeerId) -> bool {
        if let Some(connections) = self.connected.get(peer) {
            !connections.is_empty()
        } else {
            false
        }
    }

    pub fn is_pending_outbound(&self, peer: &PeerId, request_id: &RequestId) -> bool {
        let est_conn = self
            .connected
            .get(peer)
            .map(|cs| cs.iter().any(|c| c.pending_inbound_responses.contains(request_id)))
            .unwrap_or(false);
        let pen_conn = self
            .pending_outbound_requests
            .get(peer)
            .map(|rps| rps.iter().any(|rp| rp.request_id == *request_id))
            .unwrap_or(false);

        est_conn || pen_conn
    }

    pub fn is_pending_inbound(&self, peer: &PeerId, request_id: &RequestId) -> bool {
        self.connected
            .get(peer)
            .map(|cs| cs.iter().any(|c| c.pending_outbound_responses.contains(request_id)))
            .unwrap_or(false)
    }

    fn next_request_id(&mut self) -> RequestId {
        let request_id = self.next_request_id;
        self.next_request_id.0 += 1;
        request_id
    }

    fn try_send_request(
        &mut self,
        peer: &PeerId,
        request: RequestProtocol<Req, Res>,
    ) -> Option<RequestProtocol<Req, Res>> {
        if let Some(connections) = self.connected.get_mut(peer) {
            if connections.is_empty() {
                return Some(request);
            }
            let ix = (request.request_id.0 as usize) % connections.len();
            let conn = &mut connections[ix];
            conn.pending_inbound_responses.insert(request.request_id);
            self.pending_events.push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id: *peer,
                handler: NotifyHandler::One(conn.id),
                event: request,
            });
            None
        } else {
            Some(request)
        }
    }

    fn remove_pending_outbound_response(
        &mut self,
        peer: &PeerId,
        connection: ConnectionId,
        request: RequestId,
    ) -> bool {
        self.get_connection_mut(peer, connection)
            .map(|c| c.pending_outbound_responses.remove(&request))
            .unwrap_or(false)
    }

    fn remove_pending_inbound_response(
        &mut self,
        peer: &PeerId,
        connection: ConnectionId,
        request: &RequestId,
    ) -> bool {
        self.get_connection_mut(peer, connection)
            .map(|c| c.pending_inbound_responses.remove(request))
            .unwrap_or(false)
    }

    fn get_connection_mut(&mut self, peer: &PeerId, connection: ConnectionId) -> Option<&mut Connection> {
        self.connected
            .get_mut(peer)
            .and_then(|connections| connections.iter_mut().find(|c| c.id == connection))
    }
}

impl<Req, Res> NetworkBehaviour for RequestResponse<Req, Res>
where
    Req: MessageEvent,
    Res: MessageEvent,
{
    type ProtocolsHandler = RequestResponseHandler<Req, Res>;
    type OutEvent = RequestResponseEvent<Req, Res>;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        RequestResponseHandler::new(
            self.inbound_protocols.clone(),
            self.config.connection_keep_alive,
            self.config.request_timeout,
            self.next_inbound_id.clone(),
        )
    }

    fn addresses_of_peer(&mut self, peer: &PeerId) -> Vec<Multiaddr> {
        let mut addresses = Vec::new();
        if let Some(connections) = self.connected.get(peer) {
            addresses.extend(connections.iter().filter_map(|c| c.address.clone()))
        }
        if let Some(more) = self.addresses.get(peer) {
            addresses.extend(more.into_iter().cloned());
        }
        addresses
    }

    fn inject_connected(&mut self, peer: &PeerId) {
        if let Some(pending) = self.pending_outbound_requests.remove(peer) {
            for request in pending {
                let request = self.try_send_request(peer, request);
                assert!(request.is_none());
            }
        }
    }

    fn inject_connection_established(&mut self, peer: &PeerId, conn: &ConnectionId, endpoint: &ConnectedPoint) {
        let address = match endpoint {
            ConnectedPoint::Dialer { address } => Some(address.clone()),
            ConnectedPoint::Listener { .. } => None,
        };
        self.connected
            .entry(*peer)
            .or_default()
            .push(Connection::new(*conn, address));
    }

    fn inject_connection_closed(&mut self, peer_id: &PeerId, conn: &ConnectionId, _: &ConnectedPoint) {
        let connections = self
            .connected
            .get_mut(peer_id)
            .expect("Expected some established connection to peer before closing.");

        let connection = connections
            .iter()
            .position(|c| &c.id == conn)
            .map(|p: usize| connections.remove(p))
            .expect("Expected connection to be established before closing.");

        if connections.is_empty() {
            self.connected.remove(peer_id);
        }

        for request_id in connection.pending_outbound_responses {
            self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                RequestResponseEvent::InboundFailure {
                    peer: *peer_id,
                    request_id,
                    error: InboundFailure::ConnectionClosed,
                },
            ));
        }

        for request_id in connection.pending_inbound_responses {
            self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                RequestResponseEvent::OutboundFailure {
                    peer: *peer_id,
                    request_id,
                    error: OutboundFailure::ConnectionClosed,
                },
            ));
        }
    }

    fn inject_disconnected(&mut self, peer: &PeerId) {
        self.connected.remove(peer);
    }

    fn inject_dial_failure(&mut self, peer: &PeerId) {
        if let Some(pending) = self.pending_outbound_requests.remove(peer) {
            for request in pending {
                self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                    RequestResponseEvent::OutboundFailure {
                        peer: *peer,
                        request_id: request.request_id,
                        error: OutboundFailure::DialFailure,
                    },
                ));
            }
        }
    }

    fn inject_event(&mut self, peer: PeerId, connection: ConnectionId, event: RequestResponseHandlerEvent<Req, Res>) {
        match event {
            RequestResponseHandlerEvent::Response { request_id, response } => {
                let removed = self.remove_pending_inbound_response(&peer, connection, &request_id);
                debug_assert!(removed, "Expect request_id to be pending before receiving response.",);

                let message = RequestResponseMessage::Response { request_id, response };
                self.pending_events
                    .push_back(NetworkBehaviourAction::GenerateEvent(RequestResponseEvent::Message {
                        peer,
                        message,
                    }));
            }
            RequestResponseHandlerEvent::Request {
                request_id,
                request,
                sender,
            } => {
                let channel = ResponseChannel {
                    request_id,
                    peer,
                    sender,
                };
                let message = RequestResponseMessage::Request {
                    request_id,
                    request,
                    channel,
                };
                self.pending_events
                    .push_back(NetworkBehaviourAction::GenerateEvent(RequestResponseEvent::Message {
                        peer,
                        message,
                    }));

                match self.get_connection_mut(&peer, connection) {
                    Some(connection) => {
                        let inserted = connection.pending_outbound_responses.insert(request_id);
                        debug_assert!(inserted, "Expect id of new request to be unknown.");
                    }
                    None => {
                        self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                            RequestResponseEvent::InboundFailure {
                                peer,
                                request_id,
                                error: InboundFailure::ConnectionClosed,
                            },
                        ));
                    }
                }
            }
            RequestResponseHandlerEvent::ResponseSent(request_id) => {
                let removed = self.remove_pending_outbound_response(&peer, connection, request_id);
                debug_assert!(removed, "Expect request_id to be pending before response is sent.");

                self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                    RequestResponseEvent::ResponseSent { peer, request_id },
                ));
            }
            RequestResponseHandlerEvent::ResponseOmission(request_id) => {
                let removed = self.remove_pending_outbound_response(&peer, connection, request_id);
                debug_assert!(removed, "Expect request_id to be pending before response is omitted.",);

                self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                    RequestResponseEvent::InboundFailure {
                        peer,
                        request_id,
                        error: InboundFailure::ResponseOmission,
                    },
                ));
            }
            RequestResponseHandlerEvent::OutboundTimeout(request_id) => {
                let removed = self.remove_pending_inbound_response(&peer, connection, &request_id);
                debug_assert!(removed, "Expect request_id to be pending before request times out.");

                self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                    RequestResponseEvent::OutboundFailure {
                        peer,
                        request_id,
                        error: OutboundFailure::Timeout,
                    },
                ));
            }
            RequestResponseHandlerEvent::InboundTimeout(request_id) => {
                self.remove_pending_outbound_response(&peer, connection, request_id);

                self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                    RequestResponseEvent::InboundFailure {
                        peer,
                        request_id,
                        error: InboundFailure::Timeout,
                    },
                ));
            }
            RequestResponseHandlerEvent::OutboundUnsupportedProtocols(request_id) => {
                let removed = self.remove_pending_inbound_response(&peer, connection, &request_id);
                debug_assert!(removed, "Expect request_id to be pending before failing to connect.",);

                self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                    RequestResponseEvent::OutboundFailure {
                        peer,
                        request_id,
                        error: OutboundFailure::UnsupportedProtocols,
                    },
                ));
            }
            RequestResponseHandlerEvent::InboundUnsupportedProtocols(request_id) => {
                self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                    RequestResponseEvent::InboundFailure {
                        peer,
                        request_id,
                        error: InboundFailure::UnsupportedProtocols,
                    },
                ));
            }
        }
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<RequestProtocol<Req, Res>, RequestResponseEvent<Req, Res>>> {
        if let Some(ev) = self.pending_events.pop_front() {
            return Poll::Ready(ev);
        } else if self.pending_events.capacity() > EMPTY_QUEUE_SHRINK_THRESHOLD {
            self.pending_events.shrink_to_fit();
        }

        Poll::Pending
    }
}

const EMPTY_QUEUE_SHRINK_THRESHOLD: usize = 100;

struct Connection {
    id: ConnectionId,
    address: Option<Multiaddr>,
    pending_outbound_responses: HashSet<RequestId>,
    pending_inbound_responses: HashSet<RequestId>,
}

impl Connection {
    fn new(id: ConnectionId, address: Option<Multiaddr>) -> Self {
        Self {
            id,
            address,
            pending_outbound_responses: Default::default(),
            pending_inbound_responses: Default::default(),
        }
    }
}
