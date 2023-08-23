// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/endpoints"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"google.golang.org/protobuf/proto"
)

const (
	// URL suffixes for various HTTP endpoints exported by the manager.

	// These endpoints are called by the distributor.
	deployURL             = "/manager/deploy"
	stopURL               = "/manager/stop"
	deleteURL             = "/manager/delete"
	getReplicaSetStateURL = "/manager/get_replica_set_state"

	// These endpoints are called by the babysitter.
	activateComponentURL    = "/manager/activate_component"
	registerReplicaURL      = "/manager/register_replica"
	reportLoadURL           = "/manager/report_load"
	getListenerAddressURL   = "/manager/get_listener_address"
	exportListenerURL       = "/manager/export_listener"
	getRoutingInfoURL       = "/manager/get_routing_info"
	getComponentsToStartURL = "/manager/get_components_to_start"
)

type configProtoPointer[T any] interface {
	*T
	GetConfig() *config.GKEConfig
	proto.Message
}

type httpServer struct {
	m              endpoints.Manager
	logger         *slog.Logger
	lis            net.Listener
	getSelfCert    func() ([]byte, []byte, error)
	verifyPeerCert func([]*x509.Certificate) (string, error)
}

func (s httpServer) run() error {
	mux := http.NewServeMux()
	mux.HandleFunc(deployURL, peerAuthorizationHandler(&s, "distributor", protomsg.HandlerDo(s.logger, s.m.Deploy)))
	mux.HandleFunc(stopURL, peerAuthorizationHandler(&s, "distributor", protomsg.HandlerDo(s.logger, s.m.Stop)))
	mux.HandleFunc(deleteURL, peerAuthorizationHandler(&s, "distributor", protomsg.HandlerDo(s.logger, s.m.Delete)))
	mux.HandleFunc(getReplicaSetStateURL, peerAuthorizationHandler(&s, "distributor", protomsg.HandlerFunc(s.logger, s.m.GetReplicaSetState)))

	// TODO(spetrovic): Implement authorization.
	var babysitterAuthorizer func(string, *config.GKEConfig) error
	mux.HandleFunc(activateComponentURL, configAuthorizationHandlerDo(&s, babysitterAuthorizer, s.m.ActivateComponent))
	mux.HandleFunc(registerReplicaURL, configAuthorizationHandlerDo(&s, babysitterAuthorizer, s.m.RegisterReplica))
	mux.HandleFunc(reportLoadURL, configAuthorizationHandlerDo(&s, babysitterAuthorizer, s.m.ReportLoad))
	mux.HandleFunc(getListenerAddressURL, configAuthorizationHandlerFunc(&s, babysitterAuthorizer, s.m.GetListenerAddress))
	mux.HandleFunc(exportListenerURL, configAuthorizationHandlerFunc(&s, babysitterAuthorizer, s.m.ExportListener))
	mux.HandleFunc(getRoutingInfoURL, configAuthorizationHandlerFunc(&s, babysitterAuthorizer, s.m.GetRoutingInfo))
	mux.HandleFunc(getComponentsToStartURL, configAuthorizationHandlerFunc(&s, babysitterAuthorizer, s.m.GetComponentsToStart))

	if s.getSelfCert == nil { // No TLS
		server := http.Server{Handler: mux}
		return server.Serve(s.lis)
	}
	tlsConfig := &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			certPEM, keyPEM, err := s.getSelfCert()
			if err != nil {
				return nil, err
			}
			cert, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				return nil, err
			}
			return &cert, nil
		},
		ClientAuth:            tls.RequireAnyClientCert,
		VerifyPeerCertificate: nil, // Verification happens in the HTTP handler.
	}
	server := &http.Server{
		Handler:   mux,
		TLSConfig: tlsConfig,
	}
	return server.ServeTLS(s.lis, "", "")
}

func configAuthorizationHandlerDo[I any, IP configProtoPointer[I]](s *httpServer, authorizer func(string, *config.GKEConfig) error, handler func(context.Context, *I) error) http.HandlerFunc {
	if s.verifyPeerCert == nil || authorizer == nil { // no authorization
		return protomsg.HandlerDo[I, IP](s.logger, handler)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract the peer identity from the underlying connection.
		peer, err := s.verifyPeerCert(r.TLS.PeerCertificates)
		if err != nil {
			http.Error(w, fmt.Sprintf("cannot verify client: %s", err.Error()), http.StatusBadRequest)
			return
		}
		wrap := func(ctx context.Context, in *I) error {
			// Get the deployment config from the request.
			cfg := IP(in).GetConfig()
			if err := authorizer(peer, cfg); err != nil {
				return fmt.Errorf("unauthorized peer %s: %w", peer, err)
			}
			return handler(ctx, in)
		}
		h := protomsg.HandlerDo[I, IP](s.logger, wrap)
		h(w, r)
	}
}

func configAuthorizationHandlerFunc[I, O any, IP configProtoPointer[I], OP protomsg.ProtoPointer[O]](s *httpServer, authorizer func(string, *config.GKEConfig) error, handler func(context.Context, *I) (*O, error)) http.HandlerFunc {
	if s.verifyPeerCert == nil || authorizer == nil { // no authorization
		return protomsg.HandlerFunc[I, O, IP, OP](s.logger, handler)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract the peer identity from the underlying connection.
		peer, err := s.verifyPeerCert(r.TLS.PeerCertificates)
		if err != nil {
			http.Error(w, fmt.Sprintf("cannot verify client: %s", err.Error()), http.StatusBadRequest)
			return
		}
		wrap := func(ctx context.Context, in *I) (*O, error) {
			// Get the deployment config from the request.
			cfg := IP(in).GetConfig()
			if err := authorizer(peer, cfg); err != nil {
				return nil, fmt.Errorf("unauthorized peer %s: %w", peer, err)
			}
			return handler(ctx, in)
		}
		h := protomsg.HandlerFunc[I, O, IP, OP](s.logger, wrap)
		h(w, r)
	}
}

func peerAuthorizationHandler(s *httpServer, expectedPeer string, handler http.HandlerFunc) http.HandlerFunc {
	if s.verifyPeerCert == nil || expectedPeer == "" { // no authorization
		return handler
	}
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract the peer identity from the underlying connection.
		peer, err := s.verifyPeerCert(r.TLS.PeerCertificates)
		if err != nil {
			http.Error(w, fmt.Sprintf("cannot verify client: %s", err.Error()), http.StatusBadRequest)
			return
		}
		if expectedPeer != peer {
			http.Error(w, fmt.Sprintf("unauthorized peer: want %s, got %s", expectedPeer, peer), http.StatusUnauthorized)
			return
		}
		handler(w, r)
	}
}

// RunHTTPServer runs the HTTP server that handles requests for the given
// manager.
func RunHTTPServer(m endpoints.Manager, logger *slog.Logger, lis net.Listener, getSelfCert func() ([]byte, []byte, error), verifyPeerCert func([]*x509.Certificate) (string, error)) error {
	return httpServer{
		m:              m,
		logger:         logger,
		lis:            lis,
		getSelfCert:    getSelfCert,
		verifyPeerCert: verifyPeerCert,
	}.run()
}
