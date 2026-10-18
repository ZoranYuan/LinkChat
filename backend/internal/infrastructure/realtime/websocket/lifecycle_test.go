package websocket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gorilla "github.com/gorilla/websocket"
)

func newLifecycleGateway(t *testing.T) *Gateway {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	gateway := NewGateway(ctx)
	t.Cleanup(func() {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_ = gateway.Shutdown(shutdownCtx)
	})
	return gateway
}

func newLifecycleConnection(t *testing.T, gateway *Gateway) *Connection {
	t.Helper()
	accepted := make(chan *gorilla.Conn, 1)
	upgrader := gorilla.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			accepted <- conn
		}
	}))
	t.Cleanup(server.Close)
	client, _, err := gorilla.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	conn := <-accepted
	t.Cleanup(func() { _ = conn.Close() })
	identity, err := NewConnectionIdentity("user-1", "device-1", string(PlatfromWeb), "auth-session-1")
	if err != nil {
		t.Fatal(err)
	}
	return NewConnection(gateway.NetworkContext(), conn, identity, 8, nil, MessageBatchConfig{})
}

func registerLifecycleConnection(t *testing.T, gateway *Gateway, connection *Connection, handler MessageHandler) <-chan struct{} {
	t.Helper()
	unregistered := make(chan struct{})
	if err := gateway.RegisterAndStart(connection, 60, 50, 1, handler, func(c *Connection) {
		gateway.Unregister(c)
		close(unregistered)
	}); err != nil {
		t.Fatal(err)
	}
	return unregistered
}

func waitLifecycleSignal(t *testing.T, signal <-chan struct{}, reason string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal(reason)
	}
}

func TestGatewayGlobalCancelDrainsInboundBeforeCancelingBusiness(t *testing.T) {
	gateway := newLifecycleGateway(t)
	connection := newLifecycleConnection(t, gateway)
	connection.inbound <- Message{Op: "first"}
	connection.inbound <- Message{Op: "second"}
	started := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan string, 2)
	handlerErrors := make(chan error, 2)
	unregistered := registerLifecycleConnection(t, gateway, connection, func(ctx context.Context, _ *Connection, op string, _ []byte) {
		if op == "first" {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		_, err := gateway.OnlineRoomConnectionCount(ctx, "room-1")
		handlerErrors <- err
		completed <- op
	})
	waitLifecycleSignal(t, started, "first callback did not start")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- gateway.Shutdown(shutdownCtx) }()
	waitLifecycleSignal(t, unregistered, "network cancellation did not close the socket and stop readLoop")
	if connection.ctx.Err() == nil || gateway.businessCtx.Err() != nil {
		t.Fatal("network shutdown must not cancel unfinished business callbacks")
	}
	select {
	case err := <-done:
		t.Fatalf("shutdown returned before inbound drained: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not finish after callbacks completed")
	}
	for _, want := range []string{"first", "second"} {
		select {
		case got := <-completed:
			if got != want {
				t.Fatalf("callback order=%s want=%s", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("accepted callback %s was not executed", want)
		}
		if err := <-handlerErrors; err != nil {
			t.Fatalf("callback was canceled before completion: %v", err)
		}
	}
	if gateway.businessCtx.Err() == nil {
		t.Fatal("successful shutdown must release the business context")
	}
}

func TestGatewayShutdownWaitsForAlreadyUnregisteredDispatcher(t *testing.T) {
	gateway := newLifecycleGateway(t)
	connection := newLifecycleConnection(t, gateway)
	connection.inbound <- Message{Op: "msg"}
	started := make(chan struct{})
	release := make(chan struct{})
	unregistered := registerLifecycleConnection(t, gateway, connection, func(ctx context.Context, _ *Connection, _ string, _ []byte) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	waitLifecycleSignal(t, started, "callback did not start")
	connection.Close()
	waitLifecycleSignal(t, unregistered, "connection did not unregister")
	gateway.mu.RLock()
	online := len(gateway.connections)
	gateway.mu.RUnlock()
	if online != 0 {
		t.Fatalf("online connections=%d want=0", online)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- gateway.Shutdown(shutdownCtx) }()
	waitLifecycleSignal(t, gateway.NetworkContext().Done(), "shutdown did not cancel the global network context")
	select {
	case err := <-done:
		t.Fatalf("unregister must not complete the dispatch WaitGroup: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not observe the completed dispatcher")
	}
}

func TestGatewayShutdownTimeoutCancelsBusiness(t *testing.T) {
	gateway := newLifecycleGateway(t)
	connection := newLifecycleConnection(t, gateway)
	connection.inbound <- Message{Op: "msg"}
	started := make(chan struct{})
	finished := make(chan struct{})
	unregistered := registerLifecycleConnection(t, gateway, connection, func(ctx context.Context, _ *Connection, _ string, _ []byte) {
		close(started)
		<-ctx.Done()
		close(finished)
	})
	waitLifecycleSignal(t, started, "callback did not start")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := gateway.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error=%v want deadline exceeded", err)
	}
	waitLifecycleSignal(t, finished, "shutdown timeout did not cancel the business callback")
	waitLifecycleSignal(t, unregistered, "shutdown timeout did not stop the network reader")
}

func TestConnectionCloseDoesNotCancelOtherConnectionsOrBusiness(t *testing.T) {
	gateway := newLifecycleGateway(t)
	first := newLifecycleConnection(t, gateway)
	second := newLifecycleConnection(t, gateway)
	firstClosed := registerLifecycleConnection(t, gateway, first, nil)
	secondClosed := registerLifecycleConnection(t, gateway, second, nil)
	first.Close()
	waitLifecycleSignal(t, firstClosed, "first connection did not close")
	if gateway.NetworkContext().Err() != nil || second.ctx.Err() != nil || gateway.businessCtx.Err() != nil {
		t.Fatal("closing one connection must not cancel its parent contexts or another connection")
	}
	second.Close()
	waitLifecycleSignal(t, secondClosed, "second connection did not close")
}

func TestGatewayRejectsRegistrationAfterGlobalShutdown(t *testing.T) {
	gateway := newLifecycleGateway(t)
	if err := gateway.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	identity, err := NewConnectionIdentity("user-1", "device-1", string(PlatfromWeb), "auth-session-1")
	if err != nil {
		t.Fatal(err)
	}
	connection := NewConnection(gateway.NetworkContext(), nil, identity, 8, nil, MessageBatchConfig{})
	if err := gateway.RegisterAndStart(connection, 60, 50, 1, nil, gateway.Unregister); !errors.Is(err, ErrGatewayClosed) {
		t.Fatalf("registration error=%v want gateway closed", err)
	}
	if connection.ctx.Err() == nil {
		t.Fatal("rejected connection must be canceled")
	}
}

func TestConnectionWriteBatchLoopReturnsFailureToCaller(t *testing.T) {
	gateway := newLifecycleGateway(t)
	connection := newLifecycleConnection(t, gateway)
	defer connection.Close()
	done := make(chan error, 1)
	go func() { done <- connection.writeBatchLoop(0, 1) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "ping period") {
			t.Fatalf("unexpected first sender result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writeBatchLoop did not return the sender failure")
	}
	if connection.ctx.Err() != nil {
		t.Fatal("writeBatchLoop must leave connection close to its caller")
	}
}

func TestConnectionSenderFailureClosesNetworkAndDrainsInbound(t *testing.T) {
	gateway := newLifecycleGateway(t)
	connection := newLifecycleConnection(t, gateway)
	connection.inbound <- Message{Op: "msg"}
	started := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan error, 1)
	unregistered := make(chan struct{})
	if err := gateway.RegisterAndStart(connection, 60, 0, 1, func(ctx context.Context, _ *Connection, _ string, _ []byte) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		completed <- ctx.Err()
	}, func(c *Connection) {
		gateway.Unregister(c)
		close(unregistered)
	}); err != nil {
		t.Fatal(err)
	}
	waitLifecycleSignal(t, started, "accepted business callback did not start")
	waitLifecycleSignal(t, unregistered, "sender failure did not close the network through writeLoop")
	if connection.ctx.Err() == nil || gateway.businessCtx.Err() != nil {
		t.Fatal("sender failure must cancel the network without canceling business")
	}
	close(release)
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("accepted callback was canceled: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("accepted callback did not finish after sender failure")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := gateway.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
}
