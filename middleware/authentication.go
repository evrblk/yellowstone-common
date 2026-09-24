// TODO: move the middleware to evrblk-go?
package middleware

import (
	"context"
	"errors"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/evrblk/evrblk-go/authn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	errUnauthenticated       = status.Error(codes.Unauthenticated, "unauthenticated")
	errUnsupportedApiKeyType = errors.New("unsupported api key type")
	errVTProtoError          = errors.New("request does not implement VTProtoMessage")
)

const (
	signatureKey = "evrblk-signature"
	apiKeyKey    = "evrblk-api-key-id"
	timestampKey = "evrblk-timestamp"
)

type apiKey struct {
	id   string
	body string
}

// AuthenticationMiddleware is a gRPC unary interceptor that verifies evrblk
// API key request signatures. serviceName is the domain string baked into
// the signature (e.g. "Moab", "Grackle") — it must match what clients sign
// with, so each service passes its own name to NewAuthenticationMiddleware.
type AuthenticationMiddleware struct {
	serviceName  string
	authKeysPath string

	mu   sync.Mutex
	keys map[string]*apiKey
}

func NewAuthenticationMiddleware(authKeysPath string, serviceName string) *AuthenticationMiddleware {
	return &AuthenticationMiddleware{
		serviceName:  serviceName,
		authKeysPath: authKeysPath,
		keys:         make(map[string]*apiKey),
	}
}

func (m *AuthenticationMiddleware) Unary(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errUnauthenticated
	}

	if len(md.Get(signatureKey)) != 1 {
		return nil, errUnauthenticated
	}
	signature := md.Get(signatureKey)[0]

	if len(md.Get(apiKeyKey)) != 1 {
		return nil, errUnauthenticated
	}
	apiKeyIdStr := md.Get(apiKeyKey)[0]

	if len(md.Get(timestampKey)) != 1 {
		return nil, errUnauthenticated
	}
	timestamp, err := strconv.Atoi(md.Get(timestampKey)[0])
	if err != nil {
		return nil, errUnauthenticated
	}

	key, err := m.getApiKey(apiKeyIdStr)
	if err != nil {
		return nil, errUnauthenticated
	}

	method := path.Base(info.FullMethod)

	if err := m.verifySignature(req, key, signature, int64(timestamp), method); err != nil {
		return nil, errUnauthenticated
	}

	return handler(ctx, req)
}

func (m *AuthenticationMiddleware) verifySignature(req any, key *apiKey, signature string, timestamp int64, method string) error {
	requestProto, ok := req.(authn.VTProtoMessage)
	if !ok {
		return errVTProtoError
	}

	now := time.Now()

	if strings.HasPrefix(key.id, "key_alfa_") {
		return authn.VerifyAlfaSignature(signature, timestamp, now, key.body, requestProto, m.serviceName, method)
	} else if strings.HasPrefix(key.id, "key_bravo_") {
		date := authn.GetDateOfTimestamp(timestamp)
		hashedSecret, err := authn.HashBravoSecretWithDate(key.body, date)
		if err != nil {
			return err
		}

		return authn.VerifyBravoSignature(signature, timestamp, now, hashedSecret, requestProto, m.serviceName, method)
	}
	return errUnsupportedApiKeyType
}

func (m *AuthenticationMiddleware) getApiKey(apiKeyId string) (*apiKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key, ok := m.keys[apiKeyId]
	if ok {
		return key, nil
	}

	body, err := os.ReadFile(path.Join(m.authKeysPath, apiKeyId))
	if err != nil {
		return key, err
	}

	key = &apiKey{
		id:   apiKeyId,
		body: string(body),
	}

	m.keys[apiKeyId] = key

	return key, nil
}
