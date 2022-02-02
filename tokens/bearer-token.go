package tokens

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/token"
	"github.com/valyala/fasthttp"
)

type fromHandler = func(h *fasthttp.RequestHeader) []byte

const (
	bearerTokenHdr = "Bearer"
	bearerTokenKey = "__context_bearer_token_key"
)

// BearerToken usage:
//
// if err = storeBearerToken(ctx); err != nil {
// 	log.Error("could not fetch bearer token", zap.Error(err))
// 	c.Error("could not fetch bearer token", fasthttp.StatusBadRequest)
// 	return
// }

// BearerTokenFromHeader extracts bearer token from Authorization request header.
func BearerTokenFromHeader(h *fasthttp.RequestHeader) []byte {
	auth := h.Peek(fasthttp.HeaderAuthorization)
	if auth == nil || !bytes.HasPrefix(auth, []byte(bearerTokenHdr)) {
		return nil
	}
	if auth = bytes.TrimPrefix(auth, []byte(bearerTokenHdr+" ")); len(auth) == 0 {
		return nil
	}
	return auth
}

// BearerTokenFromCookie extracts bearer token from cookies.
func BearerTokenFromCookie(h *fasthttp.RequestHeader) []byte {
	auth := h.Cookie(bearerTokenHdr)
	if len(auth) == 0 {
		return nil
	}

	return auth
}

// StoreBearerToken extracts bearer token from header or cookie and stores
// it in the request context.
func StoreBearerToken(ctx *fasthttp.RequestCtx) error {
	tkn, err := fetchBearerToken(ctx)
	if err != nil {
		return err
	}
	// This is an analog of context.WithValue.
	ctx.SetUserValue(bearerTokenKey, tkn)
	return nil
}

// LoadBearerToken returns bearer token stored in context given (if it's
// present there).
func LoadBearerToken(ctx context.Context) *token.BearerToken {
	tkn, _ := ctx.Value(bearerTokenKey).(*token.BearerToken)
	return tkn
}

func fetchBearerToken(ctx *fasthttp.RequestCtx) (*token.BearerToken, error) {
	// ignore empty value
	if ctx == nil {
		return nil, nil
	}
	var (
		lastErr error

		buf []byte
		tkn = new(token.BearerToken)
	)
	for _, parse := range []fromHandler{BearerTokenFromHeader, BearerTokenFromCookie} {
		if buf = parse(&ctx.Request.Header); buf == nil {
			continue
		} else if data, err := base64.StdEncoding.DecodeString(string(buf)); err != nil {
			lastErr = fmt.Errorf("can't base64-decode bearer token: %w", err)
			continue
		} else if err = tkn.Unmarshal(data); err != nil {
			lastErr = fmt.Errorf("can't unmarshal bearer token: %w", err)
			continue
		} else if tkn == nil {
			continue
		}

		return tkn, nil
	}

	return nil, lastErr
}

// ReadBearer reads JSON-encoded bearer token from the HTTP request header and returns:
//   -1 if token is incorrect;
//   0 if token is missing;
//   1 otherwise.
func ReadBearer(dst *[]byte, hdr *fasthttp.RequestHeader) int8 {
	var b64Bearer []byte

	const prefix = bearerTokenHdr + " "
	bPrefix := []byte(prefix)

	if hdrAuth := hdr.Peek(fasthttp.HeaderAuthorization); bytes.HasPrefix(hdrAuth, bPrefix) {
		b64Bearer = bytes.TrimPrefix(hdrAuth, bPrefix)
	} else {
		b64Bearer = hdr.Cookie(bearerTokenHdr)
	}

	if b64Bearer == nil {
		return 0
	}

	*dst = make([]byte, base64.StdEncoding.DecodedLen(len(b64Bearer)))

	_, err := base64.StdEncoding.Decode(*dst, b64Bearer)
	if err != nil {
		return -1
	}

	return 1
}
