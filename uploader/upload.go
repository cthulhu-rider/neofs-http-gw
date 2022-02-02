package uploader

import (
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"strconv"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-http-gw/response"
	"github.com/nspcc-dev/neofs-http-gw/tokens"
	"github.com/nspcc-dev/neofs-http-gw/utils"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

// PrmInit groups initialization parameters of the Uploader.
type PrmInit struct {
	neoFS NeoFS

	defaultTimestamp bool

	logger *zap.Logger
}

// SetNeoFS sets NeoFS component used to store the objects.
// Required parameter.
func (x *PrmInit) SetNeoFS(neoFS NeoFS) {
	x.neoFS = neoFS
}

// SetLogger sets component to write log messages.
// By default, log messages are not written.
func (x *PrmInit) SetLogger(logger *zap.Logger) {
	x.logger = logger
}

// EnableDefaultTimestamping sets flag which makes Uploader to
// set object timestamps in case of their lack.
func (x *PrmInit) EnableDefaultTimestamping() {
	x.defaultTimestamp = true
}

// Uploader is an upload request handler.
type Uploader struct {
	neoFS NeoFS

	defaultTimestamp bool

	logger *zap.Logger
}

// Init initializes Uploader.
//
// Panics if NeoFS instance is not set or nil.
//
// If logger is not specified, no-op logger is used.
func (x *Uploader) Init(prm PrmInit) {
	if prm.neoFS == nil {
		panic("init uploader: NeoFS component is unset/nil")
	}

	x.neoFS = prm.neoFS
	x.defaultTimestamp = prm.defaultTimestamp

	if prm.logger != nil {
		x.logger = prm.logger
	} else {
		x.logger = zap.NewNop()
	}
}

type uploadContext struct {
	// processing request, embedded to use uploadContext as context.Context
	*fasthttp.RequestCtx
	// global failure flag
	fail bool
	// failure reason
	err error
	// response data
	resp struct {
		ObjectID    string `json:"object_id"`
		ContainerID string `json:"container_id"`
	}
	// logger for request context
	logger *zap.Logger
	// payload source
	srcPayload interface {
		io.ReadCloser
		FileName() string
	}
	// component to form and store object
	creator ObjectCreator
	// flag to write creation time of the object if header is missing
	defaultTimestamp bool
}

func failRequest(ctx *uploadContext, msg string) {
	response.Error(ctx.RequestCtx, msg, fasthttp.StatusBadRequest)

	if ctx.err != nil {
		ctx.logger.Error(msg,
			zap.String("container", ctx.resp.ContainerID),
			zap.Error(ctx.err),
		)
	}
}

func failExpiration(ctx *uploadContext) {
	failRequest(ctx, "problem with expiration header, try expiration in epoch")
}

func initPayloadSource(ctx *uploadContext) {
	reader := multipart.NewReader(
		ctx.RequestBodyStream(),
		string(ctx.Request.Header.MultipartFormBoundary()),
	)

	var part *multipart.Part

	for {
		part, ctx.err = reader.NextPart()
		if ctx.fail = ctx.err != nil; ctx.fail {
			return
		}

		if part.FormName() == "" || part.FileName() == "" {
			continue
		}

		ctx.srcPayload = part
	}
}

// Upload handles multipart upload request.
func (x *Uploader) Upload(c *fasthttp.RequestCtx) {
	// init upload context
	var ctx uploadContext

	ctx.logger = x.logger
	ctx.RequestCtx = c
	ctx.creator = x.neoFS.InitObjectCreation(ctx)
	ctx.defaultTimestamp = x.defaultTimestamp

	// bind container
	ctx.resp.ContainerID, _ = c.UserValue("cid").(string) // check emptiness?

	ctx.creator.IntoContainer(&ctx.fail, ctx.resp.ContainerID)
	if ctx.fail {
		failRequest(&ctx, "incorrect container ID")
		return
	}

	// read bearer token
	switch encodedBearer := []byte(nil); tokens.ReadBearer(&encodedBearer, &c.Request.Header) {
	case -1:
		failRequest(&ctx, "incorrect bearer token header")
		return
	case 1:
		if ctx.creator.UseBearerToken(&ctx.fail, encodedBearer); ctx.fail {
			failRequest(&ctx, "incorrect bearer token JSON")
			return
		}
	}

	// compose multi-part file
	initPayloadSource(&ctx)
	if ctx.fail {
		failRequest(&ctx, "receive multipart/form")
		return
	}

	defer ctx.srcPayload.Close()

	// provide payload
	ctx.creator.ReadPayloadFrom(ctx.srcPayload)

	// write headers
	processHeaders(&ctx)

	// finish creation and save the object in NeoFS
	ctx.creator.Close(&ctx.fail, &ctx.resp.ObjectID)
	if ctx.fail {
		failRequest(&ctx, "store file in NeoFS failed")
		return
	}

	// form the response
	enc := json.NewEncoder(ctx)
	enc.SetIndent("", "\t")

	ctx.err = enc.Encode(ctx.resp)
	if ctx.fail = ctx.err != nil; ctx.fail {
		failRequest(&ctx, "form response failed")
		return
	}

	// Multipart is multipart and thus can contain more than one part which
	// we ignore at the moment. Also, when dealing with chunked encoding
	// the last zero-length chunk might be left unread (because multipart
	// reader only cares about its boundary and doesn't look further) and
	// it will be (erroneously) interpreted as the start of the next
	// pipelined header. Thus we need to drain the body buffer.
	_, _ = io.CopyBuffer(io.Discard, ctx.srcPayload, make([]byte, 4096))

	// Report status code and content type.
	c.Response.SetStatusCode(fasthttp.StatusOK)
	c.Response.Header.SetContentType("application/json; charset=UTF-8")
}

func calcSystemPrefixLen(hdr *Header) {
	switch hdr.Key {
	default:
		hdr.SystemPrefixLen = 0
	case
		"Neofs-",
		"NEOFS-",
		"neofs-":
		hdr.SystemPrefixLen = 6
	}
}

func processHeaders(ctx *uploadContext) {
	type expirationType uint8

	const (
		_ expirationType = iota
		expirationRFC3339
		expirationTimestamp
		expirationDuration
	)

	var (
		hdr  Header
		meta HeadersMeta

		exp struct {
			typ, typCur expirationType

			dur time.Duration
		}
	)

	// iterate over all headers
	ctx.Request.Header.VisitAll(func(k, v []byte) {
		if ctx.fail {
			// no other way to abort iteration
			return
		}

		// skip empty values
		if len(v) == 0 {
			return
		}

		// cut user attribute prefix
		hdr.Key = strings.TrimPrefix(string(k), utils.UserAttributeHeaderPrefix)
		if len(hdr.Key) == len(k) {
			// no prefix
			return
		}

		// detect expiration headers
		switch exp.typCur = 0; hdr.Key {
		case utils.ExpirationRFC3339Attr:
			exp.typCur = expirationRFC3339
		case utils.ExpirationTimestampAttr:
			exp.typCur = expirationTimestamp
		case utils.ExpirationDurationAttr:
			exp.typCur = expirationDuration
		}

		if exp.typCur > 0 {
			// 1. check if expiration epoch header has not been already encountered
			// because it overlaps any other expiration header
			// 2. check if more prioritized header has not been already processed
			if !meta.encountered.expiration && exp.typCur >= exp.typ {
				switch exp.typ = exp.typCur; exp.typ {
				case expirationRFC3339:
					var timeExpiration time.Time

					timeExpiration, ctx.err = time.Parse(time.RFC3339, hdr.Value)
					if ctx.fail = ctx.err != nil; !ctx.fail {
						exp.dur = timeExpiration.Sub(time.Now().UTC())
						// value will be checked after the switch statement
					}
				case expirationTimestamp:
					var timestamp int64

					timestamp, ctx.err = strconv.ParseInt(hdr.Value, 10, 64)
					if ctx.fail = ctx.err != nil; !ctx.fail {
						exp.dur = time.Unix(timestamp, 0).Sub(time.Now())
						// value will be checked after the switch statement
					}
				case expirationDuration:
					exp.dur, ctx.err = time.ParseDuration(hdr.Value)

					ctx.fail = ctx.err != nil
				}

				// check if expiration time is from the future
				if !ctx.fail && exp.dur <= 0 {
					ctx.fail = true
					ctx.err = errors.New("expiration time not from the future")
				}

				// catch failure
				if ctx.fail {
					failExpiration(ctx)

					ctx.logger.Error("incorrect expiration header",
						zap.String("header", hdr.Key),
						zap.String("value", hdr.Value),
						zap.Error(ctx.err),
					)
				}

				// we shouldn't write expiration time straightaway because we can
				// encounter more prioritized header on next iterations
			}

			// we don't write expiration headers to the created objects directly,
			// we'll do it after all headers are processed
			return
		}

		// calculate length of the header's system prefix
		calcSystemPrefixLen(&hdr)

		hdr.Value = string(v)

		ctx.creator.WriteHeader(&meta, hdr)
	})

	if !ctx.fail {
		// write remaining headers
		if !meta.encountered.expiration {
			ctx.creator.ExpireAfter(&ctx.fail, exp.dur)
			if ctx.fail {
				failExpiration(ctx)
				return
			}
		}

		if !meta.encountered.timestamp && ctx.defaultTimestamp {
			ctx.creator.CreatedAt(time.Now())
		}

		if !meta.encountered.filename {
			ctx.creator.FromFile(ctx.srcPayload.FileName())
		}
	}
}
