package downloader

import (
	"context"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-http-gw/utils"

	"github.com/valyala/fasthttp"
)

type ResRead struct {
	head bool

	selector uint8

	status uint8

	reqCtx *fasthttp.RequestCtx
}

// Header describes HTTP header containing NeoFS-specific information.
type Header struct {
	// Is system header.
	System bool

	// Non-empty header key with in upper case.
	Key string

	// Non-empty header value.
	Value string
}

func (x *ResRead) EOF() {
	x.status = 1
}

func (x *ResRead) FailCommonAPI() {
	x.status = 2
}

func (x *ResRead) NotFound() {
	x.status = 3
}

func (x *ResRead) WaitingHeaders(dst *bool) {
	*dst = x.selector != byFile
}

// +head, +get, -zip
func (x *ResRead) WriteHeader(hdr Header) {
	if hdr.System {
		ss := strings.Split(hdr.Key, "-")
		for i := range ss {
			ss[i] = strings.Title(strings.ToLower(ss[i]))
		}

		hdr.Key = "Neofs-" + strings.Join(ss, "-")
	}

	x.reqCtx.Response.Header.Set(
		utils.UserAttributeHeaderPrefix+hdr.Key,
		hdr.Value,
	)
}

func (x *ResRead) WaitingCreationTime(dst *bool) {
	*dst
}

func (x *ResRead) WriteCreatedAt(t time.Time) {
	x.reqCtx.Response.Header.Set(fasthttp.HeaderLastModified, t.UTC().Format(http.TimeFormat))
}

func (x *ResRead) WaitingPayload(dst *bool) {
	*dst = !x.head
}

// +head, +get, -zip
func (x *ResRead) WriteContentType(ct string) {
	x.reqCtx.Response.Header.SetContentType(ct)
}

// -head, +get, +zip
func (x *ResRead) HandlePayload(sz uint64, r io.Reader) {
	x.reqCtx.Response.SetBodyStream(r, int(sz)) // -zip
	x.reqCtx.Response.Header.Set(fasthttp.HeaderContentLength, strconv.FormatUint(sz, 10))
}

func (x *ResRead) WaitingFilename(dst *bool) {
	*dst = !x.head
}

// -head, +get, +zip
func (x *ResRead) WriteFilename(n string) {
	if x.reqCtx.Request.URI().QueryArgs().GetBool("download") {
		x.reqCtx.Response.Header.Set(
			fasthttp.HeaderContentDisposition,
			"attachment; filename="+path.Base(n),
		)
	} else {
		x.reqCtx.Response.Header.Set(
			fasthttp.HeaderContentDisposition,
			"inline; filename="+path.Base(n),
		)
	}
}

func (x *ResRead) WaitingOwner(dst *bool) {
	*dst = x.selector != byFile
}

// +head, +get, -zip
func (x *ResRead) WriteOwner(id string) {
	x.reqCtx.Response.Header.Set("X-Owner-Id", id)
}

// ObjectReader
type ObjectReader interface {
	ReadNext(*ResRead)

	// UseBearerToken provides JSON-encoded bearer token which
	// should be used for the object reading.
	//
	// Writes true if token format is incorrect.
	UseBearerToken(*bool, []byte)

	// FromContainer specifies the string ID of the container
	// from which the object should be read.
	//
	// Writes true if ID format is incorrect.
	FromContainer(*bool, string)

	// ByID specifies the string ID as a selector of the object to be read.
	//
	// Writes true if ID format is incorrect.
	ByID(*bool, string)

	// ByAttribute specifies k:v user attribute as a selector of the object
	// to be read. Returns:
	//   -1 if selection failed or multiple object satisfy;
	//   0 if object not found;
	//   1 otherwise.
	ByAttribute(key, value string) int8

	// ByFilenamePrefix specifies prefix of the filename associated with
	// the objects as a selector.Returns:
	//	//   -1 if selection failed or multiple object satisfy;
	//	//   0 if object not found;
	//	//   1 otherwise.
	ByFilenamePrefix(string) int8
}

// NeoFS is an interface of the NeoFS network.
type NeoFS interface {
	// InitObjectRead initiates reading of the object from NeoFS with provided context.
	// Explicit receive is prepared using the ObjectReadPreparer.
	InitObjectRead(context.Context) ObjectReader
}
