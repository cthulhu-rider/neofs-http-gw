package downloader

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-http-gw/response"
	"github.com/nspcc-dev/neofs-http-gw/tokens"
	"github.com/nspcc-dev/neofs-http-gw/utils"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/object/address"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

type (
	detector struct {
		io.Reader
		err         error
		contentType string
		done        chan struct{}
		data        []byte
	}

	request struct {
		*fasthttp.RequestCtx
		log *zap.Logger
	}

	objectIDs []*oid.ID

	errReader struct {
		data   []byte
		err    error
		offset int
	}
)

var errObjectNotFound = errors.New("object not found")

func newReader(data []byte, err error) *errReader {
	return &errReader{data: data, err: err}
}

func (r *errReader) Read(b []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(b, r.data[r.offset:])
	r.offset += n
	if r.offset >= len(r.data) {
		return n, r.err
	}
	return n, nil
}

const contentTypeDetectSize = 512

func newDetector() *detector {
	return &detector{done: make(chan struct{}), data: make([]byte, contentTypeDetectSize)}
}

func (d *detector) Wait() {
	<-d.done
}

func (d *detector) SetReader(reader io.Reader) {
	d.Reader = reader
}

func (d *detector) Detect() {
	n, err := d.Reader.Read(d.data)
	if err != nil && err != io.EOF {
		d.err = err
		return
	}
	d.data = d.data[:n]
	d.contentType = http.DetectContentType(d.data)
	close(d.done)
}

func (d *detector) MultiReader() io.Reader {
	return io.MultiReader(newReader(d.data, d.err), d.Reader)
}

func isValidToken(s string) bool {
	for _, c := range s {
		if c <= ' ' || c > 127 {
			return false
		}
		if strings.ContainsRune("()<>@,;:\\\"/[]?={}", c) {
			return false
		}
	}
	return true
}

func isValidValue(s string) bool {
	for _, c := range s {
		// HTTP specification allows for more technically, but we don't want to escape things.
		if c < ' ' || c > 127 || c == '"' {
			return false
		}
	}
	return true
}

func (r request) receiveFile(neoFS NeoFS, objectAddress *address.Address) {
	var (
		err      error
		dis      = "inline"
		start    = time.Now()
		filename string
		obj      *object.Object
	)
	if err = tokens.StoreBearerToken(r.RequestCtx); err != nil {
		r.log.Error("could not fetch and store bearer token", zap.Error(err))
		response.Error(r.RequestCtx, "could not fetch and store bearer token", fasthttp.StatusBadRequest)
		return
	}
	readDetector := newDetector()
	options := new(client.GetObjectParams).
		WithAddress(objectAddress).
		WithPayloadReaderHandler(func(reader io.Reader) {
			readDetector.SetReader(reader)
			readDetector.Detect()
		})

	obj, err = clnt.GetObject(r.RequestCtx, options, bearerOpts(r.RequestCtx))
	if err != nil {
		r.handleNeoFSErr(err, start)
		return
	}
	if r.Request.URI().QueryArgs().GetBool("download") {
		dis = "attachment"
	}
	r.Response.SetBodyStream(readDetector.MultiReader(), int(obj.PayloadSize()))
	r.Response.Header.Set("Content-Length", strconv.FormatUint(obj.PayloadSize(), 10))
	var contentType string
	for _, attr := range obj.Attributes() {
		key := attr.Key()
		val := attr.Value()
		if !isValidToken(key) || !isValidValue(val) {
			continue
		}
		if strings.HasPrefix(key, utils.SystemAttributePrefix) {
			key = systemBackwardTranslator(key)
		}
		r.Response.Header.Set(utils.UserAttributeHeaderPrefix+key, val)
		switch key {
		case object.AttributeFileName:
			filename = val
		case object.AttributeTimestamp:
			value, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				r.log.Info("couldn't parse creation date",
					zap.String("key", key),
					zap.String("val", val),
					zap.Error(err))
				continue
			}
			r.Response.Header.Set("Last-Modified",
				time.Unix(value, 0).UTC().Format(http.TimeFormat))
		case object.AttributeContentType:
			contentType = val
		}
	}
	r.Response.Header.Set("X-Object-Id", obj.ID().String())
	r.Response.Header.Set("X-Owner-Id", obj.OwnerID().String())
	r.Response.Header.Set("X-Container-Id", obj.ContainerID().String())

	//if len(contentType) == 0 {
	//	if readDetector.err != nil {
	//		r.log.Error("could not read object", zap.Error(err))
	//		response.Error(r.RequestCtx, "could not read object", fasthttp.StatusBadRequest)
	//		return
	//	}
	//	readDetector.Wait()
	//	contentType = readDetector.contentType
	//}
	//r.SetContentType(contentType)

	//r.Response.Header.Set("Content-Disposition", dis+"; filename="+path.Base(filename))
}

// systemBackwardTranslator is used to convert headers looking like '__NEOFS__ATTR_NAME' to 'Neofs-Attr-Name'.
func systemBackwardTranslator(key string) string {
	// trim specified prefix '__NEOFS__'
	key = strings.TrimPrefix(key, utils.SystemAttributePrefix)

	var res strings.Builder
	res.WriteString("Neofs-")

	strs := strings.Split(key, "_")
	for i, s := range strs {
		s = strings.Title(strings.ToLower(s))
		res.WriteString(s)
		if i != len(strs)-1 {
			res.WriteString("-")
		}
	}

	return res.String()
}

func (r *request) handleNeoFSErr(err error, start time.Time) {
	r.log.Error(
		"could not receive object",
		zap.Stringer("elapsed", time.Since(start)),
		zap.Error(err),
	)
	var (
		msg   = fmt.Sprintf("could not receive object: %v", err)
		code  = fasthttp.StatusBadRequest
		cause = err
	)
	for unwrap := errors.Unwrap(err); unwrap != nil; unwrap = errors.Unwrap(cause) {
		cause = unwrap
	}

	if strings.Contains(cause.Error(), "not found") ||
		strings.Contains(cause.Error(), "can't fetch container info") {
		code = fasthttp.StatusNotFound
		msg = errObjectNotFound.Error()
	}

	response.Error(r.RequestCtx, msg, code)
}

func (o objectIDs) Slice() []string {
	res := make([]string, 0, len(o))
	for _, oid := range o {
		res = append(res, oid.String())
	}
	return res
}

// Downloader is a download request handler.
type Downloader struct {
	neoFS NeoFS

	logger *zap.Logger

	settings Settings
}

type Settings struct {
	ZipCompression bool
}

func failRequest(ctx *fasthttp.RequestCtx, msg string) {
	failRequestWithCode(ctx, msg, fasthttp.StatusBadRequest)
}

func failNotFound(ctx *fasthttp.RequestCtx) {
	failRequestWithCode(ctx, "object not found", fasthttp.StatusNotFound)
}

func failRequestWithCode(ctx *fasthttp.RequestCtx, msg string, code int) {
	response.Error(ctx, msg, code)
}

const (
	_ uint8 = iota
	byID
	byAttr
	byFile
)

func (x *Downloader) read(c *fasthttp.RequestCtx, selector uint8, head bool) {
	// init reader
	rObj := x.neoFS.InitObjectRead(c)

	// bind container
	sCnr, _ := c.UserValue("cid").(string)

	var fail bool

	rObj.FromContainer(&fail, sCnr)
	if fail {
		failRequest(c, "incorrect container ID")
		return
	}

	// read bearer token
	switch encodedBearer := []byte(nil); tokens.ReadBearer(&encodedBearer, &c.Request.Header) {
	case -1:
		failRequest(c, "incorrect bearer token header")
		return
	case 1:
		if rObj.UseBearerToken(&fail, encodedBearer); fail {
			failRequest(c, "incorrect bearer token JSON")
			return
		}
	}

	// select objects
	switch selector {
	case byID:
		sObj, _ := c.UserValue("oid").(string)

		rObj.ByID(&fail, sObj)
		if fail {
			failRequest(c, "incorrect object ID")
			return
		}

		c.Response.Header.Set("X-Object-Id", sObj)
	case byAttr:
		key, _ := url.QueryUnescape(c.UserValue("attr_key").(string))
		val, _ := url.QueryUnescape(c.UserValue("attr_val").(string))

		switch rObj.ByAttribute(key, val) {
		case -1:
			failRequest(c, "select by attribute failed")
			return
		case 0:
			failNotFound(c)
			return
		}
	case byFile:
		prefix, _ := url.QueryUnescape(c.UserValue("prefix").(string))

		switch rObj.ByFilenamePrefix(prefix) {
		case -1:
			failRequest(c, "select by filename prefix failed")
			return
		case 0:
			failNotFound(c)
			return
		}
	}

	// distinguish HEAD
	if head {
		rObj.Head()
	}

	// perform reading
	var res ResRead

	res.reqCtx = c

loop:
	for {
		switch rObj.ReadNext(&res); res.status {
		case 1:
			break loop
		case 2:
			failRequest(c, "read object failed")
			return
		case 3:
			failNotFound(c)
			return
		}
	}

	// set remaining headers
	c.Response.Header.Set("X-Container-Id", sCnr)
}

// DownloadByAddress handles download requests using simple cid/oid format.
func (x *Downloader) DownloadByAddress(c *fasthttp.RequestCtx) {
	x.read(c, byID, false)
}

// HeadByAddress handles head requests using simple cid/oid format.
func (x *Downloader) HeadByAddress(c *fasthttp.RequestCtx) {
	x.read(c, byID, true)
}

// DownloadByAttribute handles attribute-based download requests.
func (x *Downloader) DownloadByAttribute(c *fasthttp.RequestCtx) {
	x.read(c, byAttr, false)
}

// HeadByAttribute handles attribute-based head requests.
func (x *Downloader) HeadByAttribute(c *fasthttp.RequestCtx) {
	x.read(c, byAttr, true)
}

// DownloadZipped handles zip by prefix requests.
func (d *Downloader) DownloadZipped(c *fasthttp.RequestCtx) {
	status := fasthttp.StatusBadRequest
	scid, _ := c.UserValue("cid").(string)
	prefix, _ := url.QueryUnescape(c.UserValue("prefix").(string))
	log := d.log.With(zap.String("cid", scid), zap.String("prefix", prefix))

	containerID := cid.New()
	if err := containerID.Parse(scid); err != nil {
		log.Error("wrong container id", zap.Error(err))
		response.Error(c, "wrong container id", status)
		return
	}

	if err := tokens.StoreBearerToken(c); err != nil {
		log.Error("could not fetch and store bearer token", zap.Error(err))
		response.Error(c, "could not fetch and store bearer token", fasthttp.StatusBadRequest)
		return
	}

	ids, err := d.searchByPrefix(c, containerID, prefix)
	if err != nil {
		log.Error("couldn't find objects", zap.Error(err))
		if errors.Is(err, errObjectNotFound) {
			status = fasthttp.StatusNotFound
		}
		response.Error(c, "couldn't find objects", status)
		return
	}

	c.Response.Header.Set("Content-Type", "application/zip")
	c.Response.Header.Set("Content-Disposition", "attachment; filename=\"archive.zip\"")
	c.Response.SetStatusCode(http.StatusOK)

	if err = d.streamFiles(c, containerID, ids); err != nil {
		log.Error("couldn't stream files", zap.Error(err))
		response.Error(c, "couldn't stream", fasthttp.StatusInternalServerError)
		return
	}
}

func (d *Downloader) streamFiles(c *fasthttp.RequestCtx, cid *cid.ID, ids []*object.ID) error {
	zipWriter := zip.NewWriter(c)
	compression := zip.Store
	if d.settings.ZipCompression {
		compression = zip.Deflate
	}

	for _, id := range ids {
		var r io.Reader
		readerInitCtx, initReader := context.WithCancel(c)
		options := new(client.GetObjectParams).
			WithAddress(formAddress(cid, id)).
			WithPayloadReaderHandler(func(reader io.Reader) {
				r = reader
				initReader()
			})

		obj, err := d.pool.GetObject(c, options, bearerOpts(c))
		if err != nil {
			return err
		}

		header := &zip.FileHeader{
			Name:     getFilename(obj),
			Method:   compression,
			Modified: time.Now(),
		}
		entryWriter, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}

		<-readerInitCtx.Done()
		_, err = io.Copy(entryWriter, r)
		if err != nil {
			return err
		}

		if err = zipWriter.Flush(); err != nil {
			return err
		}
	}

	return zipWriter.Close()
}

func getFilename(obj *object.Object) string {
	for _, attr := range obj.Attributes() {
		if attr.Key() == object.AttributeFileName {
			return attr.Value()
		}
	}

	return ""
}
