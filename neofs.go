package main

import (
	"context"
	"encoding/binary"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-sdk-go/owner"

	objectapiv2 "github.com/nspcc-dev/neofs-api-go/v2/object"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"

	"go.uber.org/zap"

	"github.com/nspcc-dev/neofs-http-gw/uploader"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/token"
)

type objectCreator struct {
	logger *zap.Logger

	clientPool pool.Pool

	ctx context.Context

	defaultOwner *owner.ID

	bearerTokenSet bool
	bearerToken    token.BearerToken

	obj object.RawObject

	attrs []*object.Attribute

	payload io.Reader
}

func (x *objectCreator) Close(dstFail *bool, dstID *string) {
	if x.bearerTokenSet {
		x.obj.SetOwnerID(x.bearerToken.Issuer())
	} else {
		x.obj.SetOwnerID(x.defaultOwner)
	}

	x.obj.SetAttributes(x.attrs...)

	var prm client.PutObjectParams

	prm.WithObject(x.obj.Object())
	prm.WithPayloadReader(x.payload)

	var opts []pool.CallOption

	if x.bearerTokenSet {
		opts = []pool.CallOption{pool.WithBearer(&x.bearerToken)}
	}

	id, err := x.clientPool.PutObject(x.ctx, &prm, opts...)
	if err != nil {
		*dstFail = true

		x.logger.Error("put object via NeoFS API protocol",
			zap.Error(err),
		)

		return
	}

	*dstID = id.String()
}

func (x *objectCreator) UseBearerToken(dstFail *bool, jBearer []byte) {
	err := x.bearerToken.UnmarshalJSON(jBearer)
	if err != nil {
		*dstFail = true

		x.logger.Error("decode bearer token JSON",
			zap.Error(err),
		)

		return
	}

	x.bearerTokenSet = true
}

func (x *objectCreator) IntoContainer(dstFail *bool, sCnr string) {
	var cnr cid.ID

	err := cnr.Parse(sCnr)
	if err != nil {
		*dstFail = true

		x.logger.Error("parse container ID",
			zap.String("encoded", sCnr),
			zap.Error(err),
		)

		return
	}

	x.obj.SetContainerID(&cnr)
}

func (x *objectCreator) addAttribute(key, val string) {
	var attr object.Attribute

	attr.SetKey(key)
	attr.SetValue(val)

	x.attrs = append(x.attrs, &attr)
}

func (x *objectCreator) WriteHeader(meta *uploader.HeadersMeta, hdr uploader.Header) {
	if hdr.SystemPrefixLen > 0 {
		hdr.Key = objectapiv2.SysAttributePrefix + hdr.Key[hdr.SystemPrefixLen:]
		hdr.Key = strings.ReplaceAll(hdr.Key, "-", "_")
		hdr.Key = strings.ToUpper(hdr.Key)
	}

	switch hdr.Key {
	case objectapiv2.SysAttributeExpEpoch:
		meta.MetExpiration()
	case object.AttributeFileName:
		meta.MetFilename()
	case object.AttributeTimestamp:
		meta.MetTimestamp()
	}

	x.addAttribute(hdr.Key, hdr.Value)
}

func (x *objectCreator) FromFile(n string) {
	x.addAttribute(object.AttributeFileName, n)
}

func (x *objectCreator) CreatedAt(t time.Time) {
	x.addAttribute(object.AttributeTimestamp, strconv.FormatInt(t.Unix(), 10))
}

func (x *objectCreator) ExpireAfter(dstFail *bool, expDur time.Duration) {
	conn, _, err := x.clientPool.Connection()
	if err != nil {
		*dstFail = true

		x.logger.Error("get client connection from pool",
			zap.Error(err),
		)

		return
	}

	var prmNetInfo client.NetworkInfoPrm

	resNetInfo, err := conn.NetworkInfo(x.ctx, prmNetInfo)
	if err != nil {
		*dstFail = true

		x.logger.Error("read network info through NeoFS API",
			zap.Error(err),
		)

		return
	}

	netInfo := resNetInfo.Info()

	var durEpoch struct {
		found bool
		val   uint64
	}

	netInfo.NetworkConfig().IterateParameters(func(parameter *netmap.NetworkParameter) bool {
		durEpoch.found = string(parameter.Key()) == "EpochDuration"
		if durEpoch.found {
			data := make([]byte, 8)

			copy(data, parameter.Value())

			durEpoch.val = binary.LittleEndian.Uint64(data)
		}

		return durEpoch.found
	})

	x.addAttribute(
		objectapiv2.SysAttributeExpEpoch,
		strconv.FormatUint(
			netInfo.CurrentEpoch()+uint64(expDur.Milliseconds())*durEpoch.val/uint64(netInfo.MsPerBlock()),
			10,
		),
	)
}

func (x *objectCreator) ReadPayloadFrom(r io.Reader) {
	x.payload = r
}

// implements uploader.NeoFS using pool.Pool.
type neofs struct {
	logger *zap.Logger

	clientPool pool.Pool

	defaultOwner *owner.ID
}

func (x *neofs) InitObjectCreation(ctx context.Context) uploader.ObjectCreator {
	return &objectCreator{
		logger:       x.logger,
		clientPool:   x.clientPool,
		ctx:          ctx,
		defaultOwner: x.defaultOwner,
	}
}
