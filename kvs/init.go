package kvs

import (
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	logger = zap.L().Named("dat.kvs")
}
