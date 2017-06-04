package factory

import "github.com/duythinht/zaptor"

var logger = zaptor.GetLogger("factory").WithLevelBy("FACTORY_LOG_LEVEL")
