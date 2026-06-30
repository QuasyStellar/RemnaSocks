package orchestrator

import (
	"fmt"
	"os"
)

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

var currentLogLevel = LogLevelInfo

func InitLogger() {
	lvl := os.Getenv("LOG_LEVEL")
	switch lvl {
	case "DEBUG":
		currentLogLevel = LogLevelDebug
	case "INFO":
		currentLogLevel = LogLevelInfo
	case "WARN":
		currentLogLevel = LogLevelWarn
	case "ERROR":
		currentLogLevel = LogLevelError
	default:
		currentLogLevel = LogLevelInfo
	}
}

func logDebug(format string, v ...interface{}) {
	if currentLogLevel <= LogLevelDebug {
		fmt.Printf("[DEBUG] "+format+"\n", v...)
	}
}

func logInfo(format string, v ...interface{}) {
	if currentLogLevel <= LogLevelInfo {
		fmt.Printf("[INFO] "+format+"\n", v...)
	}
}

func logWarn(format string, v ...interface{}) {
	if currentLogLevel <= LogLevelWarn {
		fmt.Printf("[WARN] "+format+"\n", v...)
	}
}

func logError(format string, v ...interface{}) {
	if currentLogLevel <= LogLevelError {
		fmt.Printf("[ERROR] "+format+"\n", v...)
	}
}
