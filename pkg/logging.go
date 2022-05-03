package pkg

import (
	"io/ioutil"
	"log"
	"os"
)

// Debug is optional debug logger
var Debug *log.Logger

// Out logs to stdout
var Out *log.Logger

// Error logs to stderr
var Error *log.Logger

// Trace is used for xtrace
var Trace *log.Logger

// Initialize the loggers
func init() {
	Debug = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	Trace = log.New(ioutil.Discard, "", log.Lshortfile)
	Out = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	Error = log.New(os.Stderr, "ERROR: ", log.Ltime|log.Lshortfile)
}

// SetDebug turns debug on or off
func SetDebug(enabled bool) {
	if enabled {
		Debug.SetOutput(os.Stdout)
	} else {
		Debug.SetOutput(ioutil.Discard)
	}
}