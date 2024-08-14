package fmap

import (
	"os"
	"syscall"
)

// The flag used when creating the file
var CREATEFLAG = os.O_RDWR | os.O_CREATE | syscall.O_NOATIME | os.O_TRUNC

// The flag used when opening the file
var OPENFLAG = os.O_RDWR | os.O_CREATE | syscall.O_NOATIME
