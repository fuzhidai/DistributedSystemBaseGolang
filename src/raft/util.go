package raft

import "log"

// Debugging
const Debug = 0
const CDebug = 0
const BDebug = 0
const ADebug = 1
const NDebug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func CPrintf(format string, a ...interface{}) (n int, err error) {
	if CDebug > 0 {
		log.Printf(format, a...)
	}
	return
}

func BPrintf(format string, a ...interface{}) (n int, err error) {
	if BDebug > 0 {
		log.Printf(format, a...)
	}
	return
}

func APrintf(format string, a ...interface{}) (n int, err error) {
	if ADebug > 0 {
		log.Printf(format, a...)
	}
	return
}

func NPrintf(format string, a ...interface{}) (n int, err error) {
	if NDebug > 0 {
		log.Printf(format, a...)
	}
	return
}
