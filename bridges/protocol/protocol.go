// Package protocol defines the binary wire protocol between the state-actor
// generator and per-client bridge programs.
//
// The generator spawns a bridge binary (e.g. geth-bridge), streams write
// commands to its stdin, and reads responses from stdout. Stderr is reserved
// for logging.
//
// Wire format:
//
//	Request:  [1B cmd][4B LE payload len][payload...]
//	Response: [1B status][4B LE payload len][payload...]
//
// Write commands (PutAccount, PutStorage, PutCode) are fire-and-forget:
// no response is sent. Errors are buffered and returned on the next sync
// command (Flush, ComputeRoot, WriteGenesis, Close).
package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Command types.
const (
	CmdPutAccount  byte = 0x01
	CmdPutStorage  byte = 0x02
	CmdPutCode     byte = 0x03
	CmdFlush       byte = 0x04
	CmdComputeRoot byte = 0x05
	CmdWriteGenesis byte = 0x06
	CmdClose       byte = 0x07
)

// Response status codes.
const (
	StatusOK    byte = 0x00
	StatusError byte = 0xFF
)

// Fixed field sizes.
const (
	AddressLen = 20
	HashLen    = 32
	Uint64Len  = 8
)

// PutAccount payload layout:
//
//	[20B address][8B nonce LE][32B balance BE uint256][32B codeHash]
const PutAccountLen = AddressLen + Uint64Len + HashLen + HashLen // 92

// PutStorage payload layout:
//
//	[20B address][32B slot][32B value]
const PutStorageLen = AddressLen + HashLen + HashLen // 84

// PutCode payload layout:
//
//	[32B codeHash][variable code bytes]
// Total length = 32 + len(code)

// WriteGenesis payload: JSON-encoded genesis config.

// --- Writing helpers (used by the generator / sender side) ---

// WriteMsg writes a command message to w.
func WriteMsg(w io.Writer, cmd byte, payload []byte) error {
	var hdr [5]byte
	hdr[0] = cmd
	binary.LittleEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// ReadResponse reads a response (status + payload) from r.
func ReadResponse(r io.Reader) (status byte, payload []byte, err error) {
	var hdr [5]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, fmt.Errorf("read response header: %w", err)
	}
	status = hdr[0]
	plen := binary.LittleEndian.Uint32(hdr[1:])
	if plen > 0 {
		payload = make([]byte, plen)
		if _, err = io.ReadFull(r, payload); err != nil {
			return 0, nil, fmt.Errorf("read response payload: %w", err)
		}
	}
	return status, payload, nil
}

// --- Reading helpers (used by the bridge / receiver side) ---

// ReadMsg reads one command message from r.
func ReadMsg(r io.Reader) (cmd byte, payload []byte, err error) {
	var hdr [5]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err // EOF is expected on clean shutdown
	}
	cmd = hdr[0]
	plen := binary.LittleEndian.Uint32(hdr[1:])
	if plen > 0 {
		payload = make([]byte, plen)
		if _, err = io.ReadFull(r, payload); err != nil {
			return 0, nil, fmt.Errorf("read payload (%d bytes): %w", plen, err)
		}
	}
	return cmd, payload, nil
}

// WriteResponse writes a response message to w.
func WriteResponse(w io.Writer, status byte, payload []byte) error {
	var hdr [5]byte
	hdr[0] = status
	binary.LittleEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// WriteOK writes a success response with optional payload.
func WriteOK(w io.Writer, payload []byte) error {
	return WriteResponse(w, StatusOK, payload)
}

// WriteError writes an error response with a message.
func WriteError(w io.Writer, msg string) error {
	return WriteResponse(w, StatusError, []byte(msg))
}

// --- Payload encoding helpers ---

// EncodePutAccount encodes a PutAccount payload.
func EncodePutAccount(addr [AddressLen]byte, nonce uint64, balance [HashLen]byte, codeHash [HashLen]byte) []byte {
	var buf [PutAccountLen]byte
	copy(buf[0:20], addr[:])
	binary.LittleEndian.PutUint64(buf[20:28], nonce)
	copy(buf[28:60], balance[:])
	copy(buf[60:92], codeHash[:])
	return buf[:]
}

// DecodePutAccount decodes a PutAccount payload.
func DecodePutAccount(p []byte) (addr [AddressLen]byte, nonce uint64, balance [HashLen]byte, codeHash [HashLen]byte, err error) {
	if len(p) != PutAccountLen {
		return addr, 0, balance, codeHash, fmt.Errorf("PutAccount: expected %d bytes, got %d", PutAccountLen, len(p))
	}
	copy(addr[:], p[0:20])
	nonce = binary.LittleEndian.Uint64(p[20:28])
	copy(balance[:], p[28:60])
	copy(codeHash[:], p[60:92])
	return
}

// EncodePutStorage encodes a PutStorage payload.
func EncodePutStorage(addr [AddressLen]byte, slot [HashLen]byte, value [HashLen]byte) []byte {
	var buf [PutStorageLen]byte
	copy(buf[0:20], addr[:])
	copy(buf[20:52], slot[:])
	copy(buf[52:84], value[:])
	return buf[:]
}

// DecodePutStorage decodes a PutStorage payload.
func DecodePutStorage(p []byte) (addr [AddressLen]byte, slot [HashLen]byte, value [HashLen]byte, err error) {
	if len(p) != PutStorageLen {
		return addr, slot, value, fmt.Errorf("PutStorage: expected %d bytes, got %d", PutStorageLen, len(p))
	}
	copy(addr[:], p[0:20])
	copy(slot[:], p[20:52])
	copy(value[:], p[52:84])
	return
}

// DecodePutCode decodes a PutCode payload into codeHash + code.
func DecodePutCode(p []byte) (codeHash [HashLen]byte, code []byte, err error) {
	if len(p) < HashLen {
		return codeHash, nil, fmt.Errorf("PutCode: payload too short (%d bytes)", len(p))
	}
	copy(codeHash[:], p[:32])
	code = p[32:]
	return
}
