package generator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/nerolation/state-actor/bridges/protocol"
)

// BridgeWriter implements StateWriter by spawning an external bridge process
// and communicating via the binary protocol over stdin/stdout.
//
// The bridge process (e.g. geth-bridge, erigon-bridge) uses the client's own
// libraries to write state, making this approach forward-compatible with
// client DB layout changes.
type BridgeWriter struct {
	cmd    *exec.Cmd
	stdin  *bufio.Writer
	stdout *bufio.Reader

	accountBytes atomic.Uint64
	storageBytes atomic.Uint64
	codeBytes    atomic.Uint64
}

// NewBridgeWriter spawns a bridge process and returns a writer.
// bridgeBin is the path to the bridge binary (e.g. "./geth-bridge").
// args are passed to the bridge (e.g. "-db", "/path/to/chaindata").
func NewBridgeWriter(bridgeBin string, args ...string) (*BridgeWriter, error) {
	cmd := exec.Command(bridgeBin, args...)
	cmd.Stderr = os.Stderr // bridge logs go to our stderr

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("create stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("create stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start bridge %q: %w", bridgeBin, err)
	}

	return &BridgeWriter{
		cmd:    cmd,
		stdin:  bufio.NewWriterSize(stdinPipe, 4*1024*1024),
		stdout: bufio.NewReaderSize(stdoutPipe, 4*1024*1024),
	}, nil
}

// WriteAccount sends a PutAccount command to the bridge (fire-and-forget).
func (w *BridgeWriter) WriteAccount(addr common.Address, acc *types.StateAccount, _ uint64) error {
	var balance [32]byte
	acc.Balance.WriteToSlice(balance[:])

	var codeHash [32]byte
	copy(codeHash[:], acc.CodeHash)

	payload := protocol.EncodePutAccount(addr, acc.Nonce, balance, codeHash)
	w.accountBytes.Add(uint64(len(payload)))
	return protocol.WriteMsg(w.stdin, protocol.CmdPutAccount, payload)
}

// WriteStorage sends a PutStorage command to the bridge (fire-and-forget).
func (w *BridgeWriter) WriteStorage(addr common.Address, _ uint64, slot, value common.Hash) error {
	payload := protocol.EncodePutStorage(addr, slot, value)
	w.storageBytes.Add(uint64(len(payload)))
	return protocol.WriteMsg(w.stdin, protocol.CmdPutStorage, payload)
}

// WriteCode sends a PutCode command to the bridge (fire-and-forget).
func (w *BridgeWriter) WriteCode(codeHash common.Hash, code []byte) error {
	payload := make([]byte, 32+len(code))
	copy(payload[:32], codeHash[:])
	copy(payload[32:], code)
	w.codeBytes.Add(uint64(len(payload)))
	return protocol.WriteMsg(w.stdin, protocol.CmdPutCode, payload)
}

// SetStateRoot is a no-op for bridge writers; the bridge computes its own root.
func (w *BridgeWriter) SetStateRoot(_ common.Hash) error {
	return nil
}

// WriteGenesisBlock sends genesis config to the bridge and returns the block hash.
func (w *BridgeWriter) WriteGenesisBlock(config *params.ChainConfig, stateRoot common.Hash) error {
	req := struct {
		ChainConfig *params.ChainConfig `json:"chainConfig"`
		StateRoot   common.Hash         `json:"stateRoot"`
		GasLimit    uint64              `json:"gasLimit"`
		BaseFee     *big.Int            `json:"baseFee"`
	}{
		ChainConfig: config,
		StateRoot:   stateRoot,
		GasLimit:    30_000_000,
		BaseFee:     big.NewInt(1_000_000_000),
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal genesis config: %w", err)
	}

	if err := w.sendSync(protocol.CmdWriteGenesis, payload); err != nil {
		return err
	}
	return nil
}

// Flush sends a Flush command and waits for acknowledgement.
func (w *BridgeWriter) Flush() error {
	return w.sendSync(protocol.CmdFlush, nil)
}

// ComputeRoot asks the bridge to compute and return the state root.
func (w *BridgeWriter) ComputeRoot() (common.Hash, error) {
	if err := w.stdin.Flush(); err != nil {
		return common.Hash{}, fmt.Errorf("flush stdin: %w", err)
	}
	if err := protocol.WriteMsg(w.stdin, protocol.CmdComputeRoot, nil); err != nil {
		return common.Hash{}, fmt.Errorf("send ComputeRoot: %w", err)
	}
	if err := w.stdin.Flush(); err != nil {
		return common.Hash{}, fmt.Errorf("flush stdin: %w", err)
	}

	status, payload, err := protocol.ReadResponse(w.stdout)
	if err != nil {
		return common.Hash{}, fmt.Errorf("read ComputeRoot response: %w", err)
	}
	if status != protocol.StatusOK {
		return common.Hash{}, fmt.Errorf("bridge error: %s", string(payload))
	}
	if len(payload) != 32 {
		return common.Hash{}, fmt.Errorf("expected 32-byte root, got %d bytes", len(payload))
	}

	var root common.Hash
	copy(root[:], payload)
	return root, nil
}

// Close sends a Close command and waits for the bridge process to exit.
func (w *BridgeWriter) Close() error {
	// Send close command
	if err := w.stdin.Flush(); err != nil {
		return err
	}
	protocol.WriteMsg(w.stdin, protocol.CmdClose, nil)
	w.stdin.Flush()

	// Read ack (best effort)
	protocol.ReadResponse(w.stdout)

	// Wait for process exit
	return w.cmd.Wait()
}

// Stats returns write statistics.
func (w *BridgeWriter) Stats() WriterStats {
	return WriterStats{
		AccountBytes: w.accountBytes.Load(),
		StorageBytes: w.storageBytes.Load(),
		CodeBytes:    w.codeBytes.Load(),
	}
}

// sendSync flushes buffered writes, sends a command, and reads the response.
func (w *BridgeWriter) sendSync(cmd byte, payload []byte) error {
	if err := w.stdin.Flush(); err != nil {
		return fmt.Errorf("flush stdin: %w", err)
	}
	if err := protocol.WriteMsg(w.stdin, cmd, payload); err != nil {
		return fmt.Errorf("send cmd 0x%02x: %w", cmd, err)
	}
	if err := w.stdin.Flush(); err != nil {
		return fmt.Errorf("flush stdin: %w", err)
	}

	status, respPayload, err := protocol.ReadResponse(w.stdout)
	if err != nil {
		return fmt.Errorf("read response for cmd 0x%02x: %w", cmd, err)
	}
	if status != protocol.StatusOK {
		return fmt.Errorf("bridge error: %s", string(respPayload))
	}
	return nil
}
