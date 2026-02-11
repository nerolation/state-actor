package generator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// LiveStats holds real-time generation statistics.
type LiveStats struct {
	mu sync.RWMutex

	// Config
	TargetAccounts  int    `json:"targetAccounts"`
	TargetContracts int    `json:"targetContracts"`
	OutputFormat    string `json:"outputFormat"`
	Distribution    string `json:"distribution"`
	Seed            int64  `json:"seed"`

	// Progress
	AccountsCreated     int   `json:"accountsCreated"`
	ContractsCreated    int   `json:"contractsCreated"`
	StorageSlotsCreated int   `json:"storageSlotsCreated"`
	TotalBytes          int64 `json:"totalBytes"`
	AccountBytes        int64 `json:"accountBytes"`
	StorageBytes        int64 `json:"storageBytes"`
	CodeBytes           int64 `json:"codeBytes"`

	// Timing
	StartTime   time.Time `json:"startTime"`
	ElapsedMs   int64     `json:"elapsedMs"`
	Throughput  float64   `json:"throughput"` // slots/sec

	// State
	Phase     string `json:"phase"` // "init", "accounts", "contracts", "finalizing", "done"
	StateRoot string `json:"stateRoot"`

	// Distribution histogram (slots per contract)
	SlotHistogram []int `json:"slotHistogram"` // buckets: 0-10, 10-100, 100-1K, 1K-10K, 10K+
}

// StatsServer provides an HTTP endpoint for live stats.
type StatsServer struct {
	stats  *LiveStats
	server *http.Server
}

// NewStatsServer creates a new stats server.
func NewStatsServer(port int) *StatsServer {
	stats := &LiveStats{
		Phase:         "init",
		SlotHistogram: make([]int, 5),
	}

	mux := http.NewServeMux()
	ss := &StatsServer{
		stats: stats,
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
	}

	mux.HandleFunc("/stats", ss.handleStats)
	mux.HandleFunc("/", ss.handleDashboard)

	return ss
}

// Start starts the stats server in a goroutine.
func (ss *StatsServer) Start() error {
	go ss.server.ListenAndServe()
	return nil
}

// Stop stops the stats server.
func (ss *StatsServer) Stop() {
	ss.server.Close()
}

// Stats returns the live stats for updating.
func (ss *StatsServer) Stats() *LiveStats {
	return ss.stats
}

func (ss *StatsServer) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ss.stats.mu.RLock()
	defer ss.stats.mu.RUnlock()

	// Update elapsed time
	if !ss.stats.StartTime.IsZero() {
		ss.stats.ElapsedMs = time.Since(ss.stats.StartTime).Milliseconds()
		if ss.stats.ElapsedMs > 0 {
			ss.stats.Throughput = float64(ss.stats.StorageSlotsCreated) / (float64(ss.stats.ElapsedMs) / 1000)
		}
	}

	json.NewEncoder(w).Encode(ss.stats)
}

func (ss *StatsServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(dashboardHTML))
}

// Update methods for LiveStats

func (ls *LiveStats) SetConfig(accounts, contracts int, format, dist string, seed int64) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.TargetAccounts = accounts
	ls.TargetContracts = contracts
	ls.OutputFormat = format
	ls.Distribution = dist
	ls.Seed = seed
}

func (ls *LiveStats) SetPhase(phase string) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.Phase = phase
	if phase == "accounts" || phase == "contracts" {
		if ls.StartTime.IsZero() {
			ls.StartTime = time.Now()
		}
	}
}

func (ls *LiveStats) AddAccount() {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.AccountsCreated++
}

func (ls *LiveStats) AddContract(slots int) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.ContractsCreated++
	ls.StorageSlotsCreated += slots

	// Update histogram
	switch {
	case slots < 10:
		ls.SlotHistogram[0]++
	case slots < 100:
		ls.SlotHistogram[1]++
	case slots < 1000:
		ls.SlotHistogram[2]++
	case slots < 10000:
		ls.SlotHistogram[3]++
	default:
		ls.SlotHistogram[4]++
	}
}

func (ls *LiveStats) AddBytes(account, storage, code int64) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.AccountBytes += account
	ls.StorageBytes += storage
	ls.CodeBytes += code
	ls.TotalBytes = ls.AccountBytes + ls.StorageBytes + ls.CodeBytes
}

func (ls *LiveStats) SetStateRoot(root string) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.StateRoot = root
	ls.Phase = "done"
}

const dashboardHTML = `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>state-actor monitor</title>
    <style>
        :root {
            --bg: #0c0c0c;
            --surface: #141414;
            --border: #252525;
            --text: #888;
            --text-hi: #ccc;
            --accent: #00d4aa;
            --orange: #e89b50;
            --purple: #a78bfa;
            --red: #f87171;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'SF Mono', 'Consolas', monospace;
            font-size: 12px;
            background: var(--bg);
            color: var(--text);
            padding: 1rem;
            min-height: 100vh;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            max-width: 1200px;
            margin: 0 auto;
        }
        .card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 4px;
            padding: 1rem;
        }
        .card.wide { grid-column: span 2; }
        .card-title {
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text);
            margin-bottom: 0.75rem;
        }
        .big-num {
            font-size: 28px;
            font-weight: 600;
            color: var(--accent);
            line-height: 1;
        }
        .big-num.orange { color: var(--orange); }
        .big-num.purple { color: var(--purple); }
        .unit {
            font-size: 11px;
            color: var(--text);
            margin-left: 4px;
        }
        .sub {
            font-size: 11px;
            color: var(--text);
            margin-top: 4px;
        }
        .progress-bar {
            height: 4px;
            background: var(--border);
            border-radius: 2px;
            margin-top: 0.75rem;
            overflow: hidden;
        }
        .progress-fill {
            height: 100%;
            background: var(--accent);
            transition: width 0.3s;
        }
        .bar-chart {
            display: flex;
            align-items: flex-end;
            gap: 4px;
            height: 60px;
            margin-top: 0.5rem;
        }
        .bar {
            flex: 1;
            background: var(--accent);
            border-radius: 2px 2px 0 0;
            min-height: 2px;
            opacity: 0.8;
        }
        .bar-labels {
            display: flex;
            gap: 4px;
            margin-top: 4px;
            font-size: 9px;
            color: var(--text);
        }
        .bar-labels span { flex: 1; text-align: center; }
        .status {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 10px;
            text-transform: uppercase;
        }
        .status.running { background: rgba(0,212,170,0.2); color: var(--accent); }
        .status.done { background: rgba(168,139,250,0.2); color: var(--purple); }
        .status.init { background: rgba(136,136,136,0.2); color: var(--text); }
        .bytes-grid {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 0.5rem;
            margin-top: 0.5rem;
        }
        .bytes-item {
            text-align: center;
            padding: 0.5rem;
            background: var(--bg);
            border-radius: 3px;
        }
        .bytes-val {
            font-size: 14px;
            color: var(--text-hi);
        }
        .bytes-label {
            font-size: 9px;
            color: var(--text);
            margin-top: 2px;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1rem;
            padding-bottom: 1rem;
            border-bottom: 1px solid var(--border);
            max-width: 1200px;
            margin-left: auto;
            margin-right: auto;
        }
        .header h1 {
            font-size: 14px;
            font-weight: 600;
            color: var(--accent);
        }
        .config-line {
            font-size: 11px;
            color: var(--text);
        }
        .config-line span { color: var(--orange); }
        .root {
            font-family: monospace;
            font-size: 11px;
            color: var(--purple);
            word-break: break-all;
            margin-top: 0.5rem;
        }
        .no-data {
            color: var(--text);
            text-align: center;
            padding: 2rem;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>state-actor</h1>
        <div class="config-line" id="config">waiting for data...</div>
    </div>
    <div class="grid" id="dashboard">
        <div class="no-data">connecting...</div>
    </div>
    <script>
        function fmt(n) {
            if (n >= 1e9) return (n/1e9).toFixed(1) + 'B';
            if (n >= 1e6) return (n/1e6).toFixed(1) + 'M';
            if (n >= 1e3) return (n/1e3).toFixed(1) + 'K';
            return n.toString();
        }
        function fmtBytes(b) {
            if (b >= 1e9) return (b/1e9).toFixed(2) + ' GB';
            if (b >= 1e6) return (b/1e6).toFixed(2) + ' MB';
            if (b >= 1e3) return (b/1e3).toFixed(2) + ' KB';
            return b + ' B';
        }
        function fmtTime(ms) {
            if (ms >= 60000) return (ms/60000).toFixed(1) + 'm';
            if (ms >= 1000) return (ms/1000).toFixed(1) + 's';
            return ms + 'ms';
        }

        function render(s) {
            const pctAccounts = s.targetAccounts ? (s.accountsCreated / s.targetAccounts * 100) : 0;
            const pctContracts = s.targetContracts ? (s.contractsCreated / s.targetContracts * 100) : 0;
            const statusClass = s.phase === 'done' ? 'done' : (s.phase === 'init' ? 'init' : 'running');

            document.getElementById('config').innerHTML = 
                '<span>--seed</span> ' + s.seed + 
                ' <span>--distribution</span> ' + s.distribution + 
                ' <span>--output-format</span> ' + s.outputFormat;

            const maxHist = Math.max(...s.slotHistogram, 1);

            document.getElementById('dashboard').innerHTML = ` + "`" + `
                <div class="card">
                    <div class="card-title">status</div>
                    <span class="status ${statusClass}">${s.phase}</span>
                    <div class="sub">${fmtTime(s.elapsedMs)} elapsed</div>
                </div>
                <div class="card">
                    <div class="card-title">throughput</div>
                    <div class="big-num">${fmt(Math.round(s.throughput))}<span class="unit">slots/s</span></div>
                </div>
                <div class="card">
                    <div class="card-title">accounts</div>
                    <div class="big-num orange">${fmt(s.accountsCreated)}</div>
                    <div class="sub">of ${fmt(s.targetAccounts)} target</div>
                    <div class="progress-bar"><div class="progress-fill" style="width:${pctAccounts}%"></div></div>
                </div>
                <div class="card">
                    <div class="card-title">contracts</div>
                    <div class="big-num orange">${fmt(s.contractsCreated)}</div>
                    <div class="sub">of ${fmt(s.targetContracts)} target</div>
                    <div class="progress-bar"><div class="progress-fill" style="width:${pctContracts}%"></div></div>
                </div>
                <div class="card">
                    <div class="card-title">storage slots</div>
                    <div class="big-num purple">${fmt(s.storageSlotsCreated)}</div>
                    <div class="sub">${(s.storageSlotsCreated / Math.max(s.contractsCreated, 1)).toFixed(1)} avg per contract</div>
                </div>
                <div class="card">
                    <div class="card-title">total size</div>
                    <div class="big-num">${fmtBytes(s.totalBytes)}</div>
                    <div class="bytes-grid">
                        <div class="bytes-item"><div class="bytes-val">${fmtBytes(s.accountBytes)}</div><div class="bytes-label">accounts</div></div>
                        <div class="bytes-item"><div class="bytes-val">${fmtBytes(s.storageBytes)}</div><div class="bytes-label">storage</div></div>
                        <div class="bytes-item"><div class="bytes-val">${fmtBytes(s.codeBytes)}</div><div class="bytes-label">code</div></div>
                    </div>
                </div>
                <div class="card wide">
                    <div class="card-title">slot distribution (contracts by slot count)</div>
                    <div class="bar-chart">
                        ${s.slotHistogram.map(v => '<div class="bar" style="height:' + (v/maxHist*100) + '%"></div>').join('')}
                    </div>
                    <div class="bar-labels">
                        <span>0-10</span><span>10-100</span><span>100-1K</span><span>1K-10K</span><span>10K+</span>
                    </div>
                </div>
                ${s.stateRoot ? '<div class="card wide"><div class="card-title">state root</div><div class="root">' + s.stateRoot + '</div></div>' : ''}
            ` + "`" + `;
        }

        function poll() {
            fetch('/stats')
                .then(r => r.json())
                .then(render)
                .catch(() => {});
        }

        poll();
        setInterval(poll, 500);
    </script>
</body>
</html>`
