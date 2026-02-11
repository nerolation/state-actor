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

	// Treemap data - top contracts by slot count
	TreemapData []TreemapNode `json:"treemapData"`
}

// TreemapNode represents a contract in the treemap
type TreemapNode struct {
	ID    int `json:"id"`
	Slots int `json:"slots"`
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

	// Update treemap data - keep last 200 contracts as rolling sample
	ls.TreemapData = append(ls.TreemapData, TreemapNode{
		ID:    ls.ContractsCreated,
		Slots: slots,
	})
	if len(ls.TreemapData) > 200 {
		ls.TreemapData = ls.TreemapData[len(ls.TreemapData)-200:]
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
            --bg: #0a0a0c;
            --surface: #131318;
            --border: #1e1e26;
            --text: #666;
            --text-hi: #999;
            --accent: #00d4aa;
            --accent2: #00a888;
            --orange: #e89b50;
            --purple: #a78bfa;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'SF Mono', 'Consolas', monospace;
            font-size: 11px;
            background: var(--bg);
            color: var(--text);
            padding: 1rem;
            min-height: 100vh;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1rem;
            padding-bottom: 0.75rem;
            border-bottom: 1px solid var(--border);
        }
        .header h1 { font-size: 13px; font-weight: 600; color: var(--accent); }
        .config-line { font-size: 10px; color: var(--text); }
        .config-line span { color: var(--orange); }
        
        .grid {
            display: grid;
            grid-template-columns: repeat(6, 1fr);
            gap: 0.75rem;
        }
        .card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 4px;
            padding: 0.75rem;
        }
        .card.span2 { grid-column: span 2; }
        .card.span3 { grid-column: span 3; }
        .card.span4 { grid-column: span 4; }
        .card.span6 { grid-column: span 6; }
        .card-title {
            font-size: 9px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text);
            margin-bottom: 0.5rem;
        }
        .metric {
            font-size: 22px;
            font-weight: 600;
            color: var(--accent);
            line-height: 1;
        }
        .metric.orange { color: var(--orange); }
        .metric.purple { color: var(--purple); }
        .metric .unit { font-size: 10px; color: var(--text); margin-left: 2px; }
        .sub { font-size: 10px; color: var(--text); margin-top: 3px; }
        
        .progress {
            height: 3px;
            background: var(--border);
            border-radius: 2px;
            margin-top: 0.5rem;
            overflow: hidden;
        }
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, var(--accent2), var(--accent));
            transition: width 0.3s;
        }
        
        .status {
            display: inline-block;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 9px;
            text-transform: uppercase;
            font-weight: 500;
        }
        .status.running { background: rgba(0,212,170,0.15); color: var(--accent); }
        .status.done { background: rgba(167,139,250,0.15); color: var(--purple); }
        .status.init { background: rgba(102,102,102,0.15); color: var(--text); }
        
        .bar-chart {
            display: flex;
            align-items: flex-end;
            gap: 3px;
            height: 50px;
        }
        .bar {
            flex: 1;
            background: linear-gradient(180deg, var(--accent), var(--accent2));
            border-radius: 2px 2px 0 0;
            min-height: 2px;
        }
        .bar-labels {
            display: flex;
            gap: 3px;
            margin-top: 3px;
            font-size: 8px;
        }
        .bar-labels span { flex: 1; text-align: center; }
        
        .treemap {
            width: 100%;
            height: 180px;
            position: relative;
            border-radius: 3px;
            overflow: hidden;
        }
        .treemap-rect {
            position: absolute;
            box-sizing: border-box;
            border: 1px solid var(--bg);
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 8px;
            color: rgba(0,0,0,0.6);
            overflow: hidden;
            transition: all 0.2s;
        }
        .treemap-rect:hover {
            border-color: #fff;
            z-index: 10;
        }
        
        .root-hash {
            font-family: monospace;
            font-size: 10px;
            color: var(--purple);
            word-break: break-all;
            background: var(--bg);
            padding: 0.5rem;
            border-radius: 3px;
            margin-top: 0.5rem;
        }

        @media (max-width: 900px) {
            .grid { grid-template-columns: repeat(2, 1fr); }
            .card.span2, .card.span3, .card.span4, .card.span6 { grid-column: span 2; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>state-actor</h1>
            <div class="config-line" id="config">waiting...</div>
        </div>
        <div class="grid" id="dashboard"></div>
    </div>
    <script>
        const colors = [
            '#00d4aa', '#00c49a', '#00b48a', '#00a47a', '#00946a',
            '#00845a', '#00744a', '#00643a', '#00542a', '#00441a'
        ];
        
        function fmt(n) {
            if (n >= 1e9) return (n/1e9).toFixed(1) + 'B';
            if (n >= 1e6) return (n/1e6).toFixed(1) + 'M';
            if (n >= 1e3) return (n/1e3).toFixed(1) + 'K';
            return n.toString();
        }
        function fmtBytes(b) {
            if (b >= 1e9) return (b/1e9).toFixed(1) + 'GB';
            if (b >= 1e6) return (b/1e6).toFixed(1) + 'MB';
            if (b >= 1e3) return (b/1e3).toFixed(1) + 'KB';
            return b + 'B';
        }
        function fmtTime(ms) {
            if (ms >= 60000) return (ms/60000).toFixed(1) + 'm';
            if (ms >= 1000) return (ms/1000).toFixed(1) + 's';
            return ms + 'ms';
        }
        
        function buildTreemap(data, width, height) {
            if (!data || data.length === 0) return '<div style="color:var(--text);padding:1rem;text-align:center">generating...</div>';
            
            // Sort by slots descending, take top 100
            const sorted = [...data].sort((a, b) => b.slots - a.slots).slice(0, 100);
            const total = sorted.reduce((s, d) => s + d.slots, 0);
            if (total === 0) return '';
            
            // Simple squarified treemap layout
            let rects = [];
            let x = 0, y = 0, w = width, h = height;
            let remaining = [...sorted];
            
            while (remaining.length > 0) {
                const isHoriz = w >= h;
                const side = isHoriz ? h : w;
                
                // Find best row
                let row = [];
                let rowTotal = 0;
                let best = Infinity;
                
                for (let i = 0; i < remaining.length; i++) {
                    row.push(remaining[i]);
                    rowTotal += remaining[i].slots;
                    const rowFrac = rowTotal / total;
                    const rowSize = rowFrac * (isHoriz ? w : h);
                    
                    let worst = 0;
                    for (const r of row) {
                        const frac = r.slots / rowTotal;
                        const rw = isHoriz ? rowSize : frac * side;
                        const rh = isHoriz ? frac * side : rowSize;
                        const aspect = Math.max(rw/rh, rh/rw);
                        worst = Math.max(worst, aspect);
                    }
                    
                    if (worst > best) {
                        row.pop();
                        rowTotal -= remaining[i].slots;
                        break;
                    }
                    best = worst;
                }
                
                if (row.length === 0) row = [remaining[0]], rowTotal = remaining[0].slots;
                
                // Layout row
                const rowFrac = rowTotal / total;
                const rowSize = rowFrac * (isHoriz ? w : h);
                let offset = 0;
                
                for (const r of row) {
                    const frac = r.slots / rowTotal;
                    const itemSize = frac * side;
                    const colorIdx = Math.min(Math.floor(Math.log10(Math.max(r.slots, 1)) * 2), colors.length - 1);
                    
                    if (isHoriz) {
                        rects.push({ x: x, y: y + offset, w: rowSize, h: itemSize, slots: r.slots, color: colors[colorIdx] });
                    } else {
                        rects.push({ x: x + offset, y: y, w: itemSize, h: rowSize, slots: r.slots, color: colors[colorIdx] });
                    }
                    offset += itemSize;
                }
                
                // Update bounds
                if (isHoriz) { x += rowSize; w -= rowSize; }
                else { y += rowSize; h -= rowSize; }
                
                // Remove processed items
                remaining = remaining.filter(r => !row.includes(r));
                total - rowTotal;
            }
            
            return rects.map(r => 
                '<div class="treemap-rect" style="left:' + r.x + 'px;top:' + r.y + 'px;width:' + r.w + 'px;height:' + r.h + 'px;background:' + r.color + '" title="' + r.slots + ' slots">' + 
                (r.w > 25 && r.h > 15 ? r.slots : '') + '</div>'
            ).join('');
        }

        function render(s) {
            const pctA = s.targetAccounts ? (s.accountsCreated / s.targetAccounts * 100) : 0;
            const pctC = s.targetContracts ? (s.contractsCreated / s.targetContracts * 100) : 0;
            const statusClass = s.phase === 'done' ? 'done' : (s.phase === 'init' ? 'init' : 'running');
            const maxHist = Math.max(...s.slotHistogram, 1);
            const avgSlots = s.contractsCreated > 0 ? (s.storageSlotsCreated / s.contractsCreated).toFixed(1) : '0';

            document.getElementById('config').innerHTML = 
                '<span>seed</span> ' + s.seed + ' · <span>dist</span> ' + s.distribution + ' · <span>format</span> ' + s.outputFormat;

            document.getElementById('dashboard').innerHTML = 
                '<div class="card">' +
                    '<div class="card-title">status</div>' +
                    '<span class="status ' + statusClass + '">' + s.phase + '</span>' +
                    '<div class="sub">' + fmtTime(s.elapsedMs) + '</div>' +
                '</div>' +
                '<div class="card">' +
                    '<div class="card-title">throughput</div>' +
                    '<div class="metric">' + fmt(Math.round(s.throughput)) + '<span class="unit">slots/s</span></div>' +
                '</div>' +
                '<div class="card">' +
                    '<div class="card-title">accounts</div>' +
                    '<div class="metric orange">' + fmt(s.accountsCreated) + '</div>' +
                    '<div class="sub">/ ' + fmt(s.targetAccounts) + '</div>' +
                    '<div class="progress"><div class="progress-fill" style="width:' + pctA + '%"></div></div>' +
                '</div>' +
                '<div class="card">' +
                    '<div class="card-title">contracts</div>' +
                    '<div class="metric orange">' + fmt(s.contractsCreated) + '</div>' +
                    '<div class="sub">/ ' + fmt(s.targetContracts) + '</div>' +
                    '<div class="progress"><div class="progress-fill" style="width:' + pctC + '%"></div></div>' +
                '</div>' +
                '<div class="card">' +
                    '<div class="card-title">slots</div>' +
                    '<div class="metric purple">' + fmt(s.storageSlotsCreated) + '</div>' +
                    '<div class="sub">' + avgSlots + ' avg/contract</div>' +
                '</div>' +
                '<div class="card">' +
                    '<div class="card-title">size</div>' +
                    '<div class="metric">' + fmtBytes(s.totalBytes) + '</div>' +
                    '<div class="sub">acct ' + fmtBytes(s.accountBytes) + ' · stor ' + fmtBytes(s.storageBytes) + '</div>' +
                '</div>' +
                '<div class="card span3">' +
                    '<div class="card-title">distribution histogram</div>' +
                    '<div class="bar-chart">' +
                        s.slotHistogram.map(v => '<div class="bar" style="height:' + (v/maxHist*100) + '%"></div>').join('') +
                    '</div>' +
                    '<div class="bar-labels"><span>0-10</span><span>10-100</span><span>100-1K</span><span>1K-10K</span><span>10K+</span></div>' +
                '</div>' +
                '<div class="card span3">' +
                    '<div class="card-title">state treemap (recent contracts by slot count)</div>' +
                    '<div class="treemap">' + buildTreemap(s.treemapData, 520, 180) + '</div>' +
                '</div>' +
                (s.stateRoot ? '<div class="card span6"><div class="card-title">state root</div><div class="root-hash">' + s.stateRoot + '</div></div>' : '');
        }

        function poll() {
            fetch('/stats').then(r => r.json()).then(render).catch(() => {});
        }
        poll();
        setInterval(poll, 400);
    </script>
</body>
</html>`
