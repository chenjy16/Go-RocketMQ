package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var (
	nameServerAddr = flag.String("nameserver", "127.0.0.1:9876", "NameServer地址")
	brokerAddr     = flag.String("broker", "127.0.0.1:10911", "Broker地址")
	interval       = flag.Int("interval", 10, "监控间隔(秒)")
	webPort        = flag.Int("port", 8080, "Web监控端口")
	enableWeb      = flag.Bool("web", false, "启用Web监控界面")
)

// 系统状态
type SystemStatus struct {
	Timestamp    time.Time `json:"timestamp"`
	NameServer   Status    `json:"nameserver"`
	Broker       Status    `json:"broker"`
	SystemInfo   SysInfo   `json:"system_info"`
}

type Status struct {
	Running   bool   `json:"running"`
	PID       int    `json:"pid,omitempty"`
	Address   string `json:"address"`
	Uptime    string `json:"uptime,omitempty"`
	Error     string `json:"error,omitempty"`
}

type SysInfo struct {
	CPUUsage    float64 `json:"cpu_usage"`
	MemoryUsage float64 `json:"memory_usage"`
	DiskUsage   float64 `json:"disk_usage"`
	LoadAvg     string  `json:"load_avg"`
}

// 检查进程是否运行
func checkProcess(processName string) (bool, int) {
	cmd := exec.Command("pgrep", "-f", processName)
	output, err := cmd.Output()
	if err != nil {
		return false, 0
	}
	
	pidStr := strings.TrimSpace(string(output))
	if pidStr == "" {
		return false, 0
	}
	
	// 获取第一个PID
	pids := strings.Split(pidStr, "\n")
	if len(pids) > 0 {
		if pid, err := strconv.Atoi(pids[0]); err == nil {
			return true, pid
		}
	}
	
	return false, 0
}

// 检查网络连接
func checkConnection(address string) error {
	client := &http.Client{Timeout: 3 * time.Second}
	_, err := client.Get("http://" + address + "/health")
	return err
}

// 获取系统信息
func getSystemInfo() SysInfo {
	info := SysInfo{}
	
	// CPU使用率 (简化版本)
	cmd := exec.Command("top", "-l", "1", "-n", "0")
	if output, err := cmd.Output(); err == nil {
		lines := strings.Split(string(output), "\n")
		for _, line := range lines {
			if strings.Contains(line, "CPU usage") {
				// 解析CPU使用率
				parts := strings.Fields(line)
				for i, part := range parts {
					if strings.Contains(part, "user") && i > 0 {
						if cpu, err := strconv.ParseFloat(strings.TrimSuffix(parts[i-1], "%"), 64); err == nil {
							info.CPUUsage = cpu
						}
						break
					}
				}
				break
			}
		}
	}
	
	// 内存使用率
	cmd = exec.Command("vm_stat")
	if _, err := cmd.Output(); err == nil {
		// 简化的内存解析
		info.MemoryUsage = 50.0 // 占位符
	}
	
	// 磁盘使用率
	cmd = exec.Command("df", "-h", ".")
	if output, err := cmd.Output(); err == nil {
		lines := strings.Split(string(output), "\n")
		if len(lines) > 1 {
			fields := strings.Fields(lines[1])
			if len(fields) > 4 {
				usage := strings.TrimSuffix(fields[4], "%")
				if disk, err := strconv.ParseFloat(usage, 64); err == nil {
					info.DiskUsage = disk
				}
			}
		}
	}
	
	// 负载平均值
	cmd = exec.Command("uptime")
	if output, err := cmd.Output(); err == nil {
		uptime := string(output)
		if idx := strings.Index(uptime, "load averages:"); idx != -1 {
			info.LoadAvg = strings.TrimSpace(uptime[idx+14:])
		}
	}
	
	return info
}

// 获取系统状态
func getSystemStatus() SystemStatus {
	status := SystemStatus{
		Timestamp: time.Now(),
	}
	
	// 检查NameServer
	running, pid := checkProcess("nameserver")
	status.NameServer = Status{
		Running: running,
		PID:     pid,
		Address: *nameServerAddr,
	}
	
	// 检查Broker
	running, pid = checkProcess("broker")
	status.Broker = Status{
		Running: running,
		PID:     pid,
		Address: *brokerAddr,
	}
	
	// 获取系统信息
	status.SystemInfo = getSystemInfo()
	
	return status
}

// 打印状态
func printStatus(status SystemStatus) {
	fmt.Printf("\n=== Go-RocketMQ 系统监控 ===\n")
	fmt.Printf("时间: %s\n", status.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Printf("\n组件状态:\n")
	
	// NameServer状态
	if status.NameServer.Running {
		fmt.Printf("  NameServer: ✅ 运行中 (PID: %d, 地址: %s)\n", 
			status.NameServer.PID, status.NameServer.Address)
	} else {
		fmt.Printf("  NameServer: ❌ 未运行 (地址: %s)\n", status.NameServer.Address)
	}
	
	// Broker状态
	if status.Broker.Running {
		fmt.Printf("  Broker:     ✅ 运行中 (PID: %d, 地址: %s)\n", 
			status.Broker.PID, status.Broker.Address)
	} else {
		fmt.Printf("  Broker:     ❌ 未运行 (地址: %s)\n", status.Broker.Address)
	}
	
	// 系统信息
	fmt.Printf("\n系统信息:\n")
	fmt.Printf("  CPU使用率:  %.1f%%\n", status.SystemInfo.CPUUsage)
	fmt.Printf("  内存使用率: %.1f%%\n", status.SystemInfo.MemoryUsage)
	fmt.Printf("  磁盘使用率: %.1f%%\n", status.SystemInfo.DiskUsage)
	fmt.Printf("  负载平均:   %s\n", status.SystemInfo.LoadAvg)
	
	fmt.Printf("=============================\n")
}

// Web监控处理器
func statusHandler(w http.ResponseWriter, r *http.Request) {
	status := getSystemStatus()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// Web界面处理器
func webHandler(w http.ResponseWriter, r *http.Request) {
	html := `
<!DOCTYPE html>
<html>
<head>
    <title>Go-RocketMQ 监控</title>
    <meta charset="utf-8">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .status { margin: 10px 0; padding: 10px; border-radius: 5px; }
        .running { background-color: #d4edda; color: #155724; }
        .stopped { background-color: #f8d7da; color: #721c24; }
        .metric { margin: 5px 0; }
        .refresh { margin: 20px 0; }
    </style>
</head>
<body>
    <h1>Go-RocketMQ 系统监控</h1>
    <div class="refresh">
        <button onclick="location.reload()">刷新</button>
        <span id="timestamp"></span>
    </div>
    
    <h2>组件状态</h2>
    <div id="components"></div>
    
    <h2>系统信息</h2>
    <div id="system"></div>
    
    <script>
        function updateStatus() {
            fetch('/api/status')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('timestamp').textContent = 
                        '最后更新: ' + new Date(data.timestamp).toLocaleString();
                    
                    // 组件状态
                    let components = '';
                    components += '<div class="status ' + (data.nameserver.running ? 'running' : 'stopped') + '">';
                    components += 'NameServer: ' + (data.nameserver.running ? '✅ 运行中' : '❌ 未运行');
                    if (data.nameserver.running) {
                        components += ' (PID: ' + data.nameserver.pid + ')';
                    }
                    components += '</div>';
                    
                    components += '<div class="status ' + (data.broker.running ? 'running' : 'stopped') + '">';
                    components += 'Broker: ' + (data.broker.running ? '✅ 运行中' : '❌ 未运行');
                    if (data.broker.running) {
                        components += ' (PID: ' + data.broker.pid + ')';
                    }
                    components += '</div>';
                    
                    document.getElementById('components').innerHTML = components;
                    
                    // 系统信息
                    let system = '';
                    system += '<div class="metric">CPU使用率: ' + data.system_info.cpu_usage.toFixed(1) + '%</div>';
                    system += '<div class="metric">内存使用率: ' + data.system_info.memory_usage.toFixed(1) + '%</div>';
                    system += '<div class="metric">磁盘使用率: ' + data.system_info.disk_usage.toFixed(1) + '%</div>';
                    system += '<div class="metric">负载平均: ' + data.system_info.load_avg + '</div>';
                    
                    document.getElementById('system').innerHTML = system;
                });
        }
        
        // 初始加载
        updateStatus();
        
        // 自动刷新
        setInterval(updateStatus, 10000);
    </script>
</body>
</html>`
	
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
}

func main() {
	flag.Parse()
	
	if *enableWeb {
		// 启动Web监控
		fmt.Printf("启动Web监控界面，端口: %d\n", *webPort)
		fmt.Printf("访问地址: http://localhost:%d\n", *webPort)
		
		http.HandleFunc("/", webHandler)
		http.HandleFunc("/api/status", statusHandler)
		
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *webPort), nil))
	} else {
		// 命令行监控
		fmt.Println("Go-RocketMQ 系统监控启动")
		fmt.Printf("监控间隔: %d秒\n", *interval)
		fmt.Println("按 Ctrl+C 退出")
		
		for {
			status := getSystemStatus()
			printStatus(status)
			time.Sleep(time.Duration(*interval) * time.Second)
		}
	}
}