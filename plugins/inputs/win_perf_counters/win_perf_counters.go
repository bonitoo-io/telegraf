// +build windows

package win_perf_counters

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

var sampleConfig = `
  ## By default this plugin returns basic CPU and Disk statistics.
  ## See the README file for more examples.
  ## Uncomment examples below or write your own as you see fit. If the system
  ## being polled for data does not have the Object at startup of the Telegraf
  ## agent, it will not be gathered.
  ## Settings:
  # PrintValid = false # Print All matching performance counters
  # Whether request a timestamp along with the PerfCounter data or just use current time
  # UsePerfCounterTime=true
  # If UseWildcardsExpansion params is set to true, wildcards (partial wildcards in instance names and wildcards in counters names) in configured counter paths will be expanded
  # and in case of localized Windows, counter paths will be also localized. It also returns instance indexes in instance names.
  # If false, wildcards (not partial) in instance names will still be expanded, but instance indexes will not be returned in instance names.
  #UseWildcardsExpansion = false
  # Period after which counters will be reread from configuration and wildcards in counter paths expanded
  CountersRefreshInterval="1m"

  [[inputs.win_perf_counters.object]]
    # Processor usage, alternative to native, reports on a per core.
    ObjectName = "Processor"
    Instances = ["*"]
    Counters = [
      "% Idle Time",
      "% Interrupt Time",
      "% Privileged Time",
      "% User Time",
      "% Processor Time",
      "% DPC Time",
    ]
    Measurement = "win_cpu"
    # Names or IPs of remote computers to gather counters from, including local computer.
    # Telegraf's user must be already authenticated to the remote computers.
    # Computers = ["localhost","SQL-SERVER-01","SQL-SERVER-02"]
    # Set to true to include _Total instance when querying for all (*).
    # IncludeTotal=false
    # Print out when the performance counter is missing from object, counter or instance.
    # WarnOnMissing = false

  [[inputs.win_perf_counters.object]]
    # Disk times and queues
    ObjectName = "LogicalDisk"
    Instances = ["*"]
    Counters = [
      "% Idle Time",
      "% Disk Time",
      "% Disk Read Time",
      "% Disk Write Time",
      "% User Time",
      "% Free Space",
      "Current Disk Queue Length",
      "Free Megabytes",
    ]
    Measurement = "win_disk"

  [[inputs.win_perf_counters.object]]
    ObjectName = "PhysicalDisk"
    Instances = ["*"]
    Counters = [
      "Disk Read Bytes/sec",
      "Disk Write Bytes/sec",
      "Current Disk Queue Length",
      "Disk Reads/sec",
      "Disk Writes/sec",
      "% Disk Time",
      "% Disk Read Time",
      "% Disk Write Time",
    ]
    Measurement = "win_diskio"

  [[inputs.win_perf_counters.object]]
    ObjectName = "Network Interface"
    Instances = ["*"]
    Counters = [
      "Bytes Received/sec",
      "Bytes Sent/sec",
      "Packets Received/sec",
      "Packets Sent/sec",
      "Packets Received Discarded",
      "Packets Outbound Discarded",
      "Packets Received Errors",
      "Packets Outbound Errors",
    ]
    Measurement = "win_net"


  [[inputs.win_perf_counters.object]]
    ObjectName = "System"
    Counters = [
      "Context Switches/sec",
      "System Calls/sec",
      "Processor Queue Length",
      "System Up Time",
    ]
    Instances = ["------"]
    Measurement = "win_system"

  [[inputs.win_perf_counters.object]]
    # Example counterPath where the Instance portion must be removed to get data back,
    # such as from the Memory object.
    ObjectName = "Memory"
    Counters = [
      "Available Bytes",
      "Cache Faults/sec",
      "Demand Zero Faults/sec",
      "Page Faults/sec",
      "Pages/sec",
      "Transition Faults/sec",
      "Pool Nonpaged Bytes",
      "Pool Paged Bytes",
      "Standby Cache Reserve Bytes",
      "Standby Cache Normal Priority Bytes",
      "Standby Cache Core Bytes",
    ]
    Instances = ["------"] # Use 6 x - to remove the Instance bit from the counterPath.
    Measurement = "win_mem"

  [[inputs.win_perf_counters.object]]
    # Example query where the Instance portion must be removed to get data back,
    # such as from the Paging File object.
    ObjectName = "Paging File"
    Counters = [
      "% Usage",
    ]
    Instances = ["_Total"]
    Measurement = "win_swap"
`

type WinPerfCounters struct {
	PrintValid bool
	//deprecated: determined dynamically
	PreVistaSupport         bool
	UsePerfCounterTime      bool
	Object                  []perfobject
	CountersRefreshInterval internal.Duration
	UseWildcardsExpansion   bool

	Log telegraf.Logger

	lastRefreshed time.Time
	queryCreator  PerformanceQueryCreator
	hostCounters  map[string]*hostCountersInfo
}

type hostCountersInfo struct {
	// computer name used as key and for printing
	computer string
	// computer name used in tag
	computerTag string
	counters    []*counter
	query       PerformanceQuery
	timestamp   time.Time
}

type perfobject struct {
	Computers     []string
	ObjectName    string
	Counters      []string
	Instances     []string
	Measurement   string
	WarnOnMissing bool
	FailOnMissing bool
	IncludeTotal  bool
}

type counter struct {
	counterPath   string
	computer      string
	objectName    string
	counter       string
	instance      string
	measurement   string
	includeTotal  bool
	counterHandle PDH_HCOUNTER
}

type instanceGrouping struct {
	name       string
	instance   string
	objectname string
}

type fieldGrouping map[instanceGrouping]map[string]interface{}

var sanitizedChars = strings.NewReplacer("/sec", "_persec", "/Sec", "_persec",
	" ", "_", "%", "Percent", `\`, "")

// extractCounterInfoFromCounterPath gets object name, instance name (if available) and counter name from counter path
// General Counter path pattern is: \\computer\object(parent/instance#index)\counter
// parent/instance#index part is skipped in single instance objects (e.g. Memory): \\computer\object\counter
func extractCounterInfoFromCounterPath(counterPath string) (computer, object, instance, counter string, err error) {
	computer = ""
	leftComputerBorderIndex := -1
	rightObjectBorderIndex := -1
	leftObjectBorderIndex := -1
	leftCounterBorderIndex := -1
	rightInstanceBorderIndex := -1
	leftInstanceBorderIndex := -1
	bracketLevel := 0

	for i := len(counterPath) - 1; i >= 0; i-- {
		switch counterPath[i] {
		case '\\':
			if bracketLevel == 0 {
				if leftCounterBorderIndex == -1 {
					leftCounterBorderIndex = i
				} else if leftObjectBorderIndex == -1 {
					leftObjectBorderIndex = i
				} else if leftComputerBorderIndex == -1 {
					leftComputerBorderIndex = i
				}
			}
		case '(':
			bracketLevel--
			if leftInstanceBorderIndex == -1 && bracketLevel == 0 && leftObjectBorderIndex == -1 && leftCounterBorderIndex > -1 {
				leftInstanceBorderIndex = i
				rightObjectBorderIndex = i
			}
		case ')':
			if rightInstanceBorderIndex == -1 && bracketLevel == 0 && leftCounterBorderIndex > -1 {
				rightInstanceBorderIndex = i
			}
			bracketLevel++
		}
	}
	if rightObjectBorderIndex == -1 {
		rightObjectBorderIndex = leftCounterBorderIndex
	}
	if rightObjectBorderIndex == -1 || leftObjectBorderIndex == -1 {
		err = errors.New("cannot parse object from: " + counterPath)
		return
	}
	if leftComputerBorderIndex > -1 {
		// validate there is leading \\ and not empty computer (\\\O)
		if leftComputerBorderIndex != 1 || leftComputerBorderIndex == leftObjectBorderIndex-1 {
			err = errors.New("cannot parse computer from: " + counterPath)
			return
		} else {
			computer = counterPath[leftComputerBorderIndex+1 : leftObjectBorderIndex]
		}
	}

	if leftInstanceBorderIndex > -1 && rightInstanceBorderIndex > -1 {
		instance = counterPath[leftInstanceBorderIndex+1 : rightInstanceBorderIndex]
	} else if (leftInstanceBorderIndex == -1 && rightInstanceBorderIndex > -1) || (leftInstanceBorderIndex > -1 && rightInstanceBorderIndex == -1) {
		err = errors.New("cannot parse instance from: " + counterPath)
		return
	}
	object = counterPath[leftObjectBorderIndex+1 : rightObjectBorderIndex]
	counter = counterPath[leftCounterBorderIndex+1:]
	return
}

func (m *WinPerfCounters) Description() string {
	return "Input plugin to counterPath Performance Counters on Windows operating systems"
}

func (m *WinPerfCounters) SampleConfig() string {
	return sampleConfig
}

//objectName string, counter string, instance string, measurement string, include_total bool
func (m *WinPerfCounters) AddItem(counterPath, computer, objectName, instance, counterName, measurement string, includeTotal bool) error {
	var err error
	var counterHandle PDH_HCOUNTER
	var hostCounter *hostCountersInfo
	var ok bool
	computerTag := computer
	if computer == "" {
		computer = "localhost"
		computerTag = ""
	}
	if m.hostCounters == nil {
		m.hostCounters = make(map[string]*hostCountersInfo)
	}
	if hostCounter, ok = m.hostCounters[computer]; !ok {
		hostCounter = &hostCountersInfo{computer: computer, computerTag: computerTag}
		m.hostCounters[computer] = hostCounter
		hostCounter.query = m.queryCreator.NewPerformanceQuery(computer)
		if err = hostCounter.query.Open(); err != nil {
			return err
		}
		hostCounter.counters = make([]*counter, 0, 0)
	}

	if !hostCounter.query.IsVistaOrNewer() {
		counterHandle, err = hostCounter.query.AddCounterToQuery(counterPath)
		if err != nil {
			return err
		}
	} else {
		counterHandle, err = hostCounter.query.AddEnglishCounterToQuery(counterPath)
		if err != nil {
			return err
		}

	}

	if m.UseWildcardsExpansion {
		origInstance := instance
		counterPath, err = hostCounter.query.GetCounterPath(counterHandle)
		if err != nil {
			return err
		}
		counters, err := hostCounter.query.ExpandWildCardPath(counterPath)
		if err != nil {
			return err
		}

		for _, counterPath := range counters {
			var err error
			counterHandle, err := hostCounter.query.AddCounterToQuery(counterPath)

			computer, objectName, instance, counterName, err = extractCounterInfoFromCounterPath(counterPath)
			if err != nil {
				return err
			}

			if instance == "_Total" && origInstance == "*" && !includeTotal {
				continue
			}

			newItem := &counter{counterPath, computer, objectName, counterName, instance, measurement,
				includeTotal, counterHandle}
			hostCounter.counters = append(hostCounter.counters, newItem)

			if m.PrintValid {
				m.Log.Infof("Valid: %s", counterPath)
			}
		}
	} else {
		newItem := &counter{counterPath, computer, objectName, counterName, instance, measurement,
			includeTotal, counterHandle}
		hostCounter.counters = append(hostCounter.counters, newItem)
		if m.PrintValid {
			m.Log.Infof("Valid: %s", counterPath)
		}
	}

	return nil
}

func (m *WinPerfCounters) ParseConfig() error {
	var counterPath string

	if len(m.Object) > 0 {
		for _, PerfObject := range m.Object {
			computers := PerfObject.Computers
			if len(computers) == 0 {
				computers = append(computers, "")
			}
			for _, computer := range computers {
				computerName := ""
				if strings.ToLower(computer) == "localhost" {
					// localhost as a computer name in counter path doesn't work
					computer = ""
				}
				if computer != "" {
					computerName = "\\\\" + computer
				}
				for _, counter := range PerfObject.Counters {
					for _, instance := range PerfObject.Instances {

						objectname := PerfObject.ObjectName

						if instance == "------" {
							counterPath = computerName + "\\" + objectname + "\\" + counter
						} else {
							counterPath = computerName + "\\" + objectname + "(" + instance + ")\\" + counter
						}

						err := m.AddItem(counterPath, computer, objectname, instance, counter, PerfObject.Measurement, PerfObject.IncludeTotal)

						if err != nil {
							if PerfObject.FailOnMissing || PerfObject.WarnOnMissing {
								m.Log.Errorf("invalid counterPath: '%s'. Error: %s\n", counterPath, err.Error())
							}
							if PerfObject.FailOnMissing {
								return err
							}
						}
					}
				}
			}
		}
		return nil
	} else {
		err := errors.New("no performance objects configured")
		return err
	}

}

func (m *WinPerfCounters) Gather(acc telegraf.Accumulator) error {
	// Parse the config once
	var err error

	if m.lastRefreshed.IsZero() || (m.CountersRefreshInterval.Duration.Nanoseconds() > 0 && m.lastRefreshed.Add(m.CountersRefreshInterval.Duration).Before(time.Now())) {
		if err = m.cleanQueries(); err != nil {
			return err
		}

		if err = m.ParseConfig(); err != nil {
			return err
		}
		for _, hostCounterSet := range m.hostCounters {
			//some counters need two data samples before computing a value
			if err = hostCounterSet.query.CollectData(); err != nil {
				return err
			}
		}
		m.lastRefreshed = time.Now()
		// minimum time between collecting two samples
		time.Sleep(time.Second)
	}

	for _, hostCounterSet := range m.hostCounters {
		if m.UsePerfCounterTime && hostCounterSet.query.IsVistaOrNewer() {
			hostCounterSet.timestamp, err = hostCounterSet.query.CollectDataWithTime()
			if err != nil {
				return err
			}
		} else {
			hostCounterSet.timestamp = time.Now()
			if err = hostCounterSet.query.CollectData(); err != nil {
				return err
			}
		}
	}
	var wg sync.WaitGroup
	//iterate over computers
	for _, hostCounterInfo := range m.hostCounters {
		go func(hostInfo *hostCountersInfo) {
			m.Log.Debugf("gathering from %s", hostInfo.computer)
			start := time.Now()
			err = m.gatherComputerCounters(hostInfo, acc)
			m.Log.Debugf("gathering from %s finished in %.3fs", hostInfo.computer, time.Now().Sub(start).Seconds())
			if err != nil {
				acc.AddError(fmt.Errorf("error during collecting data on host '%s': %s", hostInfo.computer, err.Error()))
			}
			wg.Done()
		}(hostCounterInfo)
		wg.Add(1)
	}

	wg.Wait()
	return nil
}

func (m *WinPerfCounters) gatherComputerCounters(hostCounterInfo *hostCountersInfo, acc telegraf.Accumulator) error {
	// For iterate over the known metrics and get the samples.
	collectedFields := make(fieldGrouping)
	for _, metric := range hostCounterInfo.counters {
		// collect
		if m.UseWildcardsExpansion {
			value, err := hostCounterInfo.query.GetFormattedCounterValueDouble(metric.counterHandle)
			if err == nil {
				addCounterMeasurement(metric, metric.instance, value, collectedFields)
			} else {
				//ignore invalid data  as some counters from process instances returns this sometimes
				if !isKnownCounterDataError(err) {
					return fmt.Errorf("error while getting value for counter %s: %v", metric.counterPath, err)
				}
			}
		} else {
			counterValues, err := hostCounterInfo.query.GetFormattedCounterArrayDouble(metric.counterHandle)
			if err == nil {
				for _, cValue := range counterValues {
					var add bool
					if metric.includeTotal {
						// If IncludeTotal is set, include all.
						add = true
					} else if metric.instance == "*" && !strings.Contains(cValue.InstanceName, "_Total") {
						// Catch if set to * and that it is not a '*_Total*' instance.
						add = true
					} else if metric.instance == cValue.InstanceName {
						// Catch if we set it to total or some form of it
						add = true
					} else if strings.Contains(metric.instance, "#") && strings.HasPrefix(metric.instance, cValue.InstanceName) {
						// If you are using a multiple instance identifier such as "w3wp#1"
						// phd.dll returns only the first 2 characters of the identifier.
						add = true
						cValue.InstanceName = metric.instance
					} else if metric.instance == "------" {
						add = true
					}

					if add {
						addCounterMeasurement(metric, cValue.InstanceName, cValue.Value, collectedFields)
					}
				}
			} else {
				//ignore invalid data as some counters from process instances returns this sometimes
				if !isKnownCounterDataError(err) {
					return fmt.Errorf("error while getting value for counter %s: %v", metric.counterPath, err)
				}
			}
		}
	}
	for instance, fields := range collectedFields {
		var tags = map[string]string{
			"objectname": instance.objectname,
		}
		if len(instance.instance) > 0 {
			tags["instance"] = instance.instance
		}
		if len(hostCounterInfo.computerTag) > 0 {
			tags["computer"] = hostCounterInfo.computerTag
		}
		acc.AddFields(instance.name, fields, tags, hostCounterInfo.timestamp)
	}
	return nil

}

func (m *WinPerfCounters) cleanQueries() error {
	for _, hostCounterInfo := range m.hostCounters {
		if err := hostCounterInfo.query.Close(); err != nil {
			return err
		}
	}
	m.hostCounters = nil
	return nil
}

func addCounterMeasurement(metric *counter, instanceName string, value float64, collectFields fieldGrouping) {
	measurement := sanitizedChars.Replace(metric.measurement)
	if measurement == "" {
		measurement = "win_perf_counters"
	}
	var instance = instanceGrouping{measurement, instanceName, metric.objectName}
	if collectFields[instance] == nil {
		collectFields[instance] = make(map[string]interface{})
	}
	collectFields[instance][sanitizedChars.Replace(metric.counter)] = float32(value)
}

func isKnownCounterDataError(err error) bool {
	if pdhErr, ok := err.(*PdhError); ok && (pdhErr.ErrorCode == PDH_INVALID_DATA ||
		pdhErr.ErrorCode == PDH_CALC_NEGATIVE_VALUE ||
		pdhErr.ErrorCode == PDH_CSTATUS_INVALID_DATA ||
		pdhErr.ErrorCode == PDH_NO_DATA) {
		return true
	}
	return false
}

func init() {
	inputs.Add("win_perf_counters", func() telegraf.Input {
		return &WinPerfCounters{CountersRefreshInterval: internal.Duration{Duration: time.Second * 60}, queryCreator: &PerformanceQueryCreatorImpl{}}
	})
}
