package goclickzetta

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"sync"
	"time"
)

// requestIDGenerator generates unique request IDs using a snowflake-like algorithm.
// ID format: 30 characters
//   - 17 characters timestamp: yyyyMMddHHmmssSSS
//   - 13 characters workerId + sequenceId (base36 encoded)
//
// example: 202212072020530531233ta0pw9gu
type requestIDGenerator struct {
	mu            sync.Mutex
	workerId      int64
	sequence      int64
	lastTimestamp int64
	location      *time.Location
}

const (
	sequenceBits      = 12
	workerIdShift     = sequenceBits
	workerIdBits      = 48
	sequenceMask      = int64(-1) ^ (int64(-1) << sequenceBits)       // 4095
	sequenceBeginMask = int64(-1) ^ (int64(-1) << (sequenceBits - 1)) // 2047
	workerIdMask      = int64(-1) ^ (int64(-1) << workerIdBits)
)

var (
	generatorOnce   sync.Once
	globalGenerator *requestIDGenerator
)

func getRequestIDGenerator() *requestIDGenerator {
	generatorOnce.Do(func() {
		wId := getMACAddressAsInt64()
		loc, err := time.LoadLocation("Asia/Shanghai")
		if err != nil {
			loc = time.FixedZone("CST", 8*3600)
		}
		globalGenerator = &requestIDGenerator{
			workerId:      wId & workerIdMask,
			lastTimestamp: -1,
			location:      loc,
		}
	})
	return globalGenerator
}

// getMACAddressAsInt64 returns the MAC address as an int64 value.
// Falls back to a random value if no valid MAC address is found.
func getMACAddressAsInt64() int64 {
	interfaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range interfaces {
			hw := iface.HardwareAddr
			if len(hw) == 6 {
				mac := fmt.Sprintf("%02x%02x%02x%02x%02x%02x", hw[0], hw[1], hw[2], hw[3], hw[4], hw[5])
				if mac != "000000000000" && mac != "ffffffffffff" {
					val, err := strconv.ParseInt(mac, 16, 64)
					if err == nil {
						return val
					}
				}
			}
		}
	}
	// fallback to random
	n, _ := rand.Int(rand.Reader, big.NewInt(1<<48))
	return n.Int64()
}

func (g *requestIDGenerator) generate() string {
	g.mu.Lock()
	defer g.mu.Unlock()

	timestamp := time.Now().UnixMilli()

	if timestamp < g.lastTimestamp {
		panic(fmt.Sprintf("clock moved backwards, refusing to generate id for %d milliseconds",
			g.lastTimestamp-timestamp))
	}

	if g.lastTimestamp == timestamp {
		g.sequence = (g.sequence + 1) & sequenceMask
		if g.sequence == 0 {
			// sequence exhausted, wait for next millisecond
			for timestamp <= g.lastTimestamp {
				timestamp = time.Now().UnixMilli()
			}
		}
	} else {
		// new millisecond, randomize starting sequence
		n, _ := rand.Int(rand.Reader, big.NewInt(sequenceBeginMask+1))
		g.sequence = n.Int64() & sequenceBeginMask
	}

	g.lastTimestamp = timestamp

	lon := (g.workerId << workerIdShift) | g.sequence

	t := time.UnixMilli(timestamp).In(g.location)
	timeStr := t.Format("20060102150405") + fmt.Sprintf("%03d", t.Nanosecond()/1e6)

	return timeStr + strconv.FormatInt(lon, 36)
}
