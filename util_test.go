package goclickzetta

import (
	"fmt"
	"testing"
	"time"
)

func TestTimeFormat(t *testing.T) {
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.999999999"))
}
