package orcraft_test

import (
	"os"
	"testing"

	"github.com/proxysql/golib/log"
)

func TestMain(m *testing.M) {
	// the FSM logs every applied entry at DEBUG and every rejected one at ERROR;
	// the chaos tests produce thousands
	log.SetLevel(log.CRITICAL)
	os.Exit(m.Run())
}
