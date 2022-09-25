package cpu_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/stretchr/testify/assert"
)

func TestCpuNum(t *testing.T) {
	cpu.SetCpuNum(10)
	cpu.SetCpuNum(-1)
	assert.Equal(t, 10, cpu.GetCpuNum())
}
