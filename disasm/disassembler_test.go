package disasm_test

import (
	"bufio"
	"bytes"
	"debug/elf"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gitlab.com/yaotsu/gcn3/disasm"
)

func TestDisassembler(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "GCN3 Disassembler")
}

var _ = Describe("Disassembler", func() {
	It("should disassemble kernel 1", func() {
		var buf bytes.Buffer

		elfFile, err := elf.Open("../test_data/disasm/kernels.hsaco")
		defer elfFile.Close()
		Expect(err).To(BeNil())

		targetFile, err := os.Open("..//test_data/disasm/kernel.s")
		Expect(err).To(BeNil())
		defer targetFile.Close()

		disasm := disasm.NewDisassembler()

		disasm.Disassemble(elfFile, &buf)

		resultScanner := bufio.NewScanner(&buf)
		targetScanner := bufio.NewScanner(targetFile)
		for targetScanner.Scan() {
			Expect(resultScanner.Scan()).To(Equal(true))
			Expect(resultScanner.Text()).To(Equal(targetScanner.Text()))
		}

	})
})
