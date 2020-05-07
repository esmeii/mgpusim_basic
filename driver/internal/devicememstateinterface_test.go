package internal

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Implementation of regular deviceMemoryState", func() {

	regularDMS := newDeviceRegularMemoryState()

	BeforeEach(func() {
		regularDMS.setStorageSize(0x1_0000_0000)
		regularDMS.setInitialAddress(0x0_0000_1000)
		rDMS := regularDMS.(*deviceMemoryStateImpl)
		rDMS.availablePAddrs = rDMS.availablePAddrs[len(rDMS.availablePAddrs):]
	})

	It("should add PAddrs to regular DMS", func() {
		regularDMS.addSinglePAddr(0x0_0000_1000)
		regularDMS.addSinglePAddr(0x0_0000_2000)
		regularDMS.addSinglePAddr(0x0_0000_3000)
		regularDMS.addSinglePAddr(0x0_0000_4000)

		rDMS := regularDMS.(*deviceMemoryStateImpl)

		Expect(rDMS.availablePAddrs).To(HaveLen(4))
	})

	It("should get next available PAddrs", func() {
		//if last test didn't work this probably won't either
		regularDMS.addSinglePAddr(0x0_0000_1000)
		regularDMS.addSinglePAddr(0x0_0000_2000)
		regularDMS.addSinglePAddr(0x0_0000_3000)
		regularDMS.addSinglePAddr(0x0_0000_4000)

		addr1 := regularDMS.popNextAvailablePAddrs()
		addr2 := regularDMS.popNextAvailablePAddrs()

		Expect(addr1).To(Equal(uint64(0x0_0000_1000)))
		Expect(addr2).To(Equal(uint64(0x0_0000_2000)))
		rDMS := regularDMS.(*deviceMemoryStateImpl)
		Expect(rDMS.availablePAddrs).To(HaveLen(2))
	})

	It("should have no available PAddrs", func() {
		ok := regularDMS.noAvailablePAddrs()
		Expect(ok).To(BeTrue())

		regularDMS.addSinglePAddr(0x0_0000_1000)
		ok = regularDMS.noAvailablePAddrs()
		Expect(ok).To(BeFalse())
	})

})