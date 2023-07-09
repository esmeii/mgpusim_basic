// Package rdma provides the implementation of an RDMA engine.
package rdma

import (
	"fmt"
	"log"
	"os"
	"reflect"

	"gitlab.com/akita/akita/v3/sim"
	"gitlab.com/akita/akita/v3/tracing"
	"gitlab.com/akita/mem/v3/mem"
)

type transaction struct {
	fromInside  sim.Msg
	fromOutside sim.Msg
	toInside    sim.Msg
	toOutside   sim.Msg
	sendTime    sim.VTimeInSec
	recvTime    sim.VTimeInSec //230706
	addr        uint64
}

// An Engine is a component that helps one GPU to access the memory on
// another GPU
type Engine struct {
	*sim.TickingComponent

	ToOutside sim.Port

	ToL1 sim.Port
	ToL2 sim.Port

	CtrlPort sim.Port

	isDraining              bool
	pauseIncomingReqsFromL1 bool
	currentDrainReq         *DrainReq

	localModules           mem.LowModuleFinder
	RemoteRDMAAddressTable mem.LowModuleFinder

	transactionsFromOutside []transaction
	transactionsFromInside  []transaction

	//ES
	transactionsTotal []transaction

	responseQingDelay sim.VTimeInSec //230706
	l2Latency         sim.VTimeInSec
	requestQingDelay  sim.VTimeInSec
	totalLatency      sim.VTimeInSec

	srcUsageCount map[string]int
	dstUsageCount map[string]int
}

// SetLocalModuleFinder sets the table to lookup for local data.
func (e *Engine) SetLocalModuleFinder(lmf mem.LowModuleFinder) {
	e.localModules = lmf
}

// Tick checks if make progress
func (e *Engine) Tick(now sim.VTimeInSec) bool {
	madeProgress := false

	madeProgress = e.processFromCtrlPort(now) || madeProgress
	if e.isDraining {
		madeProgress = e.drainRDMA(now) || madeProgress
	}
	madeProgress = e.processFromL1(now) || madeProgress
	madeProgress = e.processFromL2(now) || madeProgress
	madeProgress = e.processFromOutside(now) || madeProgress

	return madeProgress
}
func (e *Engine) writeFileResolvedRequest(rsp mem.AccessRsp) {
	f, err := os.OpenFile("./rdma.log", os.O_APPEND|os.O_RDWR, 0755)
	if err != nil {
		// Handle the error, such as creating the file if it doesn't exist
		if os.IsNotExist(err) {
			f, err = os.Create("rdma.log")
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	}
	defer f.Close()
	fmt.Fprintf(f, "##########################[Resolved Transaction]########################\n")
	fmt.Fprintf(f, "[Src] = %s\t[Dst] = %s\t [Total Epoch] = %f\n", rsp.Meta().Dst.Name(), rsp.Meta().Src.Name(), e.totalLatency)
}
func (e *Engine) writeFileRdma(transactions []transaction) {
	f, err := os.OpenFile("./rdma.log", os.O_APPEND|os.O_RDWR, 0755)
	if err != nil {
		// Handle the error, such as creating the file if it doesn't exist
		if os.IsNotExist(err) {
			f, err = os.Create("rdma.log")
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	}
	defer f.Close()
	fmt.Fprintf(f, "+++++++++++++++++++++[Unresolved Transaction]++++++++++++++++\n")
	for _, trans := range transactions {
		fmt.Fprintf(f, "[Send Time] = %5f\t[SRC] = %20s\t[DST] = %20s\t[Addr] = %5u\t[ID] = %10s\n", trans.addr, trans.fromInside.Meta().Src.Name(), trans.fromInside.Meta().Dst.Name(), trans.addr, trans.fromInside.Meta().ID) //float
	}
}

//ES
func (e *Engine) writeFileGeneratedReq(req mem.AccessReq) {
	f, err := os.OpenFile("./rdma.log", os.O_APPEND|os.O_RDWR, 0755)
	if err != nil {
		// Handle the error, such as creating the file if it doesn't exist
		if os.IsNotExist(err) {
			f, err = os.Create("rdma.log")
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	}

	defer f.Close()
	fmt.Fprintf(f, "************************Generated Transaction [In]*************************")
	fmt.Fprintf(f, "[Send Time] = %5f\t[SRC] = %20s\t[DST] = %20s\t[Addr] = %5u\t[Byte Size] = %5u\t[ID] = %10s\n", req.GetAddress(), req.GetByteSize(), req.Meta().ID, req.Meta().Src.Name(), req.Meta().Dst.Name(), req.Meta().SendTime) //float

}
func (e *Engine) CountDuplicates(dstGPU []string) map[string]int {
	counts := make(map[string]int)

	for _, str := range dstGPU {
		counts[str]++
	}

	return counts
}

func (e *Engine) processFromCtrlPort(now sim.VTimeInSec) bool {
	req := e.CtrlPort.Peek()
	if req == nil {
		return false
	}

	req = e.CtrlPort.Retrieve(now)
	switch req := req.(type) {
	case *DrainReq:
		e.currentDrainReq = req
		e.isDraining = true
		e.pauseIncomingReqsFromL1 = true
		return true
	case *RestartReq:
		return e.processRDMARestartReq(now)
	default:
		log.Panicf("cannot process request of type %s", reflect.TypeOf(req))
		return false
	}
}

func (e *Engine) processRDMARestartReq(now sim.VTimeInSec) bool {
	restartCompleteRsp := RestartRspBuilder{}.
		WithSendTime(now).
		WithSrc(e.CtrlPort).
		WithDst(e.currentDrainReq.Src).
		Build()
	err := e.CtrlPort.Send(restartCompleteRsp)

	if err != nil {
		return false
	}
	e.currentDrainReq = nil
	e.pauseIncomingReqsFromL1 = false

	return true
}

func (e *Engine) drainRDMA(now sim.VTimeInSec) bool {
	if e.fullyDrained() {
		drainCompleteRsp := DrainRspBuilder{}.
			WithSendTime(now).
			WithSrc(e.CtrlPort).
			WithDst(e.currentDrainReq.Src).
			Build()

		err := e.CtrlPort.Send(drainCompleteRsp)
		if err != nil {
			return false
		}
		e.isDraining = false
		return true
	}
	return false
}

func (e *Engine) fullyDrained() bool {
	return len(e.transactionsFromOutside) == 0 &&
		len(e.transactionsFromInside) == 0
}

func (e *Engine) processFromL1(now sim.VTimeInSec) bool {
	if e.pauseIncomingReqsFromL1 {
		return false
	}

	req := e.ToL1.Peek()
	if req == nil {
		return false
	}

	switch req := req.(type) {
	case mem.AccessReq:
		return e.processReqFromL1(now, req)
	default:
		log.Panicf("cannot process request of type %s", reflect.TypeOf(req))
		return false
	}
}

func (e *Engine) processFromL2(now sim.VTimeInSec) bool {
	req := e.ToL2.Peek()
	if req == nil {
		return false
	}

	switch req := req.(type) {
	case mem.AccessRsp:
		return e.processRspFromL2(now, req)
	default:
		panic("unknown req type")
	}
}

func (e *Engine) processFromOutside(now sim.VTimeInSec) bool {
	req := e.ToOutside.Peek()
	if req == nil {
		return false
	}

	switch req := req.(type) {
	case mem.AccessReq:
		return e.processReqFromOutside(now, req)
	case mem.AccessRsp:
		return e.processRspFromOutside(now, req)
	default:
		log.Panicf("cannot process request of type %s", reflect.TypeOf(req))
		return false
	}
}

//ES : 230706
func (e *Engine) countRequestUsage(transactions []transaction) {
	// Iterate over transactions
	for _, trans := range transactions {
		src := trans.fromInside.Meta().Src.Name()
		dst := trans.toOutside.Meta().Dst.Name()
		// Update src count
		e.srcUsageCount[src]++
		// Update dst count
		e.dstUsageCount[dst]++
	}

	// Print src count
	fmt.Println("Src occurrences:")
	for src, count := range e.srcUsageCount {
		fmt.Printf("%s: %d\n", src, count)
	}

	// Print dst count
	fmt.Println("Dst occurrences:")
	for dst, count := range e.dstUsageCount {
		fmt.Printf("%s: %d\n", dst, count)
	}
}

func (e *Engine) processReqFromL1(
	now sim.VTimeInSec,
	req mem.AccessReq,
) bool {
	dst := e.RemoteRDMAAddressTable.Find(req.GetAddress())

	if dst == e.ToOutside {
		panic("RDMA loop back detected")
	}

	cloned := e.cloneReq(req)
	cloned.Meta().Src = e.ToOutside
	cloned.Meta().Dst = dst
	cloned.Meta().SendTime = now
	e.requestQingDelay = now        //230706
	e.writeFileGeneratedReq(cloned) //230709
	err := e.ToOutside.Send(cloned)
	if err == nil {
		e.ToL1.Retrieve(now)

		tracing.TraceReqReceive(req, e)
		tracing.TraceReqInitiate(cloned, e, tracing.MsgIDAtReceiver(req, e))
		trans := transaction{
			fromInside: req,
			toOutside:  cloned,
			addr:       req.GetAddress(),
			sendTime:   now,
		}

		e.transactionsFromInside = append(e.transactionsFromInside, trans)
		e.transactionsTotal = append(e.transactionsTotal, trans)
		//ES
		e.writeFileRdma(e.transactionsTotal)
		e.countRequestUsage(e.transactionsTotal)
		return true
	}

	return false
}

func (e *Engine) processReqFromOutside(
	now sim.VTimeInSec,
	req mem.AccessReq,
) bool {
	dst := e.localModules.Find(req.GetAddress())

	cloned := e.cloneReq(req)
	cloned.Meta().Src = e.ToL2
	cloned.Meta().Dst = dst
	cloned.Meta().SendTime = now
	e.requestQingDelay = now - e.requestQingDelay //230706
	//
	f, err_file := os.OpenFile("./rdmaLatency.log", os.O_APPEND|os.O_RDWR, 0755)
	if err_file != nil {
		// Handle the error, such as creating the file if it doesn't exist
		if os.IsNotExist(err_file) {
			f, err_file = os.Create("rdma.log")
			if err_file != nil {
				log.Fatal(err_file)
			}
		} else {
			log.Fatal(err_file)
		}
	}

	defer f.Close()
	fmt.Fprintf(f, "[ID]= %s\t[src]= %s\t[dst]= %s\t[1 and 2]= %f\n", cloned.Meta().ID, cloned.Meta().Src.Name(), cloned.Meta().Dst.Name(), e.requestQingDelay)
	//
	err := e.ToL2.Send(cloned)
	if err == nil {
		e.ToOutside.Retrieve(now)

		tracing.TraceReqReceive(req, e)
		tracing.TraceReqInitiate(cloned, e, tracing.MsgIDAtReceiver(req, e))

		trans := transaction{
			fromOutside: req,
			toInside:    cloned,
		}

		e.transactionsFromOutside =
			append(e.transactionsFromOutside, trans)

		e.writeFileRdma(e.transactionsFromOutside)
		return true
	}
	return false
}

func (e *Engine) processRspFromL2(
	now sim.VTimeInSec,
	rsp mem.AccessRsp,
) bool {
	transactionIndex := e.findTransactionByRspToID(
		rsp.GetRspTo(), e.transactionsFromOutside)
	trans := e.transactionsFromOutside[transactionIndex]

	rspToOutside := e.cloneRsp(rsp, trans.fromOutside.Meta().ID)
	rspToOutside.Meta().SendTime = now
	rspToOutside.Meta().Src = e.ToOutside
	rspToOutside.Meta().Dst = trans.fromOutside.Meta().Src
	e.l2Latency = now //230706
	f, err_file := os.OpenFile("./rdma.log", os.O_APPEND|os.O_RDWR, 0755)
	if err_file != nil {
		// Handle the error, such as creating the file if it doesn't exist
		if os.IsNotExist(err_file) {
			f, err_file = os.Create("rdma.log")
			if err_file != nil {
				log.Fatal(err_file)
			}
		} else {
			log.Fatal(err_file)
		}
	}

	defer f.Close()
	fmt.Fprintf(f, "[ID]= %s\t[src]= %s\t[dst]= %s\t[Rsp generated in L2]= %f\n", rspToOutside.Meta().ID, rspToOutside.Meta().Dst.Name(), rspToOutside.Meta().Src.Name(), now)
	//
	err := e.ToOutside.Send(rspToOutside)
	if err == nil {
		e.ToL2.Retrieve(now)

		tracing.TraceReqFinalize(trans.toInside, e)
		tracing.TraceReqComplete(trans.fromOutside, e)

		e.transactionsFromOutside =
			append(e.transactionsFromOutside[:transactionIndex],
				e.transactionsFromOutside[transactionIndex+1:]...)

		e.writeFileRdma(e.transactionsFromOutside)
		return true
	}
	return false
}

func (e *Engine) processRspFromOutside(
	now sim.VTimeInSec,
	rsp mem.AccessRsp,
) bool {
	transactionIndex := e.findTransactionByRspToID(
		rsp.GetRspTo(), e.transactionsFromInside)
	trans := e.transactionsFromInside[transactionIndex]
	//trans.fromInside.Meta().Src.Name(), trans.toOutside.Meta().Dst.Name(), trans.addr
	rspToInside := e.cloneRsp(rsp, trans.fromInside.Meta().ID)
	rspToInside.Meta().SendTime = now
	rspToInside.Meta().Src = e.ToL1
	rspToInside.Meta().Dst = trans.fromInside.Meta().Src
	trans.recvTime = now                             //230706
	e.totalLatency = trans.recvTime - trans.sendTime //230706
	e.l2Latency = now - e.l2Latency                  //230706
	f, err_file := os.OpenFile("./rdmaLatency.log", os.O_APPEND|os.O_RDWR, 0755)
	if err_file != nil {
		// Handle the error, such as creating the file if it doesn't exist
		if os.IsNotExist(err_file) {
			f, err_file = os.Create("rdmaLatency.log")
			if err_file != nil {
				log.Fatal(err_file)
			}
		} else {
			log.Fatal(err_file)
		}
	}
	defer f.Close()
	fmt.Fprintf(f, "[ID]= %s\t[src]= %s\t[dst]= %s\t[L2 Latency]= %f\n", rspToInside.Meta().ID, rspToInside.Meta().Dst.Name(), rspToInside.Meta().Src.Name(), e.l2Latency)
	//
	err := e.ToL1.Send(rspToInside)

	if err == nil {
		e.ToOutside.Retrieve(now)

		tracing.TraceReqFinalize(trans.toOutside, e)
		tracing.TraceReqComplete(trans.fromInside, e)

		e.transactionsFromInside =
			append(e.transactionsFromInside[:transactionIndex],
				e.transactionsFromInside[transactionIndex+1:]...)
		e.writeFileResolvedRequest(rspToInside)
		return true
	}

	return false
}

func (e *Engine) findTransactionByRspToID(
	rspTo string,
	transactions []transaction,
) int {
	for i, trans := range transactions {
		if trans.toOutside != nil && trans.toOutside.Meta().ID == rspTo {
			return i
		}

		if trans.toInside != nil && trans.toInside.Meta().ID == rspTo {
			return i
		}
	}

	log.Panicf("transaction %s not found", rspTo)
	return 0
}

func (e *Engine) cloneReq(origin mem.AccessReq) mem.AccessReq {
	switch origin := origin.(type) {
	case *mem.ReadReq:
		read := mem.ReadReqBuilder{}.
			WithSendTime(origin.SendTime).
			WithSrc(origin.Src).
			WithDst(origin.Dst).
			WithAddress(origin.Address).
			WithByteSize(origin.AccessByteSize).
			Build()
		return read
	case *mem.WriteReq:
		write := mem.WriteReqBuilder{}.
			WithSendTime(origin.SendTime).
			WithSrc(origin.Src).
			WithDst(origin.Dst).
			WithAddress(origin.Address).
			WithData(origin.Data).
			WithDirtyMask(origin.DirtyMask).
			Build()
		return write
	default:
		log.Panicf("cannot clone request of type %s",
			reflect.TypeOf(origin))
	}
	return nil
}

func (e *Engine) cloneRsp(origin mem.AccessRsp, rspTo string) mem.AccessRsp {
	switch origin := origin.(type) {
	case *mem.DataReadyRsp:
		rsp := mem.DataReadyRspBuilder{}.
			WithSendTime(origin.SendTime).
			WithSrc(origin.Src).
			WithDst(origin.Dst).
			WithRspTo(rspTo).
			WithData(origin.Data).
			Build()
		return rsp
	case *mem.WriteDoneRsp:
		rsp := mem.WriteDoneRspBuilder{}.
			WithSendTime(origin.SendTime).
			WithSrc(origin.Src).
			WithDst(origin.Dst).
			WithRspTo(rspTo).
			Build()
		return rsp
	default:
		log.Panicf("cannot clone request of type %s",
			reflect.TypeOf(origin))
	}
	return nil
}

// SetFreq sets freq
func (e *Engine) SetFreq(freq sim.Freq) {
	e.TickingComponent.Freq = freq
}

// NewEngine creates new engine
func NewEngine(
	name string,
	engine sim.Engine,
	localModules mem.LowModuleFinder,
	remoteModules mem.LowModuleFinder,
) *Engine {
	e := new(Engine)
	e.TickingComponent = sim.NewTickingComponent(name, engine, 1*sim.GHz, e)
	e.localModules = localModules
	e.RemoteRDMAAddressTable = remoteModules

	e.ToL1 = sim.NewLimitNumMsgPort(e, 1, name+".ToL1")
	e.ToL2 = sim.NewLimitNumMsgPort(e, 1, name+".ToL2")
	e.CtrlPort = sim.NewLimitNumMsgPort(e, 1, name+".CtrlPort")
	e.ToOutside = sim.NewLimitNumMsgPort(e, 1, name+".ToOutside")

	return e
}
