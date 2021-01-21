package infra

import (
	"fmt"
	"time"

	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type Observers struct {
	workers []*Observer
}

type Observer struct {
	Address string
	d       peer.Deliver_DeliverFilteredClient
	logger  *log.Logger
}

func CreateObservers(channel string, nodes []Node, crypto *Crypto, logger *log.Logger) (*Observers, error) {
	var workers []*Observer
	for _, node := range nodes {
		worker, err := CreateObserver(channel, node, crypto, logger)
		if err != nil {
			return nil, err
		}
		workers = append(workers, worker)
	}
	return &Observers{workers: workers}, nil
}

func (o *Observers) Start(N int, errorCh chan error, finishCh chan struct{}, now time.Time, blockCollector *BlockCollector) {
	for i := 0; i < len(o.workers); i++ {
		go o.workers[i].Start(N, errorCh, finishCh, now, blockCollector)
	}
}

func CreateObserver(channel string, node Node, crypto *Crypto, logger *log.Logger) (*Observer, error) {
	deliverer, err := CreateDeliverFilteredClient(node, logger)
	if err != nil {
		return nil, err
	}

	seek, err := CreateSignedDeliverNewestEnv(channel, crypto)
	if err != nil {
		return nil, err
	}

	if err = deliverer.Send(seek); err != nil {
		return nil, err
	}

	// drain first response
	if _, err = deliverer.Recv(); err != nil {
		return nil, err
	}

	return &Observer{Address: node.Addr, d: deliverer, logger: logger}, nil
}

func (o *Observer) Start(N int, errorCh chan error, finishCh chan struct{}, now time.Time, blockCollector *BlockCollector) {
	defer close(finishCh)
	o.logger.Infoln("start observer")
	n := 0
	for n < N {
		r, err := o.d.Recv()
		if err != nil {
			errorCh <- err
		}

		if r == nil {
			errorCh <- errors.Errorf("received nil message, but expect a valid block instead. You could look into your peer logs for more info")
			return
		}

		fb := r.Type.(*peer.DeliverResponse_FilteredBlock)
		o.logger.Debugf("receivedTime %8.2fs\tBlock %6d\tTx %6d\t Address %s\n", time.Since(now).Seconds(), fb.FilteredBlock.Number, len(fb.FilteredBlock.FilteredTransactions), o.Address)

		if blockCollector.Commit(fb.FilteredBlock.Number) {
			// committed
			fmt.Printf("Time %8.2fs\tBlock %6d\tTx %6d\t \n", time.Since(now).Seconds(), fb.FilteredBlock.Number, len(fb.FilteredBlock.FilteredTransactions))
		}

		n = n + len(fb.FilteredBlock.FilteredTransactions)
	}
}

type EndorserObserver struct {
	p chan *Elements
	n int
	//	lock   sync.Mutex
	logger *log.Logger
}

func CreateEndorserObserver(processed chan *Elements, logger *log.Logger) *EndorserObserver {
	return &EndorserObserver{p: processed, n: 0, logger: logger}
}

func (o *EndorserObserver) Start(N int, finishCh chan struct{}, now time.Time) {
	defer close(finishCh)
	o.logger.Infoln("start observer")
	//	o.lock.Lock()
	for o.n < N {
		select {
		case e := <-o.p:
			o.n = o.n + 1
			o.logger.Debugln(e.Proposal.GetHeader())
			o.logger.Debugf("Time %8.2fs\tTx %6d Processed\n", time.Since(now).Seconds(), o.n)
		}
	}
	//	o.lock.Unlock()
	//	o.signal = true
}

type OrdererObserver struct {
	d      orderer.AtomicBroadcast_DeliverClient
	logger *log.Logger
	signal chan error
}

func CreateOrdererObserver(channel string, node Node, crypto *Crypto, logger *log.Logger) *OrdererObserver {

	if len(node.Addr) == 0 {
		return nil
	}
	deliverer, err := CreateOrdererDeliverClient(node)
	if err != nil {
		panic(err)
	}

	seek, err := CreateSignedDeliverNewestEnv(channel, crypto)
	if err != nil {
		panic(err)
	}

	if err = deliverer.Send(seek); err != nil {
		panic(err)
	}

	// drain first response
	if _, err = deliverer.Recv(); err != nil {
		panic(err)
	}

	return &OrdererObserver{d: deliverer, signal: make(chan error, 10), logger: logger}
}

func (o *OrdererObserver) Start(N int, errorCh chan error, finishCh chan struct{}, now time.Time) {
	//	defer close(o.signal)
	defer close(finishCh)
	o.logger.Debugf("start observer")
	i := 0
	n := 0
	for n < N {
		r, err := o.d.Recv()
		if err != nil {
			//			o.sigal <- err
			errorCh <- err
		}
		if r == nil {
			panic("Received nil message, but expect a valid block instead. You could look into your peer logs for more info")
		}
		tx := len(r.GetBlock().Data.Data)
		i++
		n += tx
		fmt.Printf("Time %8.2fs\tBlock %6d\t Tx %6d\n", time.Since(now).Seconds(), i, tx)
	}
}
