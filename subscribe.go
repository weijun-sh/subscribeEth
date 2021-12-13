package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	golog "log"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/weijun-sh/subscribeEth/mongodb"
)

type Config struct {
	GateWay *gatewayConfig
	MongoDB *MongoDBConfig
	BlockChain *blockChainConfig
	SwapPost *swappostConfig
}

type gatewayConfig struct {
	Endpoint string
}

type blockChainConfig struct {
	Chain string
	ChainID string
	SyncNumber uint64
}

type swappostConfig struct {
	SwapinTokens  []string
	SwapoutTokens []string
	SwapRouter []string
}

// MongoDBConfig mongodb config
type MongoDBConfig struct {
        DBURL      string
        DBName     string
        UserName   string `json:"-"`
        Password   string `json:"-"`
        Enable     bool
}

type swapPost struct {
        txid       string
        pairID     string
        rpcMethod  string
        swapServer string
}

var s struct {
	FOO struct {
		Usernames_Passwords map[string]string
	}
}

var (
	start int64
	end int64
	configFile string
	verbosity int
	logfilepath string
	swapin bool
	swapout bool

	chain string
	chainID string
	mongodbEnable bool
	mongodbConfig = &MongoDBConfig{}
	clientRpc *ethclient.Client

	exitChan chan bool = make(chan bool)
)

var (
	SwapoutTopic       common.Hash = common.HexToHash("0x6b616089d04950dc06c45c6dd787d657980543f89651aec47924752c7d16c888")
	BTCSwapoutTopic    common.Hash = common.HexToHash("0x9c92ad817e5474d30a4378deface765150479363a897b0590fbb12ae9d89396b")
	ERC20TransferTopic common.Hash = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	RouterAnycallTopic common.Hash = common.HexToHash("0x3d1b3d059223895589208a5541dce543eab6d5942b3b1129231a942d1c47bc45")
)

func init() {
	flag.Int64Var(&start, "start", 0, "start")
	flag.Int64Var(&end, "end", 0, "end")
	flag.StringVar(&configFile, "config", "./config.toml", "config")
	flag.StringVar(&logfilepath, "log", "", "log")
	flag.BoolVar(&swapin, "swapin", false, "listen swapin")
	flag.BoolVar(&swapout, "swapout", false, "listen swapout")
	flag.IntVar(&verbosity, "verbosity", 3, "verbosity")
}

func initClient(config *Config) {
	var err error
        clientRpc, err = ethclient.Dial(config.GateWay.Endpoint)
        if err != nil {
                golog.Fatal("ethclient.Dial failed", "gateway", config.GateWay.Endpoint, "err", err)
        }
        log.Info("ethclient.Dial gateway success", "gateway", config.GateWay.Endpoint)
}

func LoadConfig() *Config {
	config := &Config{}
	if _, err := toml.DecodeFile(configFile, &config); err != nil {
		panic(err)
	}
	mongodbConfig = config.MongoDB
	chain = config.BlockChain.Chain
	chainID = config.BlockChain.ChainID
	mongodbEnable = mongodbConfig.Enable
        if mongodbEnable {
                InitMongodb()
                go loopSwapPending()
        }
	err := checkConfig(config)
	if err != nil {
		log.Crit("LoadConfig", "checkConfig err", err)
	}
	log.Info("Load config success", "config", config)
	return config
}

// InitMongodb init mongodb by config
func InitMongodb() {
        log.Info("InitMongodb")
        dbConfig := GetMongodbConfig()
        mongodb.MongoServerInit([]string{dbConfig.DBURL}, dbConfig.DBName, dbConfig.UserName, dbConfig.Password)
}

func checkConfig(config *Config) error {
	if config.GateWay.Endpoint == "" {
		return errors.New("rpc Endpoint is null")
	}
	return nil
}

// GetMongodbConfig get mongodb config
func GetMongodbConfig() *MongoDBConfig {
        return mongodbConfig
}

func main() {
	flag.Parse()

	if logfilepath != "" {
		handler, err := log.FileHandler(logfilepath, log.JSONFormatEx(false, true))
		if err != nil {
			panic(err)
		}
		glogger := log.NewGlogHandler(handler)
		glogger.Verbosity(log.Lvl(verbosity))
		log.Root().SetHandler(glogger)
	} else {
		glogger := log.NewGlogHandler(log.StreamHandler(os.Stdout, log.TerminalFormat(true)))
		glogger.Verbosity(log.Lvl(verbosity))
		log.Root().SetHandler(glogger)
	}

	config := LoadConfig()
	initClient(config)

	go StartSubscribeHeader(config)
	if swapout {
		go StartSubscribeSwapout(config)
	} else {
		if swapin {
			go StartSubscribeSwapin(config)
		} else {
			go StartSubscribeSwapin(config)
			go StartSubscribeSwapout(config)
			go StartSubscribeRouter(config)
		}
	}
	<-exitChan
	fmt.Println("Exit")

}

func StartSubscribeHeader(config *Config) {
	ctx := context.Background()
	var endpoint string = config.GateWay.Endpoint

	client, err := ethclient.DialContext(ctx, endpoint)
	if err != nil {
		panic(err)
	}

	headCh := make(chan *types.Header, 128)
	defer close(headCh)

	sub := LoopSubscribeHead(client, ctx, headCh)
	defer sub.Unsubscribe()

	for {
		select {
		case msg := <-headCh:
			log.Info("Get new header", "head", msg.Number) // print block number
		case err := <-sub.Err():
			log.Warn("Subscribe header error", "error", err)
			sub.Unsubscribe()
			sub = LoopSubscribeHead(client, ctx, headCh)
		}
	}
}

func StartSubscribeSwapout(config *Config) {
	log.Info("StartSubscribeSwapout")
	if len(config.SwapPost.SwapoutTokens) == 0 {
		fmt.Printf("StartSubscribeSwapout exit.\n")
		return
	}
	var endpoint string = config.GateWay.Endpoint

	ctx := context.Background()
	client, err := ethclient.DialContext(ctx, endpoint)
	if err != nil {
		panic(err)
	}

	var addreeePairIDMap = make(map[common.Address]string)
	var serverMap = make(map[common.Address]string)
	var tokenAddresses = make([]common.Address, 0)

	topics := make([][]common.Hash, 0)
	topics = append(topics, []common.Hash{SwapoutTopic, BTCSwapoutTopic}) // SwapoutTopic or BTCSwapoutTopic

	for _, item := range config.SwapPost.SwapoutTokens {
		pairID := strings.Split(item, ",")[0]
		addr := common.HexToAddress(strings.Split(item, ",")[1])
		server := strings.Split(item, ",")[2]

		addreeePairIDMap[addr] = pairID
		serverMap[addr] = server
		tokenAddresses = append(tokenAddresses, addr)
		fmt.Printf("address: %v, pairID: %v, server: %v\n", addr, addreeePairIDMap[addr], serverMap[addr])
	}

	swapoutfq := ethereum.FilterQuery{
		Addresses: tokenAddresses,
		Topics:    topics,
	}

	if start > 0 {
		swapoutfq.FromBlock = big.NewInt(start)
	}
	if end > 0 {
		swapoutfq.ToBlock = big.NewInt(end)
	}

	log.Info("swapout fq", "swapoutfq", swapoutfq)

	ch := make(chan types.Log, 128)
	defer close(ch)

	sub := LoopSubscribe(client, ctx, swapoutfq, ch)
       defer sub.Unsubscribe()

	// subscribe swapout
	for {
		select {
		case msg := <-ch:
			log.Info("Find event", "event", msg)
			txhash := msg.TxHash.String()
			pairID := addreeePairIDMap[msg.Address]
			server := serverMap[msg.Address]
			log.Info("Swapout", "txhash", txhash, "msg.Address", msg.Address, "pairID", pairID, "server", server)
			swap := &swapPost{
			        txid:       txhash,
			        pairID:     pairID,
			        rpcMethod:  "swap.Swapout",
			        swapServer: server,
			}

			swaperr := DoSwap(txhash, pairID, "swap.Swapout", server)
			if mongodbEnable {
				if swaperr != nil {
					addMongodbSwapPendingPost(swap)
				} else {
					addMongodbSwapPost(swap)
				}
			}
		case err := <-sub.Err():
			log.Info("Subscribe swapout error", "error", err)
			sub.Unsubscribe()
			sub = LoopSubscribe(client, ctx, swapoutfq, ch)
		}
	}
}

func StartSubscribeSwapin(config *Config) {
	log.Info("StartSubscribeSwapin")
	if len(config.SwapPost.SwapinTokens) == 0 {
		fmt.Printf("StartSubscribeSwapin exit.\n")
		return
	}
	var endpoint string = config.GateWay.Endpoint

	ctx := context.Background()
	client, err := ethclient.DialContext(ctx, endpoint)
	if err != nil {
		panic(err)
	}

	var addreeePairIDMap = make(map[common.Address]string)
	var serverMap = make(map[common.Address]string)
	var ETHDepositAddress = common.Address{}
	// erc20
	var addreeePairIDMapErc20 = make(map[common.Address]map[common.Address]string)
	var depositAddressMap = make(map[common.Address][]common.Address)
	var serverMapErc20 = make(map[common.Address]map[common.Address]string)

	for _, item := range config.SwapPost.SwapinTokens {
		itemSlice := strings.Split(item, ",")
		tokenAddr := common.HexToAddress(itemSlice[1])
		pairID := itemSlice[0]
		server := itemSlice[3]
		depositAddr := common.HexToAddress(itemSlice[2])
		// ready for erc20
		if depositAddressMap[depositAddr] == nil {
			depositAddressMap[depositAddr] = make([]common.Address, 0)
		}
		depositAddressMap[depositAddr] = append(depositAddressMap[depositAddr], tokenAddr)
		if serverMapErc20[depositAddr] == nil {
			serverMapErc20[depositAddr] = make(map[common.Address]string)
		}
		serverMapErc20[depositAddr][tokenAddr] = server
		if addreeePairIDMapErc20[depositAddr] == nil {
			addreeePairIDMapErc20[depositAddr] = make(map[common.Address]string)
		}
		addreeePairIDMapErc20[depositAddr][tokenAddr] = pairID

		// for native
		if tokenAddr != (common.Address{}) {
			continue
		}
		addreeePairIDMap[depositAddr] = pairID
		serverMap[depositAddr] = server
		ETHDepositAddress = depositAddr

		fmt.Printf("address: %v, pairID: %v, server: %v\n", tokenAddr, addreeePairIDMap[tokenAddr], serverMap[tokenAddr])
	}

	// subscribe ETH swapin
	fq := ethereum.FilterQuery{
		Addresses: []common.Address{ETHDepositAddress},
	}

	go func() {
		ch := make(chan types.Log, 128)
		defer close(ch)

               sub := LoopSubscribe(client, ctx, fq, ch)
               defer sub.Unsubscribe()

		for {
			select {
			case msg := <-ch:
				log.Info("Find event", "event", msg)
				tx, _, err := client.TransactionByHash(ctx, msg.TxHash)
				if err == nil && *tx.To() == ETHDepositAddress {
					txhash := msg.TxHash.String()
					pairID := addreeePairIDMap[msg.Address]
					server := serverMap[msg.Address]
					log.Info("ETH swap in", "txhash", txhash, "msg.Address", msg.Address, "pairID", pairID, "server", server)
					swap := &swapPost{
					        txid:       txhash,
					        pairID:     pairID,
					        rpcMethod:  "swap.Swapin",
					        swapServer: server,
					}

					swaperr := DoSwap(txhash, pairID, "swap.Swapin", server)
					if mongodbEnable {
						if swaperr != nil {
							addMongodbSwapPendingPost(swap)
						} else {
							addMongodbSwapPost(swap)
						}
					}
				}
                       case err := <-sub.Err():
                               log.Info("Subscribe swapin error", "error", err)
                               sub.Unsubscribe()
                               sub = LoopSubscribe(client, ctx, fq, ch)
			}
		}
	}()

	// subscribe ERC20 swapin
	for depositAddr, tokens := range depositAddressMap {
		topics := make([][]common.Hash, 0)
		topics = append(topics, []common.Hash{ERC20TransferTopic}) // Log [0] is ERC20 transfer
		topics = append(topics, []common.Hash{})                   // Log [1] is arbitrary
		topics = append(topics, []common.Hash{depositAddr.Hash()}) // Log [2] is deposit address

		fq := ethereum.FilterQuery{
			Addresses: tokens,
			Topics:    topics,
		}
		if start > 0 {
			fq.FromBlock = big.NewInt(start)
		}
		if end > 0 {
			fq.ToBlock = big.NewInt(end)
		}
		log.Info("swapin fq", "depositAddr", fq)

		go func() {
			ch := make(chan types.Log, 128)
			defer close(ch)

	               sub := LoopSubscribe(client, ctx, fq, ch)
	               defer sub.Unsubscribe()

			for {
				select {
				case msg := <-ch:
					log.Info("Find event", "event", msg)
					txhash := msg.TxHash.String()
					d := msg.Topics[2].String()
					dep := common.HexToAddress(d)
					pairID := addreeePairIDMapErc20[dep][msg.Address]
					server := serverMapErc20[dep][msg.Address]
					if server == "" {
						continue
					}
					log.Info("ERC20 swap in", "txhash", txhash, "msg.Address", msg.Address, "pairID", pairID, "server", server, "depositAddr", dep)
					swap := &swapPost{
					        txid:       txhash,
					        pairID:     pairID,
					        rpcMethod:  "swap.Swapin",
					        swapServer: server,
					}

					swaperr := DoSwap(txhash, pairID, "swap.Swapin", server)
					if mongodbEnable {
						if swaperr != nil {
							addMongodbSwapPendingPost(swap)
						} else {
							addMongodbSwapPost(swap)
						}
					}
				}
			}
		}()
	}
}

func LoopSubscribeHead(client *ethclient.Client, ctx context.Context, ch chan<- *types.Header) ethereum.Subscription {
	for {
		sub, err := client.SubscribeNewHead(ctx, ch)
		if err == nil {
			return sub
		}
		log.Info("SubscribeHead failed, retry in 1 second", "error", err)
		time.Sleep(time.Second * 1)
	}
}

func FilterLogs(client *ethclient.Client, ctx context.Context, fq ethereum.FilterQuery, ch chan types.Log) {
	time.Sleep(time.Second * 10)
	if end != 0 && start >= end {
		log.Crit("FilterLogs", "start", start, "> end", end)
	}
	log.Info("FilterLogs", "start", start, "end", end)

	for i := start;; i++ {
		if end != 0 {
			if i >= end {
				break
			}
		}
		log.Info("FilterLogs", "block", i)
		fq.FromBlock = big.NewInt(i)
		fq.ToBlock = big.NewInt(i+1)
		for {
			logs, err := client.FilterLogs(ctx, fq)
			if err == nil {
				//log.Info("FilterLogs", "block", i, "logs", logs)
				for _, l := range logs {
					ch <- l
				}
				break
			}
			log.Warn("FilterLogs failed, retry in 1 second", "error", err, "block", i)
			time.Sleep(time.Second * 1)
		}
		time.Sleep(time.Second * 1)
	}
	log.Info("FilterLogs finished", "start", start, "end", end)
	exitChan <- true
}

func LoopSubscribe(client *ethclient.Client, ctx context.Context, fq ethereum.FilterQuery, ch chan types.Log) ethereum.Subscription {
	for {
		sub, err := client.SubscribeFilterLogs(ctx, fq, ch)
		if err == nil {
			log.Info("Subscribe start")
			return sub
		}
		log.Info("Subscribe logs failed, retry in 1 second", "error", err)
		time.Sleep(time.Second * 1)
	}
}

func DoSwap(txid, pairID, swapio, server string) error {
	var data = strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","method":"%v","params":[{"txid":"%v","pairid":"%v"}],"id":1}`, swapio, txid, pairID))
	return postSwap(server, data)
}

func DoSwapRouter(txid, chainID, swapio, server string) error {
	var data = strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","method":"%v","params":[{"txid":"%v","chainid":"%v"}],"id":1}`, swapio, txid, chainID))
	return postSwap(server, data)
}

func postSwap(server string, data *strings.Reader) error {
	req, err := http.NewRequest("POST", server, data)
	if err != nil {
		log.Warn("Post swap error", "data", data, "server", server)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Warn("Post swap error", "data", data, "server", server)
		return err
	}
	defer resp.Body.Close()
	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warn("Post swap error", "data", data, "server", server)
		return err
	}
	log.Info("Call swap", "response", fmt.Sprintf("%s", bodyText), "data", data, "server", server)
	return nil
}

func loopSwapPending() {
        log.Info("start SwapPending loop job")
        for {
                sp := mongodb.FindAllSwapPending(chain)
                if len(sp) == 0 {
                        time.Sleep(30 * time.Second)
                        continue
                }
                log.Info("loopSwapPending", "swap", sp, "len", len(sp))
                for i, swap := range sp {
                        log.Info("loopSwapPending", "swap", swap, "index", i)
                        sp := swapPost{}
                        sp.txid = swap.Txid
                        sp.pairID = swap.PairID
                        sp.rpcMethod = swap.RpcMethod
                        sp.swapServer = swap.SwapServer
                        rm := DoSwap(sp.txid, sp.pairID, sp.rpcMethod, sp.swapServer)
                        if rm == nil {
                                mongodb.UpdateSwapPending(swap)
                        } else {
                                r, err := loopGetTxReceipt(common.HexToHash(swap.Txid))
                                if err != nil || (err == nil && r.Status != uint64(0)) {
                                        log.Warn("loopSwapPending remove", "status", 0, "txHash", swap.Txid)
                                        mongodb.RemoveSwapPending(swap)
                                        mongodb.AddSwapDeleted(swap, false)
                                }
                        }
                }
                time.Sleep(10 * time.Second)
        }
}

func loopGetTxReceipt(txHash common.Hash) (receipt *types.Receipt, err error) {
        for i := 0; i < 5; i++ { // with retry
                receipt, err = clientRpc.TransactionReceipt(context.Background(), txHash)
                if err == nil {
                        return receipt, err
                }
                time.Sleep(1 * time.Second)
        }
        return nil, err
}

func addMongodbSwapPost(swap *swapPost) {
        ms := &mongodb.MgoSwap {
                Id: swap.txid,
                Txid: swap.txid,
                PairID: swap.pairID,
		ChainID: chainID,
                RpcMethod: swap.rpcMethod,
                SwapServer: swap.swapServer,
                Chain: chain,
                Timestamp: uint64(time.Now().Unix()),
        }
        mongodb.AddSwap(ms, false)
}

func addMongodbSwapPendingPost(swap *swapPost) {
        ms := &mongodb.MgoSwap {
                Id: swap.txid,
                Txid: swap.txid,
                PairID: swap.pairID,
		ChainID: chainID,
                RpcMethod: swap.rpcMethod,
                SwapServer: swap.swapServer,
                Chain: chain,
                Timestamp: uint64(time.Now().Unix()),
        }
        mongodb.AddSwapPending(ms, false)
}

func StartSubscribeRouter(config *Config) {
	log.Info("StartSubscribeRouter")
	if len(config.SwapPost.SwapRouter) == 0 {
		fmt.Printf("StartSubscribeRouter exit.\n")
		return
	}
	var endpoint string = config.GateWay.Endpoint

	ctx := context.Background()
	client, err := ethclient.DialContext(ctx, endpoint)
	if err != nil {
		panic(err)
	}

	var addreeePairIDMap = make(map[common.Address]string)
	var serverMap = make(map[common.Address]string)
	var tokenAddresses = make([]common.Address, 0)

	topics := make([][]common.Hash, 0)
	topics = append(topics, []common.Hash{RouterAnycallTopic})

	for _, item := range config.SwapPost.SwapRouter {
		slice := strings.Split(item, ",")
		pairID := slice[0]
		addr := common.HexToAddress(slice[1])
		server := slice[2]

		addreeePairIDMap[addr] = pairID
		serverMap[addr] = server
		tokenAddresses = append(tokenAddresses, addr)
		fmt.Printf("address: %v, pairID: %v, server: %v\n", addr, addreeePairIDMap[addr], serverMap[addr])
	}

	swapoutfq := ethereum.FilterQuery{
		Addresses: tokenAddresses,
		Topics:    topics,
	}

	if start > 0 {
		swapoutfq.FromBlock = big.NewInt(start)
	}
	if end > 0 {
		swapoutfq.ToBlock = big.NewInt(end)
	}

	log.Info("swapRouter fq", "swapRouterfq", swapoutfq)

	ch := make(chan types.Log, 128)
	defer close(ch)

	sub := LoopSubscribe(client, ctx, swapoutfq, ch)
	defer sub.Unsubscribe()

	// subscribe swapout
	for {
		select {
		case msg := <-ch:
			log.Info("Find event", "event", msg)
			txhash := msg.TxHash.String()
			pairID := addreeePairIDMap[msg.Address]
			server := serverMap[msg.Address]
			log.Info("SwapRouter", "txhash", txhash, "msg.Address", msg.Address, "pairID", pairID, "server", server)
			swap := &swapPost{
			        txid:       txhash,
			        pairID:     pairID,
			        rpcMethod:  "swap.RegisterRouterSwap",
			        swapServer: server,
			}

			swaperr := DoSwapRouter(txhash, chainID, "swap.RegisterRouterSwap", server)
			if mongodbEnable {
				if swaperr != nil {
					addMongodbSwapPendingPost(swap)
				} else {
					addMongodbSwapPost(swap)
				}
			}
                case err := <-sub.Err():
                        log.Info("Subscribe swapin error", "error", err)
                        sub.Unsubscribe()
                        sub = LoopSubscribe(client, ctx, swapoutfq, ch)
		}
	}
}

