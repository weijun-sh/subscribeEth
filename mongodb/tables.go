package mongodb

import (
	//"gopkg.in/mgo.v2/bson"
)

const (
	tbSwap        string = "swap"
	tbSwapPending string = "pending"
	tbSwapDeleted string = "deleted"
)

type MgoSwap struct {
        Id         string `bson:"_id"`//txid
        Txid       string `bson:"txid"`
        PairID     string `bson:"pairID"` //"FXSv4"
        RpcMethod  string `bson:"rpcMethod"` //"swap.Swapin"
        SwapServer string `bson:"swapServer"`
	Chain      string `bson:"chain"`
        Timestamp  uint64 `bson:"timestamp"`
}

