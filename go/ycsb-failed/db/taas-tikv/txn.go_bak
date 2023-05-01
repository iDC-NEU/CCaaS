// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

//import "C"
//import "C"
import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/pingcap/errors"
	"io/ioutil"
	"sync/atomic"

	// "github.com/zeromq/goczmq"
	// "gopkg.in/zeromq/goczmq.v1"
	"log"
	"strconv"
	"unsafe"

	//"github.com/magician/properties"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"net"
	"strings"
	"time"
)

//#include ""
import (
	"context"
	"github.com/golang/protobuf/proto"

	zmq "github.com/pebbe/zmq4"
	tikverr "github.com/tikv/client-go/v2/error"
)

const (
	tikvAsyncCommit = "tikv.async_commit"
	tikvOnePC       = "tikv.one_pc"
)

type txnConfig struct {
	asyncCommit bool
	onePC       bool
}

type txnDB struct {
	db      *txnkv.Client
	r       *util.RowCodec
	bufPool *util.BufPool
	cfg     *txnConfig
}

const TaasServerIp = "172.30.67.201"

type TaasTxn struct {
	GzipedTransaction []byte
}

var TaasTxnCH = make(chan TaasTxn, 100000)
var UnPackCH = make(chan string, 100000)

var ChanList []chan string
var init_ok uint64 = 0
var atomic_counter uint64 = 0

func SendTxnToTaas() {
	fmt.Println("连接taas Send")

	socket, _ := zmq.NewSocket(zmq.PUSH)
	socket.SetSndbuf(1000000000000000)
	socket.SetRcvbuf(1000000000000000)
	socket.SetSndhwm(1000000000000000)
	socket.SetRcvhwm(1000000000000000)
	socket.Connect("tcp://" + TaasServerIp + ":5551")

	// dealer, _ := goczmq.NewPush("tcp://" + TaasServerIp + ":5551")

	fmt.Println("连接成功 Send")
	for {
		value, ok := <-TaasTxnCH
		if ok {
			// err := dealer.SendFrame(value.GzipedTransaction, 0)
			socket.Send(string(value.GzipedTransaction), 0)
			// if err != nil {
			// 	fmt.Println("txn.go 93")
			// 	log.Fatal(err)
			// 	break
			// }
		} else {
			fmt.Println("txn.go 98")
			log.Fatal(ok)
			break
		}
	}
}

func ListenFromTaas() {
	fmt.Println("连接taas Listen")
	// router, err := goczmq.NewPull("tcp://*:5552")
	socket, err := zmq.NewSocket(zmq.PULL)
	socket.SetSndbuf(1000000000000000)
	socket.SetRcvbuf(1000000000000000)
	socket.SetSndhwm(1000000000000000)
	socket.SetRcvhwm(1000000000000000)
	socket.Bind("tcp://*:5552")
	fmt.Println("连接成功 Listen")
	if err != nil {
		log.Fatal(err)
	}
	for {
		// taas_reply, err := router.RecvMessage()
		taas_reply, err := socket.Recv(0)
		if err != nil {
			fmt.Println("txn.go 115")
			log.Fatal(err)
		}
		UnPackCH <- taas_reply
		// UnGZipedReply := UGZipBytes(taas_reply[0])
		// testMessage := &Message{}
		// err = proto.Unmarshal(UnGZipedReply, testMessage)
		// if err != nil {
		// 	fmt.Println("txn.go 118")
		// 	log.Fatal(err)
		// }
		// //fmt.Println(testMessage)
		// replyMessage := testMessage.GetReplyTxnResultToClient()
		// //fmt.Println("txn.go 123 " + string(replyMessage.ClientTxnId))
		// //fmt.Println(replyMessage.ClientTxnId % 2048)
		// (ChanList[replyMessage.ClientTxnId%2048]) <- string(replyMessage.GetTxnState().String())
	}
}

func UnPack() {
	for {
		taas_reply, ok := <-UnPackCH
		if ok {
			UnGZipedReply := UGZipBytes([]byte(taas_reply))
			testMessage := &Message{}
			err := proto.Unmarshal(UnGZipedReply, testMessage)
			if err != nil {
				fmt.Println("txn.go 142")
				log.Fatal(err)
			}
			replyMessage := testMessage.GetReplyTxnResultToClient()
			(ChanList[replyMessage.ClientTxnId%2048]) <- string(replyMessage.GetTxnState().String())
		} else {
			fmt.Println("txn.go 148")
			log.Fatal(ok)
			break
		}
	}
}

func createTxnDB(p *properties.Properties) (ycsb.DB, error) {
	pdAddr := p.GetString(tikvPD, "127.0.0.1:2379")
	db, err := txnkv.NewClient(strings.Split(pdAddr, ","))
	if err != nil {
		return nil, err
	}

	for i := 0; i < 2048; i++ {
		ChanList = append(ChanList, make(chan string, 100000))
	}

	init_ok = 1
	go SendTxnToTaas()
	go ListenFromTaas()
	for i := 0; i < 4; i++ {
		go UnPack()
	}
	time.Sleep(5)

	cfg := txnConfig{
		asyncCommit: p.GetBool(tikvAsyncCommit, true),
		onePC:       p.GetBool(tikvOnePC, true),
	}

	bufPool := util.NewBufPool()

	return &txnDB{
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: bufPool,
		cfg:     &cfg,
	}, nil
}

func (db *txnDB) Close() error {
	return db.db.Close()
}

func (db *txnDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *txnDB) CleanupThread(ctx context.Context) {
}

func (db *txnDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *txnDB) beginTxn() (*transaction.KVTxn, error) {
	txn, err := db.db.Begin()
	if err != nil {
		return nil, err
	}

	txn.SetEnableAsyncCommit(db.cfg.asyncCommit)
	txn.SetEnable1PC(db.cfg.onePC)

	return txn, err
}

func (db *txnDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row, err := tx.Get(ctx, db.getRowKey(table, key))
	if tikverr.IsErrNotFound(err) {
		return nil, nil
	} else if row == nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	return db.r.Decode(row, fields)
}

func (db *txnDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rowValues := make([]map[string][]byte, len(keys))
	for i, key := range keys {
		value, err := tx.Get(ctx, db.getRowKey(table, key))
		if tikverr.IsErrNotFound(err) || value == nil {
			rowValues[i] = nil
		} else {
			rowValues[i], err = db.r.Decode(value, fields)
			if err != nil {
				return nil, err
			}
		}
	}

	return rowValues, nil
}

func (db *txnDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	it, err := tx.Iter(db.getRowKey(table, startKey), nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	rows := make([][]byte, 0, count)
	for i := 0; i < count && it.Valid(); i++ {
		value := append([]byte{}, it.Value()...)
		rows = append(rows, value)
		if err = it.Next(); err != nil {
			return nil, err
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	res := make([]map[string][]byte, len(rows))
	for i, row := range rows {
		if row == nil {
			res[i] = nil
			continue
		}

		v, err := db.r.Decode(row, fields)
		if err != nil {
			return nil, err
		}
		res[i] = v
	}

	return res, nil
}

// 获取本机当前可用端口号
func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// 获取ip
func externalIP() (net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			ip := getIpFromAddr(addr)
			if ip == nil {
				continue
			}
			return ip, nil
		}
	}
	return nil, errors.New("connected to the network?")
}

// 获取本机IP地址
func getIpFromAddr(addr net.Addr) net.IP {
	var ip net.IP
	switch v := addr.(type) {
	case *net.IPNet:
		ip = v.IP
	case *net.IPAddr:
		ip = v.IP
	}
	if ip == nil || ip.IsLoopback() {
		return nil
	}
	ip = ip.To4()
	if ip == nil {
		return nil // not an ipv4 address
	}

	return ip
}

// 将远端发过来的消息进行解压缩
func UGZipBytes(in []byte) []byte {
	// var out bytes.Buffer
	// var in bytes.Buffer
	// in.Write(data)
	// r, _ := gzip.NewReader(&in)
	// r.Close() //这句放在后面也没有问题，不写也没有任何报错
	// //机翻注释：关闭关闭读者。它不会关闭底层的io.Reader。为了验证GZIP校验和，读取器必须完全使用，直到io.EOF。

	// io.Copy(&out, r) //这里我看了下源码不是太明白，
	// //我个人想法是这样的，Reader本身就是go中表示一个压缩文件的形式，r转化为[]byte就是一个符合压缩文件协议的压缩文件

	// return out.Bytes()
	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out
	}
	defer reader.Close()
	out, _ := ioutil.ReadAll(reader)
	//	fmt.Println("out: ", out)
	return out

}

// 修改代码的主体部分
func (db *txnDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	for init_ok == 0 {
		time.Sleep(50)
	}
	chan_id := atomic.AddUint64(&atomic_counter, 1) // return new value

	rowKey := db.getRowKey(table, key)
	var bufferBeforeGzip bytes.Buffer
	//var bufferReplyGzip bytes.Buffer
	tx, err := db.beginTxn()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	row, err := tx.Get(ctx, rowKey)
	//fmt.Println("get data before really update~~~")
	if tikverr.IsErrNotFound(err) {
		return nil
	} else if row == nil {
		return err
	}
	//ADDBY NEUPQ
	//data, err := db.r.Decode(row, nil)
	//if err != nil {
	//	return err
	//}
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	ip, err := externalIP()

	if err != nil {
		fmt.Println(err)
	}
	//输出一下当前tikv集群的连接IP地址 taasAddress
	//fmt.Println("输出一下当前tikv集群的连接IP地址")
	//fmt.Println(db.db.KVStore.GetPDClient().GetLeaderAddr())
	//fmt.Println("输出一下当前client所连接taas服务端的IP地址")
	//fmt.Println(db.db.KVStore.GetPDClient().get)
	//fmt.Println(globalProps)
	//taasAddress, _ = properties.Properties.Get(properties.Properties, "taasAddress")
	//fmt.Println(db.db.KVStore.GetPDClient().LoadGlobalConfig(ctx, "taasAddress"))
	//fmt.Println("输出一下本机的IP地址")
	//fmt.Println(ip)
	//fmt.Println(db.db.KVStore.GetClusterID())
	//fmt.Println("从client的上下文获取本机IP地址，以及空闲端口号")
	//fmt.Println(db.db.KVStore.GetClientIP())
	//fmt.Println(db.db.KVStore.GetClientFreePort())
	//fmt.Println("打印完成，检查一下打印结果是否正确！！！")
	// NEU 打印一下行数据，看一下是什么样子的
	//为当前ycsb测试线程获取本机空闲端口号
	//freePort, err := GetFreePort()
	//if err != nil {
	//	fmt.Println("本机无空闲端口可用，测试过程发生错误！！！")
	//	return err
	//}
	//fmt.Println("空闲端口号：" + strconv.Itoa(freePort))
	//fmt.Println("生成报文, 拼装socket")
	//ipPlusPort := "tcp://*:" + strconv.Itoa(freePort)
	//clientIP := ip.String() + ":" + strconv.Itoa(freePort)
	clientIP := ip.String()
	//fmt.Println("输出clientIP")
	//fmt.Println(clientIP)
	//生成protobuf报文，数据部分为空，只传递元信息
	//生成txn数据
	txnSendToTaas := Transaction{
		//Row:         {},
		StartEpoch:  0,
		CommitEpoch: 5,
		Csn:         uint64(time.Now().UnixNano()),
		ServerIp:    TaasServerIp,
		ServerId:    0,
		ClientIp:    clientIP,
		ClientTxnId: chan_id,
		TxnType:     TxnType_ClientTxn,
		TxnState:    0,
	}
	//给txn里面的row进行数据赋值

	updateKey := key
	sendRow := Row{
		OpType: OpType_Update,
		//TableName: "",
		Key: *(*[]byte)(unsafe.Pointer(&updateKey)),
		//Data: []byte("fuck you later~"),
	}
	//向row中添加实际需要更新的数据
	for field, value := range values {
		idColumn, _ := strconv.ParseUint(string(field[5]), 10, 32)
		updatedColumn := Column{
			Id:    uint32(idColumn),
			Value: value,
		}
		sendRow.Column = append(sendRow.Column, &updatedColumn)
	}
	//向txn里面添加row
	txnSendToTaas.Row = append(txnSendToTaas.Row, &sendRow)
	//向消息中添加txn
	sendMessage := &Message{
		Type: &Message_Txn{&txnSendToTaas},
	}
	sendBuffer, _ := proto.Marshal(sendMessage)
	//开始对报文进行Gzip压缩

	bufferBeforeGzip.Reset()
	gw := gzip.NewWriter(&bufferBeforeGzip)
	gw.Write(sendBuffer)
	gw.Close()
	GzipedTransaction := bufferBeforeGzip.Bytes()
	//GzipedTransaction := protoimpl.X.CompressGZIP(sendBuffer)
	//生成zmq发送上下文
	//router, err := goczmq.NewPull(ipPlusPort)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer router.Destroy()
	TaasTxnCH <- TaasTxn{GzipedTransaction}
	//fmt.Println(chan_id)
	//fmt.Println(chan_id % 2048)
	result, ok := <-(ChanList[chan_id%2048])
	//fmt.Println("txn.go 480 get from chanlist")
	if ok {
		if result != "Commit" {
			//fmt.Println("事务未提交成功，当前事务已经回滚，可以进行下一个事务")
			return err
			defer tx.Rollback()
		}
		//fmt.Println("事务提交成功，当前事务已经提交，可以进行下一个事务")
		return tx.Commit(ctx)
	} else {
		fmt.Println("txn.go 481")
		log.Fatal(ok)
		return err
		defer tx.Rollback()
	}
	return tx.Rollback()
	//taas_reply, err := router.RecvMessage()
	//if err != nil {
	//	log.Fatal(err)
	//}
	//// test by singheart
	//fmt.Println("receive message from taas", taas_reply)
	////反向解析数据
	//fmt.Println("转换完成")
	//UnGZipedReply := UGZipBytes(taas_reply[0])
	////fmt.Println("UnGzipedReply", UnGZipedReply)
	//fmt.Println("数据解析成功，等待后续操作")
	//testMessage := &Message{}
	//err = proto.Unmarshal(UnGZipedReply, testMessage)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Println(testMessage)
	//replyMessage := testMessage.GetReplyTxnResultToClient()
	//if replyMessage.GetTxnState().String() != "Commit" {
	//	fmt.Println("事务未提交成功，当前事务已经回滚，可以进行下一个事务")
	//	return err
	//	defer tx.Rollback()
	//}
	//fmt.Println("事务提交成功，当前事务已经提交，可以进行下一个事务")
	//return tx.Commit(ctx)
	// replyMessage := &ReplyTransactionToClient{}
	// err = proto.Unmarshal(UnGZipedReply, replyMessage)
	// if err != nil {
	// 	fmt.Println("数据解析失败，请检查是哪里出了问题~！！！！")
	// }
	//打印从远端服务器回复的消息
	//fmt.Println(replyMessage.GetTxnState())
	//fmt.Println(replyMessage.GetRecvNode())
	//fmt.Println(replyMessage.GetSendNode())
	//fmt.Println(replyMessage.GetClientTxnId())
	//fmt.Println("接收到消息，可以进行剩下的操作")
	//接收到消息之后，及时关闭socket
	//fmt.Println("从taas返回数据成功")
	//if replyFromCServer == "Hello" {
	//
	//}
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	//fmt.Println("put update data to temp row storage before really update~~~")

	//ADDBY NEUPQ#######################################################################################################
	//把实际需要更新的数据打印出来
	/*for field, value := range values {
		data[field] = value
	}
	//fmt.Println("load buffer pool~~~")
	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()
	//fmt.Println("load buffer pool 2~~~")
	buf, err = db.r.Encode(buf, data)
	if err != nil {
		return err
	}
	//fmt.Println("set data~~~")
	if err := tx.Set(rowKey, buf); err != nil {
		return err
	}
	//fmt.Println("before commit")*/
	//ADDBY NEUPQ#######################################################################################################
	//if replyMessage.GetTxnState().String() != "Commit" {
	//	fmt.Println("事务未提交成功，当前事务已经回滚，可以进行下一个事务")
	//	return err
	//	defer tx.Rollback()
	//}
	////else {
	//fmt.Println("事务提交成功，当前事务已经提交，可以进行下一个事务")
	//return tx.Commit(ctx)
	//}
}

func (db *txnDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	tx, err := db.beginTxn()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i, key := range keys {
		// TODO should we check the key exist?
		rowData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		if err = tx.Set(db.getRowKey(table, key), rowData); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (db *txnDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}

	tx, err := db.beginTxn()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if err = tx.Set(db.getRowKey(table, key), buf); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *txnDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	tx, err := db.beginTxn()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i, key := range keys {
		rowData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		if err = tx.Set(db.getRowKey(table, key), rowData); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (db *txnDB) Delete(ctx context.Context, table string, key string) error {
	tx, err := db.beginTxn()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	err = tx.Delete(db.getRowKey(table, key))
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *txnDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	tx, err := db.beginTxn()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, key := range keys {
		if err != nil {
			return err
		}
		err = tx.Delete(db.getRowKey(table, key))
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
