package quickRpc

/*
要实现一个函数能够被远程调用需要满足以下条件
1、 方法类型能被调用
2、 方法能够被调用
3、 方法参数有两个，并且都是暴露的
4、 方法的第二个参数是指针类型
5、 方法能够返回错误
*/

import (
	"../quickRpc/effiCode"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

// Call 封装一个结构体 Call，该结构体用于承载一次 rpc 调用所需要的所有信息。
type Call struct {
	Seq           uint64
	ServiceMethod string      //调用格式为 service.method
	Args          interface{} // 执行方法的参数
	Reply         interface{} //方法的返回值
	Error         error
	Done          chan *Call //提示，当该结构体完成。是为了支持异步调用
}

func (call *Call) done() {
	call.Done <- call
}

// Client 客户端结构体，用来实现客户端的一些方法
type Client struct {
	ec       effiCode.Codec //消息的编解码器，用来序列化和反序列化消息
	opt      *Option
	sending  sync.Mutex      //互斥锁，为了保证消息有序发送，防止多个请求报文混淆
	header   effiCode.Header //请求的消息头
	mu       sync.Mutex
	seq      uint64           //用于发送的请求编号，每个请求有唯一编号。
	pending  map[uint64]*Call //用来存储未处理完的请求，键值是seq
	closing  bool             //用户关闭连接，true表示client不可用
	shutdown bool             //服务端终止信号，true表示client不可用
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// Close 关闭连接.
// 关闭客户度端，加锁保证并发执行的正确性
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.ec.Close()
}

// IsAvailable 判定client 是否可用
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// 客户端注册一个发送消息体，消息体的序号为客户端的缓存序号，将消息体放入 pending 中，并返回该消息体的序号
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// 用来获取 map 中第一个消息体，并移除
func (client *Client) popCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()

	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 服务端或者客户端发生错误时，将 shutdown 设置为 true，并且将错误信息通知所有在 pending中等待的 call
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()

	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// 响应接收，接收到响应一般有三种情况
func (client *Client) receive() {
	var err error

	for err == nil {
		var h effiCode.Header
		if err = client.ec.ReadHeader(&h); err != nil {
			break
		}
		call := client.popCall(h.Seq)
		switch {
		case call == nil:
			err = client.ec.ReadBody(nil)
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = client.ec.ReadBody(nil)
			call.done()
		default:
			err = client.ec.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body" + err.Error())
			}
			call.done()
		}
		// 如果发生错误将终止客户端
		client.terminateCalls(err)
	}
}

// 实现客户端发送请求的能力
func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	if err := client.ec.Write(&client.header, call.Args); err != nil {
		call := client.popCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// NewClient 创建客户端前，先和服务端协商编解码协议
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := effiCode.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

// 创建一个子协程调用 receive() 接收响应
func newClientCodec(ec effiCode.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1,
		ec:      ec,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// Dial 用 network， address 连接具体的 rpc 服务
func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, opt)
}

func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// Call invokes the named function, waits for it to complete,
// and returns its error status.
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
