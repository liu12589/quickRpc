package quickRpc

import (
	"./effiCode"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

const MagicNumber = 0x3bef5c

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   effiCode.GobType,
}

type Option struct {
	MagicNumber int
	CodecType   effiCode.Type
}

type Server struct{}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

func (server *Server) Accept(lis net.Listener) {
	for true {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

// ServeConn
// 1、使用 json.NewDecoder 反序列化得到 option 实例
// 2、检查 MagicNumber\CodecType 是否正确
// 3、根据 CodecType 得到对应的消息编码解码器
// 4、使用 serverCodec 解码
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server:options:", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := effiCode.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	server.serveCodec(f(conn))
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

// serveCodec
// 读取请求
// 处理请求
// 回复请求
func (server *Server) serveCodec(ec effiCode.Codec) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for true {
		req, err := server.readRequest(ec)
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			server.sendResponse(ec, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(ec, req, sending, wg)
	}
	wg.Wait()
	_ = ec.Close()
}

type request struct {
	h            *effiCode.Header
	argv, replyv reflect.Value
}

// 获取头部信息
func (server *Server) readRequestHeader(ec effiCode.Codec) (*effiCode.Header, error) {
	var h effiCode.Header
	if err := ec.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// 获取所有数据
func (server *Server) readRequest(ec effiCode.Codec) (*request, error) {
	h, err := server.readRequestHeader(ec)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.argv = reflect.New(reflect.TypeOf(""))
	if err = ec.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read argv err:", err)
	}
	return req, nil
}

// 发送数据
func (server *Server) sendResponse(ec effiCode.Codec, h *effiCode.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := ec.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// 处理请求
func (server *Server) handleRequest(ec effiCode.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println(req.h, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("quickRpc response %d", req.h.Seq))
	server.sendResponse(ec, req.h, req.replyv.Interface(), sending)
}
