package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHttpServer(addr string) *http.Server {
	s := newHttpServer()
	r := mux.NewRouter()

	r.HandleFunc("/", s.handleProduce).Methods("POST")
	r.HandleFunc("/", s.handleConsume).Methods("GET")

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

type httpServer struct {
	Log *Log
}

func newHttpServer() *httpServer {
	return &httpServer{Log: NewLog()}
}

func (h *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var produceReq ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&produceReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	offset, err := h.Log.Append(produceReq.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := ProduceResponse{offset}
	err = json.NewEncoder(w).Encode(&resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var consumeReq ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&consumeReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rec, err := h.Log.Read(consumeReq.Offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	consumeResp := ConsumeResponse{rec}
	err = json.NewEncoder(w).Encode(&consumeResp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
