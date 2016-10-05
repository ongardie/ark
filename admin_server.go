/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// REST-based management interface to the server.
func newAdminServer(s *Server) *http.Server {
	log.Printf("listening for admin requests on %v", s.options.adminAddress)
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/" {
			http.NotFound(w, req)
			return
		}
		fmt.Fprintf(w, "admin http server")
	})
	adminMux.HandleFunc("/raft/stats", func(w http.ResponseWriter, req *http.Request) {
		for k, v := range s.raft.handle.Stats() {
			fmt.Fprintf(w, "%v: %v\n", k, v)
		}
	})
	adminMux.HandleFunc("/raft/membership", func(w http.ResponseWriter, req *http.Request) {
		future := s.raft.handle.GetConfiguration()
		err := future.Error()
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}
		fmt.Fprintf(w, "index: %v, configuration: %+v",
			future.Index(), future.Configuration())
	})
	adminMux.HandleFunc("/api/raft/addvoter", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "POST" {
			decoder := json.NewDecoder(req.Body)
			args := addVoterRequest{}
			err := decoder.Decode(&args)
			if err != nil {
				http.Error(w, fmt.Sprintf("Error decoding JSON: %v", err), http.StatusBadRequest)
				return
			}
			log.Printf("AddVoter %+v\n", args)
			future := s.raft.handle.AddVoter(
				args.ServerId,
				args.Address,
				args.PrevIndex,
				time.Second*5)
			err = future.Error()
			if err != nil {
				http.Error(w, fmt.Sprintf("AddVoter error: %v", err), http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, "AddVoter succeeded")
		} else {
			http.NotFound(w, req)
			return
		}
	})
	return &http.Server{
		Addr:    s.options.adminAddress,
		Handler: adminMux,
	}
}
