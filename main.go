package main

import (
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/diegocmsantos/kvdatabase/kvdatabase"
	"github.com/gorilla/mux"
)

// keyValuePutHandler expects to be called with a PUT request for
// the "/v1/key/{key}" resource
func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = kvdatabase.Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	kvdatabase.Logger.WritePut(key, string(value))

	w.WriteHeader(http.StatusCreated)
}

// keyValueGetHandler expects to be called with a GET requrest for
// the "/v1/key/{key}" resource
func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := kvdatabase.Get(key)
	if errors.Is(err, kvdatabase.ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
}

// keyValueGetHandler expects to be called with a DELETE requrest for
// the "/v1/key/{key}" resource
func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := kvdatabase.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	kvdatabase.Logger.WriteDelete(key)

	w.WriteHeader(http.StatusAccepted)
}

func main() {
	err := kvdatabase.InitializeTransactionLog()
	if err != nil {
		log.Fatal(err)
	}
	r := mux.NewRouter()
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods(http.MethodPut)
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods(http.MethodGet)
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods(http.MethodDelete)
	log.Fatal(http.ListenAndServe(":8080", r))
}
