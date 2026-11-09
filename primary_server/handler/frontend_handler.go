package handler

import (
	"log"
	"net/http"
)

func FrontendHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Request received")
	log.Println("Method:", r.Method)
	log.Println("Path:", r.URL.Path)
	http.FileServer(http.Dir("../frontend")).ServeHTTP(w, r)
}
