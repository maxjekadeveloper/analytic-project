package handler

import (
	"fmt"
	"net/http"
)

func FrontendHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Request received")
	fmt.Println("Method:", r.Method)
	fmt.Println("Path:", r.URL.Path)
	http.FileServer(http.Dir("../frontend")).ServeHTTP(w, r)
}
