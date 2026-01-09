package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
    // 1. Initialize Engine
    engine, err := initEngine("my_rdbms.db")
    if err != nil {
        log.Fatal(err)
    }
    defer engine.dm.Close()
    
    // Check args
    mode := "repl"
    if len(os.Args) > 1 {
        mode = os.Args[1]
    }
    
    if mode == "server" {
        startServer(engine)
    } else {
        runREPL(engine)
    }
}

func startServer(engine *Engine) {
    // API Route
    http.HandleFunc("/api/query", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != "POST" { return }
        query := r.FormValue("q")
        result := engine.Execute(query)
        fmt.Fprint(w, result)
    })

    // Frontend Route: Serves everything in the /public folder
    fs := http.FileServer(http.Dir("./public"))
    http.Handle("/", fs)

    fmt.Println("Database Engine & Console active at http://localhost:8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
