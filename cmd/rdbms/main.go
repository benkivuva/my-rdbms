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
    http.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != "POST" {
            http.Error(w, "Only POST allowed", 405)
            return
        }
        
        query := r.FormValue("q")
        if query == "" {
            http.Error(w, "Missing 'q' parameter", 400)
            return
        }
        
        // Run query and get result
        fmt.Println("Received Query:", query)
        result := engine.Execute(query)
        fmt.Fprint(w, result)
    })
    
    fmt.Println("Server listening on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
