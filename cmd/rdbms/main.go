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
        
        // Capture stdout to buffer to return result?
        // Or refactor Engine.Execute to return string/error.
        // For now, let's just log to console and return "OK" or basic info.
        // Refactoring Execute to return result is better.
        
        // Quick Hack: Just run it. Content goes to stdout.
        fmt.Println("Received Query:", query)
        engine.Execute(query)
        fmt.Fprintf(w, "Query executed. Check server logs for output.\n")
    })
    
    fmt.Println("Server listening on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
