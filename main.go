package main

import (
	"log"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		log.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	output := os.Args[1]
	urls := os.Args[2:]
}
