package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	web "net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

func main() {
	if len(os.Args) < 3 {
		log.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	output := os.Args[1]
	urls := os.Args[2:]

	var wg sync.WaitGroup
	for _, file := range urls {
		wg.Add(1)
		go func(u string) {
			isCanRangeDownload(u)
			defer wg.Done()
			_ = downloadFile(u, output)
		}(file)
	}
	wg.Wait()
}

func isCanRangeDownload(url string) error {
	clinet := &http.Client{Timeout: 30 * time.Second}

	resp, err := clinet.Head(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	contentLength := resp.Header.Get("Content-Length")
	size, _ := strconv.ParseInt(contentLength, 10, 64)

	supportsResume := resp.Header.Get("Accept-Ranges") == "bytes"

	log.Printf("Файл: %s\n", path.Base(url))
	if contentLength != "" {
		log.Printf("\tРазмер: %d байт (%d МБ)\n", size, size/1024/1024)
	} else {
		log.Printf("\tРазмер: неизвестен\n")
	}

	if supportsResume {
		log.Printf("\tДокачка: поддерживается\n")
		log.Printf("Начало загрузки...\n")
	} else {
		log.Printf("\tДокачка: не поддерживается\n")
		log.Printf("Начало загрузки целиком...\n")
	}

	return nil
}

func downloadFile(url, savePath string) error {
	if err := os.MkdirAll(savePath, os.ModeDir); err != nil {
		return errors.New("Ошибка создания каталока загрузки")
	}

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Сервер вернул: %d", resp.StatusCode)
	}

	u, err := web.Parse(url)
	if err != nil {
		return err
	}

	filename := path.Base(u.Path)

	file, err := os.Create(savePath + "/" + filename)
	if err != nil {
		return fmt.Errorf("Не удалось создать файл для сохранения: %s\n", err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)

	return err
}
