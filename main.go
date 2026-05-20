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

const (
	chunkSize = 10 * 1024 * 1024
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
			defer wg.Done()
			_ = downloadFile(u, output)
		}(file)
	}
	wg.Wait()
}

func isCanRangeDownload(url string) (int64, error) {
	clinet := &http.Client{Timeout: 30 * time.Second}

	resp, err := clinet.Head(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	contentLength := resp.Header.Get("Content-Length")
	size, _ := strconv.ParseInt(contentLength, 10, 64)

	supportsResume := resp.Header.Get("Accept-Ranges") == "bytes"

	log.Printf("Файл: %s\n", path.Base(url))
	if size != 0 {
		log.Printf("\tРазмер: %d байт (%d МБ)\n", size, size/1024/1024)
	} else {
		log.Printf("\tРазмер: неизвестен\n")
	}

	if supportsResume {
		log.Printf("\tДокачка: поддерживается\n")
	} else {
		log.Printf("\tДокачка: не поддерживается\n")
	}

	if supportsResume && size != 0 {
		log.Printf("Начало загрузки...\n")
	} else {
		log.Printf("Начало загрузки целиком...\n")
	}

	return size, nil
}

func downloadFile(url, savePath string) error {
	if err := os.MkdirAll(savePath, os.ModeDir); err != nil {
		return errors.New("Ошибка создания каталока загрузки")
	}

	fileSize, _ := isCanRangeDownload(url)
	totalChunks := (fileSize + chunkSize - 1) / chunkSize

	for i := range totalChunks {
		beg, end := chunkBorder(chunkSize, i+1)
		log.Printf("Чанк %d/%d: байты %d-%d\n", i+1, totalChunks, beg, end)
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

	log.Printf("Файл: %s (%d байт)\n", filename, fileSize)
	log.Printf("Количество чанков: %d", totalChunks)

	// пройтись про границам цанка

	file, err := os.Create(savePath + "/" + filename)
	file.Truncate(fileSize)

	if err != nil {
		return fmt.Errorf("Не удалось создать файл для сохранения: %s\n", err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)

	return err
}

func chunkBorder(chunkSize int64, nuberChunk int64) (int64, int64) {

	var begin, finish int64

	finish = (chunkSize * nuberChunk) - 1
	begin = chunkSize * (nuberChunk - 1)

	return begin, finish

}
