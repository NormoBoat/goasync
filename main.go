package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

const (
	chunkSize  = 10 * 1024 * 1024
	maxRetries = 3
	retryDelay = 2 * time.Second
)

type DownloadState struct {
	URL              string `json:"url"`
	TotalSize        int64  `json:"total_size"`
	ChunkSize        int    `json:"chink_size"`
	TotalChunks      int    `json:"total_chunks"`
	DownloadedChunks []bool `json:"downloaded_chunks"`
}

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

			err := downloadFile(u, output)
			if err != nil {
				log.Println(err)
			}

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
	if err := os.MkdirAll(savePath, 0777); err != nil {
		return errors.New("ошибка создания каталока загрузки")
	}

	fileSize, err := isCanRangeDownload(url)
	if err != nil {
		return err
	}
	totalChunks := (fileSize + chunkSize - 1) / chunkSize

	filename := path.Base(url)
	file, err := os.Create(savePath + "/" + filename)
	if err != nil {
		return fmt.Errorf("Не удалось создать файл для сохранения: %s\n", err)
	}
	defer file.Close()

	err = file.Truncate(fileSize)
	if err != nil {
		return err
	}

	state := DownloadState{
		URL:              url,
		TotalSize:        fileSize,
		ChunkSize:        chunkSize,
		TotalChunks:      int(totalChunks),
		DownloadedChunks: make([]bool, totalChunks),
	}

	progressFile := fmt.Sprintf("%s/%s.progress", savePath, filename)
	if _, err := os.Stat(progressFile); err != nil {
		if os.IsNotExist(err) {
			if _, err = os.Create(progressFile); err != nil {
				return err
			}
		}
	} else {
		data, _ := os.ReadFile(progressFile)
		json.Unmarshal(data, &state)
	}

	var beg, end int64
	for i := range totalChunks {
		if state.DownloadedChunks[i] {
			fmt.Printf("Чанк %d уже загружен, пропускаем\n", i+1)
			continue
		}
		beg, end = chunkBorder(chunkSize, i+1, totalChunks, fileSize)
		log.Printf("Чанк %d/%d: байты %d-%d\n", i+1, totalChunks, beg, end)

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return err
		}

		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", beg, end))
		clinet := &http.Client{Timeout: 30 * time.Second}
		resp, err := clinet.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusPartialContent {
			return fmt.Errorf("сервер вернул: %d", resp.StatusCode)
		}

		if _, err = file.Seek(beg, io.SeekStart); err != nil {
			log.Println(err)
		}

		for attempt := 0; attempt < maxRetries; attempt++ {

			if _, err = io.Copy(file, resp.Body); err != nil {

				log.Println(err)
				continue
			}
			if err == nil {
				break
			}

			if attempt < maxRetries-1 {
				fmt.Printf("Ошибка? повтор через %v...\n", retryDelay)
				time.Sleep(retryDelay)
			}
		}

		state.DownloadedChunks[i] = true
		data, _ := json.MarshalIndent(state, "", " ")
		if err := os.WriteFile(progressFile, data, 0644); err != nil {

			return err
		}

	}

	log.Printf("Файл: %s (%d байт)\n", filename, fileSize)
	log.Printf("Количество чанков: %d", totalChunks)

	return err
}

func chunkBorder(chunkSize int64, nuberChunk int64, totalChunks int64, fileSize int64) (int64, int64) {

	var begin, finish int64

	begin = chunkSize * (nuberChunk - 1)
	finish = (chunkSize * nuberChunk) - 1
	if nuberChunk == totalChunks {
		finish = fileSize - 1
	}

	return begin, finish

}
