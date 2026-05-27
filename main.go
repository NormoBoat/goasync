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

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

const (
	chunkSize  = 10 * 1024 * 1024
	maxRetries = 3
	retryDelay = 2 * time.Second
	maxWorkers = 8
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

	progress := mpb.New()
	log.SetOutput(progress)

	var wg sync.WaitGroup
	for _, file := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()

			err := downloadFile(u, output, progress)
			if err != nil {
				log.Println(err)
			}

		}(file)
	}
	wg.Wait()
	progress.Wait()
}

func isCanRangeDownload(url string) (int64, error) {
	client := &http.Client{Timeout: 30 * time.Second}

	resp, err := client.Head(url)
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

func downloadFile(url, savePath string, progress *mpb.Progress) error {
	if err := os.MkdirAll(savePath, 0777); err != nil {
		return errors.New("ошибка создания каталока загрузки")
	}

	fileSize, err := isCanRangeDownload(url)
	if err != nil {
		return err
	}
	totalChunks := (fileSize + chunkSize - 1) / chunkSize
	total := int(totalChunks)

	filename := path.Base(url)
	file, err := os.OpenFile(savePath+"/"+filename, os.O_CREATE|os.O_RDWR, 0666)
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
		DownloadedChunks: make([]bool, total),
	}

	progressFile := fmt.Sprintf("%s/%s.progress", savePath, filename)
	if _, err := os.Stat(progressFile); err != nil {
		if os.IsNotExist(err) {
			progressState, err := os.Create(progressFile)
			if err != nil {
				return err
			}
			if err = progressState.Close(); err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		data, _ := os.ReadFile(progressFile)
		json.Unmarshal(data, &state)
	}
	if len(state.DownloadedChunks) != total {
		downloadedChunks := make([]bool, total)
		copy(downloadedChunks, state.DownloadedChunks)
		state.DownloadedChunks = downloadedChunks
	}

	downloaded := downloadedBytes(state.DownloadedChunks, totalChunks, fileSize)
	bar := progress.AddBar(fileSize,
		mpb.PrependDecorators(
			decor.Name(filename+" ", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
			decor.Name(" "),
			decor.CountersKibiByte("% .1f / % .1f", decor.WCSyncWidth),
			decor.Name(" "),
			decor.EwmaSpeed(decor.SizeB1024(0), "% .1f", 60, decor.WCSyncWidth),
		),
	)
	bar.SetCurrent(downloaded)
	completed := false
	defer func() {
		if !completed {
			bar.Abort(false)
		}
	}()

	jobs := make(chan int, total)
	errCh := make(chan error, total)
	client := &http.Client{Timeout: 30 * time.Second}
	var wg sync.WaitGroup
	var fileMu sync.Mutex
	var stateMu sync.Mutex

	processChunk := func(id int) error {
		beg, end := chunkBorder(chunkSize, int64(id+1), totalChunks, fileSize)
		log.Printf("Чанк %d/%d: байты %d-%d\n", id+1, totalChunks, beg, end)

		var lastErr error
		for attempt := 0; attempt < maxRetries; attempt++ {
			start := time.Now()
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				return err
			}
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", beg, end))

			resp, err := client.Do(req)
			if err != nil {
				lastErr = err
			} else if resp.StatusCode != http.StatusPartialContent {
				resp.Body.Close()
				return fmt.Errorf("сервер вернул: %d", resp.StatusCode)
			} else {
				data, readErr := io.ReadAll(resp.Body)
				closeErr := resp.Body.Close()
				if readErr != nil {
					lastErr = readErr
				} else if closeErr != nil {
					lastErr = closeErr
				} else if int64(len(data)) != end-beg+1 {
					lastErr = fmt.Errorf("чанк %d: ожидалось %d байт, получено %d", id+1, end-beg+1, len(data))
				} else {
					fileMu.Lock()
					written, err := file.WriteAt(data, beg)
					fileMu.Unlock()
					if err != nil {
						return err
					}
					if written != len(data) {
						return io.ErrShortWrite
					}

					stateMu.Lock()
					state.DownloadedChunks[id] = true
					stateData, err := json.MarshalIndent(state, "", " ")
					if err == nil {
						err = os.WriteFile(progressFile, stateData, 0644)
					}
					stateMu.Unlock()
					if err != nil {
						return err
					}

					bar.EwmaIncrInt64(int64(written), time.Since(start))
					return nil
				}
			}

			if attempt < maxRetries-1 {
				log.Printf("Ошибка? повтор через %v...\n", retryDelay)
				time.Sleep(retryDelay)
			}
		}

		return fmt.Errorf("не удалось скачать чанк %d после %d попыток: %w", id+1, maxRetries, lastErr)
	}

	for w := 0; w < maxWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				if err := processChunk(id); err != nil {
					errCh <- err
				}
			}
		}()
	}

	for i := range total {
		stateMu.Lock()
		downloaded := state.DownloadedChunks[i]
		stateMu.Unlock()
		if downloaded {
			log.Printf("Чанк %d уже загружен, пропускаем\n", i+1)
			continue
		}
		jobs <- i
	}
	close(jobs)

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	log.Printf("Файл: %s (%d байт)\n", filename, fileSize)
	log.Printf("Количество чанков: %d", totalChunks)

	bar.SetTotal(fileSize, true)
	completed = true

	return nil
}

func downloadedBytes(chunks []bool, totalChunks int64, fileSize int64) int64 {
	var size int64
	for i, downloaded := range chunks {
		if !downloaded {
			continue
		}
		beg, end := chunkBorder(chunkSize, int64(i+1), totalChunks, fileSize)
		size += end - beg + 1
	}
	if size > fileSize {
		return fileSize
	}
	return size
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
