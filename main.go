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
)

func main() {
	if len(os.Args) < 3 {
		log.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	output := os.Args[1]
	urls := os.Args[2:]

	for _, files := range urls {
		downloadFile(files, output)
	}
}

func downloadFile(url, savePath string) error {
	if err := os.MkdirAll(savePath, os.ModeDir); err != nil {
		return errors.New("Ошибка создания каталока загрузки")
	}

	resp, err := http.Get(url)
	defer resp.Body.Close()
	if err != nil {
		return err
	}
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
