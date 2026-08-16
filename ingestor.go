/*
	CARACTERÍSTICAS:

	1. **Backpressure (Load Shedding):** El sistema rechaza peticiones HTTP (`503`) de forma controlada si el disco no da abasto, evitando caídas por falta de memoria (OOM).
	2. **Arquitectura WAL (Write-Ahead Log):** Garantiza que los webhooks no se den por aceptados (`200 OK`) hasta que estén físicamente sincronizados (`fsync`) en el disco SSD/magnético.
	3. **Procesamiento de Fondo Desacoplado:** El encolamiento en RAM/Disco y el envío a la API final corren en hilos separados, permitiendo absorber picos masivos de tráfico de forma instantánea.
	4. **Recuperación ante desastres (Crash Recovery):** Mediante checkpoints con sobrescritura in-situ (`O_RDWR` + `Seek`), el sistema puede retomar el procesamiento de archivos masivos exactamente desde la última línea confirmada tras un corte de energía, previniendo la corrupción de disco y manteniendo un I/O bajísimo.
	5. **Apagado Seguro (Graceful Shutdown):** Intercepta señales `SIGTERM` para drenar conexiones y vaciar buffers antes de apagar el proceso.
*/

// Note: Linux defaults to 1024 open files. To support more concurrency,
// run 'ulimit -n 65535' or add 'LimitNOFILE=65535' to the [Service] section of your systemd unit.

package main

import (
	"bufio"
	"context"
	"sync"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
)

const (
	endpoint = "/webhook"
	maxConnections = 1024
	port = "80"
	NumProcessingWorkers = 4
	CheckpointInterval   = 50 // File sync
	MinimumAcceptableFileSize = 1
	MaxFileLineBufferSize     = 2 * 1024 * 1024
	MaxFileBufferSize         = 1024 * 128
	MaxFileSize               = 1 * 1024 * 1024
	FlushTimeout              = 100 * time.Millisecond
	RotationTimeout           = 10 * time.Second
	MaxBatchSize              = 100
	IngestionsDirectory       = "./ingestions"
	ProcessingDirectory       = "./processing"
	FailedDirectory           = "./failed"
)

type MessageJob struct {
	Data []byte
	Done chan bool
}

func main() {
	if err := os.MkdirAll(IngestionsDirectory, 0755); err != nil {
		log.Fatalf("Ingestions folder error: %v", err)
	}
	if err := os.MkdirAll(ProcessingDirectory, 0755); err != nil {
		log.Fatalf("Processing folder error: %v", err)
	}
	if err := os.MkdirAll(FailedDirectory, 0755); err != nil {
		log.Fatalf("Failed ingestions folder error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	app := fiber.New(fiber.Config{
		Concurrency: maxConnections,
		AppName:     "WAL Ingestion Engine",
	})

	jobQueue := make(chan MessageJob, maxConnections)
	fileQueue := make(chan string, 1000)

	// Iniciar Worker Pool y Engine
	startFileProcessingWorkers(ctx, fileQueue, &wg)

	wg.Add(1)
	go diskWriterEngine(ctx, jobQueue, fileQueue, &wg)

	app.Post(endpoint, func(c fiber.Ctx) error {
		payload := make([]byte, len(c.Body()))
		copy(payload, c.Body())

		doneChan := make(chan bool, 1)

		select {
		case jobQueue <- MessageJob{Data: payload, Done: doneChan}:
		default:
			return c.SendStatus(fiber.StatusServiceUnavailable)
		}

		if ok := <-doneChan; !ok {
			return c.SendStatus(fiber.StatusServiceUnavailable)
		}

		return c.SendStatus(fiber.StatusOK)
	})

	go func() {
		log.Println("Ready on port: "+port, " PID:", os.Getpid())
		if err := app.Listen(":"+port, fiber.ListenConfig{}); err != nil {
			log.Panic(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	<-quit
	log.Println("Graceful Shutdown initiated...")

	if err := app.Shutdown(); err != nil {
		log.Printf("Fiber Shutdown Error: %v", err)
	}

	cancel()
	wg.Wait()
	log.Println("Server successfully stopped without data loss.")
}

func diskWriterEngine(ctx context.Context, jobs chan MessageJob, fileQueue chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		currentFile  *os.File
		writer       *bufio.Writer
		bytesWritten int
		pendingAcks  []chan bool
		filePath     string
		fileName     string
	)

	// Close current file, open new file
	rotateFile := func() {
		// Close current open file
		if currentFile != nil {
			if bytesWritten < MinimumAcceptableFileSize {
				return
			}

			writer.Flush()
			currentFile.Sync()
			currentFile.Close()
			go fileReadyToProcess(filePath, fileName, fileQueue)
		}

		// Create new file
		fileName = fmt.Sprintf("ingest_%d_%d.list", os.Getpid(), time.Now().UnixNano())
		filePath = filepath.Join(IngestionsDirectory, fileName)

		var err error
		currentFile, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("New ingestion file error: %v", err)
		}

		writer = bufio.NewWriterSize(currentFile, MaxFileBufferSize)
		bytesWritten = 0
		log.Printf("Ingestion file created: %s", filePath)
	}

	// Flush & Sync, and communicate to channel
	flushAndAck := func() {
		if len(pendingAcks) == 0 {
			return
		}

		writer.Flush()
		currentFile.Sync() // Carefully designed to avoid data loss

		for _, ch := range pendingAcks {
			ch <- true
		}

		// Clear pending list
		pendingAcks = pendingAcks[:0]
	}

	// Timer
	ticker := time.NewTicker(FlushTimeout)
	defer ticker.Stop()

	// Rotation Timer
	rotationTicker := time.NewTicker(RotationTimeout)
	defer rotationTicker.Stop()

	rotateFile()
	go processPendingFiles(filePath, fileQueue)
	go recoverProcessingFiles(fileQueue)

	for {
			select {
			case <-ctx.Done():
				// Drenar mensajes que quedaron en la cola antes de apagar
				for len(jobs) > 0 {
					job := <-jobs
					n, err := writer.Write(job.Data)
					if err == nil {
						writer.WriteByte('\n')
						bytesWritten += n + 1
						pendingAcks = append(pendingAcks, job.Done)
					} else {
						job.Done <- false
					}
				}
				flushAndAck()
				if currentFile != nil {
					currentFile.Close()
					if bytesWritten >= MinimumAcceptableFileSize {
						fileReadyToProcess(filePath, fileName, fileQueue)
					}
				}
				return

			case job := <-jobs:
			// Write to RAM
			n, err := writer.Write(job.Data)
			if err != nil {
				log.Printf("Write error: %v", err)
				job.Done <- false
			} else {
				writer.WriteByte('\n') // Line separator

				bytesWritten += n + 1
				pendingAcks = append(pendingAcks, job.Done)

				// Limitations

				if bytesWritten >= MaxFileSize {
					flushAndAck()
					rotateFile()
				} else if len(pendingAcks) >= MaxBatchSize {
					flushAndAck()
				}
			}

		case <-ticker.C:
			flushAndAck()

		case <-rotationTicker.C:
			flushAndAck()
			rotateFile()
		}
	}
}

func processPendingFiles(activeFilename string, fileQueue chan<- string) {
	entries, err := os.ReadDir(IngestionsDirectory)
	if err != nil {
		log.Printf("Process pending files directory: %v", err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		filePath := filepath.Join(IngestionsDirectory, fileName)

		// Skip active file
		if filePath == activeFilename {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue // Skip unreadable files or files with errors
		}

		size := int(info.Size())
		if size < MinimumAcceptableFileSize {
			os.Remove(filePath)
			continue // Delete empty files
		}

		fileReadyToProcess(filePath, fileName, fileQueue)
	}
}

func recoverProcessingFiles(fileQueue chan<- string) {
	entries, err := os.ReadDir(ProcessingDirectory)
	if err != nil {
		log.Printf("Recover processing files error: %v", err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		filePath := filepath.Join(ProcessingDirectory, name)

		doneDeleteFile := strings.HasSuffix(name, ".done.delete")
		if doneDeleteFile {
			baseName := strings.TrimSuffix(name, ".done.delete")
			os.Remove(filepath.Join(ProcessingDirectory, baseName))
			os.Remove(filePath)
			continue
		}

		deleteFile := strings.HasSuffix(name, ".delete")
		if deleteFile {
			baseName := strings.TrimSuffix(name, ".delete")
			os.Remove(filepath.Join(ProcessingDirectory, baseName))
			os.Remove(filePath)
			continue
		}

		listFile := strings.HasSuffix(name, ".list")
		if !listFile {
			continue
		}

		info, err := entry.Info()
		if err != nil || info.Size() < MinimumAcceptableFileSize {
			continue
		}

		log.Printf("Recovering interrupted file: %s", filePath)
		fileQueue <- filePath
	}
}

func fileReadyToProcess(filePath string, fileName string, fileQueue chan<- string) {
    processing := filepath.Join(ProcessingDirectory, fileName)
    if err := os.Rename(filePath, processing); err != nil {
        log.Printf("Cannot move to processing folder: %v", err)
        return
    }

    log.Printf("File ready to process: %s", processing)
    fileQueue <- processing // Send to worker pool
}

func fileProcess(filePath string, fileName string) {
	checkpointPath := filePath + ".done"
	skipLines := 0

	// Read starting from the checkpoint
	checkpointData, checkpointErr := os.ReadFile(checkpointPath)
	if checkpointErr == nil {
		fmt.Sscanf(string(checkpointData), "%d", &skipLines)
		log.Printf("File process resuming %s from line %d", fileName, skipLines)
	}

	// Checkpoint file always open
	chkFile, err := os.OpenFile(checkpointPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Printf("Cannot open checkpoint file: %v", err)
		return
	}
	defer chkFile.Close()

	// Open file to process
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Cannot open file to process: %v", err)
		return
	}
	defer file.Close()

	errFilepath := filepath.Join(FailedDirectory, fileName)
	lineNumber := 0

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, MaxFileLineBufferSize), MaxFileLineBufferSize)
	var lineProcessResult error

	saveCheckpoint := func(lineNum int) {
		// Go back to position 0 in the file
		chkFile.Seek(0, 0)

		// Write left-padded spaces
		fmt.Fprintf(chkFile, "%-20d", lineNum)

		chkFile.Sync()
	}

	for scanner.Scan() {
		lineNumber++
		if lineNumber <= skipLines {
			continue
		}

		line := scanner.Bytes()

		if json.Valid(line) {
			// Logic per JSON
			lineProcessResult = processJSONLine(line)

			if lineProcessResult != nil {
				log.Printf("File process line error in %s: %v", fileName, lineProcessResult)
				break
			}

			if lineNumber%CheckpointInterval == 0 {
				saveCheckpoint(lineNumber)
			}
			continue
		}

		log.Printf("File process invalid JSON on line %d of: %s", lineNumber, filePath)
	}
	saveCheckpoint(lineNumber)


	err = scanner.Err()

	if err != nil {
		log.Printf("File process scanner error in %s: %v", fileName, err)
		err = os.Rename(filePath, errFilepath)
		if err != nil {
			log.Printf("File process scanner error, rename error: %v", err)
			os.Create(filePath)
		}
		return
	}

	log.Printf("File process completed: %s (%d lines)", fileName, lineNumber)

	// Cleaning & close
	chkFile.Close()
	file.Close()

	err = os.Remove(filePath)
	if err != nil {
		log.Printf("File processing completion error: %v", err)
		os.Create(filePath + ".delete")
	}

	if lineProcessResult == nil {
		err = os.Remove(checkpointPath)
		if err != nil {
			log.Printf("Completed file process checkpoint deletion error: %v", err)
			os.Create(checkpointPath + ".delete")
		}
	}
}

func processJSONLine(json []byte) error {
	// Here comes the logic to send the JSON to another server
	// Backend needs UNIQUE message checking
	return nil
}

func startFileProcessingWorkers(ctx context.Context, fileQueue <-chan string, wg *sync.WaitGroup) {
	for i := 1; i <= NumProcessingWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case path, ok := <-fileQueue:
					if !ok {
						return
					}
					fileProcess(path, filepath.Base(path))
				}
			}
		}(i)
	}
}
