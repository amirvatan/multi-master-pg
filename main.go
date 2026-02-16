package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DBUser     = "test"
	DBPassword = "1234"
	DBHost     = "localhost"
	DBPort     = "15432"
	DBName     = "db"

	// Publication and subscription names for counter table
	PubName = "counter_pub"
	SubName = "counter_sub"
)

type Command int

const (
	Write Command = iota
	Read
	Stop
	Status
	Up
	Down
	Pub
	Sub
	Both
	Clear
	Delete
	Exit
)

// WriteLogEntry is sent to the log channel after each insert.
type WriteLogEntry struct {
	ID        int
	Name      string
	Value     int
	Timestamp time.Time
}

var (
	writeLogChan chan WriteLogEntry
	writeCancel  context.CancelFunc
	writeDone    chan struct{}
)

func validateCommand(cmdStr string) (Command, error) {
	switch strings.ToLower(cmdStr) {
	case "write":
		return Write, nil
	case "read":
		return Read, nil
	case "stop":
		return Stop, nil
	case "status":
		return Status, nil
	case "up":
		return Up, nil
	case "down":
		return Down, nil
	case "pub":
		return Pub, nil
	case "sub":
		return Sub, nil
	case "both":
		return Both, nil
	case "clear":
		return Clear, nil
	case "delete":
		return Delete, nil
	case "exit":
		return Exit, nil
	default:
		return -1, errors.New("not a valid command")
	}
}

func composeDir() (string, error) {
	// Prefer current working directory; fallback to executable dir
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(dir, "docker-compose.yml")); err == nil {
		return dir, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return dir, nil
	}
	dir = filepath.Dir(exe)
	if _, err := os.Stat(filepath.Join(dir, "docker-compose.yml")); err == nil {
		return dir, nil
	}
	return os.Getwd()
}

func runDockerComposeUp() error {
	dir, err := composeDir()
	if err != nil {
		return err
	}
	cmd := exec.Command("docker", "compose", "up", "-d")
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose up: %w", err)
	}
	return nil
}

func runDockerComposeDown() error {
	dir, err := composeDir()
	if err != nil {
		return err
	}
	cmd := exec.Command("docker", "compose", "down")
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose down: %w", err)
	}
	return nil
}

func initilizeDB(pool *pgxpool.Pool) error {

	query := `CREATE TABLE IF NOT EXISTS counter (
		id SERIAL PRIMARY KEY,
		name VARCHAR(255) NOT NULL DEFAULT 'empty',
		value INT NOT NULL DEFAULT 0
	)`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func insertRow(pool *pgxpool.Pool, name string, value int) (id int, err error) {
	query := `
		INSERT INTO counter (name, value)
		VALUES ($1, $2)
		RETURNING id;
	`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = pool.QueryRow(ctx, query, name, value).Scan(&id)
	return id, err
}

func runWriter(ctx context.Context, pool *pgxpool.Pool) {
	names := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			close(writeDone)
			return
		case <-ticker.C:
			name := names[rand.Intn(len(names))]
			value := rand.Intn(10000)
			id, err := insertRow(pool, name, value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR :::: insert failed: %v\n", err)
				continue
			}
			entry := WriteLogEntry{ID: id, Name: name, Value: value, Timestamp: time.Now()}
			writeLogChan <- entry
		}
	}
}

func publisherConnString() string {

	fmt.Print("Enter pub db ip : ")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	host := strings.TrimSpace(scanner.Text())

	return host
}

func ensurePublication(pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE counter", PubName))
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

func clearCounter(pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, "DELETE FROM counter")
	return err
}

func ensureSubscription(pool *pgxpool.Pool, publisherConn string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Escape single quotes in connection string for SQL
	query := fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION 'dbname=%s host=%s user=%s password=%s port=%s' PUBLICATION %s", SubName, DBName, publisherConn, DBUser, DBPassword, DBPort, PubName)
	fmt.Println("SUB TO : ", query)
	_, err := pool.Exec(ctx, query)
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

func runReadTable(pool *pgxpool.Pool) {
	done := make(chan struct{})
	lineChan := make(chan string, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGINT)
	defer signal.Reset(os.Interrupt, syscall.SIGINT)

	// Poller: query counter table every 500ms and print
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				queryCounterAndPrint(pool)
			}
		}
	}()

	// Stdin reader: short deadline so we check 'done' often and exit when done is closed
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			select {
			case <-done:
				return
			default:
			}
			os.Stdin.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			line, err := reader.ReadString('\n')
			if err != nil {
				if os.IsTimeout(err) {
					continue
				}
				return
			}
			select {
			case lineChan <- line:
			case <-done:
				return
			}
		}
	}()

	queryCounterAndPrint(pool) // show once immediately
	fmt.Println("INFO :::: reading counter table every 500ms — press 'q' Enter or Ctrl+C to exit")
	var exit bool
	for !exit {
		select {
		case line := <-lineChan:
			if strings.TrimSpace(strings.ToLower(line)) == "q" {
				exit = true
			}
		case <-sigCh:
			exit = true
		}
	}
	close(done)
	time.Sleep(200 * time.Millisecond)    // let goroutines see done and exit
	os.Stdin.SetReadDeadline(time.Time{}) // clear deadline so main prompt can read again
	fmt.Println("INFO :::: read stopped")
}

func queryCounterAndPrint(pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := pool.Query(ctx, "SELECT id, name, value FROM counter ORDER BY id")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR :::: query counter: %v\n", err)
		return
	}
	defer rows.Close()
	var id int
	var name string
	var value int
	var lines []string
	for rows.Next() {
		if err := rows.Scan(&id, &name, &value); err != nil {
			continue
		}
		lines = append(lines, fmt.Sprintf("  %d  |  %-12s  |  %d", id, name, value))
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR :::: rows: %v\n", err)
		return
	}
	// Clear screen and redraw (ANSI)
	fmt.Print("\033[H\033[2J")
	fmt.Printf("--- counter table @ %s ---\n", time.Now().Format("15:04:05"))
	fmt.Println("  id |  name         |  value")
	fmt.Println("-----+---------------+--------")
	if len(lines) == 0 {
		fmt.Println("  (no rows)")
	} else {
		for _, s := range lines {
			fmt.Println(s)
		}
	}
	fmt.Println()
	fmt.Println("press 'q' Enter or Ctrl+C to exit")
}

func main() {
	fmt.Println("App started")

	// Start DB with docker compose
	fmt.Println("INFO :::: starting database (docker compose up -d)...")
	if err := runDockerComposeUp(); err != nil {
		fmt.Println("ERROR :::: ", err)
		os.Exit(-1)
	}
	fmt.Println("INFO :::: waiting for database to be ready...")
	time.Sleep(3 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", DBUser, DBPassword, DBHost, DBPort, DBName)
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		fmt.Println("ERROR :::: failed to create database pool :::: ", err.Error())
		os.Exit(-1)
	}
	defer pool.Close()

	// Retry ping until DB is ready
	for i := 0; i < 15; i++ {
		if err := pool.Ping(ctx); err == nil {
			break
		}
		if i == 14 {
			fmt.Println("ERROR :::: failed to ping database :::: ", err.Error())
			os.Exit(-1)
		}
		time.Sleep(1 * time.Second)
	}

	if err := initilizeDB(pool); err != nil {
		fmt.Println("ERROR :::: failed to initialize database :::: ", err.Error())
		os.Exit(-1)
	}

	writeLogChan = make(chan WriteLogEntry, 100)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		scanner.Scan()
		command := strings.TrimSpace(scanner.Text())
		cmd, err := validateCommand(command)
		if err != nil {
			fmt.Println("ERROR :::: ", err)
			continue
		}

		switch cmd {
		case Write:
			if writeCancel != nil {
				fmt.Println("INFO :::: writer already running")
				continue
			}
			writeDone = make(chan struct{})
			var writeCtx context.Context
			writeCtx, writeCancel = context.WithCancel(context.Background())
			go runWriter(writeCtx, pool)
			fmt.Println("INFO :::: writer started (random inserts every 1s)")
		case Read:
			runReadTable(pool)
		case Stop:
			if writeCancel == nil {
				fmt.Println("INFO :::: writer not running")
				continue
			}
			writeCancel()
			<-writeDone
			writeCancel = nil
			fmt.Println("INFO :::: writer stopped")
		case Status:
			if writeCancel != nil {
				fmt.Println("STATUS :::: writer is running")
			} else {
				fmt.Println("STATUS :::: writer is stopped")
			}
		case Up:
			fmt.Println("INFO :::: starting database (docker compose up -d)...")
			if err := runDockerComposeUp(); err != nil {
				fmt.Println("ERROR :::: ", err)
			} else {
				fmt.Println("INFO :::: database started")
			}
		case Down:
			fmt.Println("INFO :::: stopping database (docker compose down)...")
			if err := runDockerComposeDown(); err != nil {
				fmt.Println("ERROR :::: ", err)
			} else {
				fmt.Println("INFO :::: database stopped")
			}
		case Pub:
			if err := ensurePublication(pool); err != nil {
				fmt.Println("ERROR :::: pub failed: ", err)
			} else {
				fmt.Printf("INFO :::: publication %q for table counter created (or already exists)\n", PubName)
			}
		case Sub:
			conn := publisherConnString()
			if conn == "" {
				fmt.Println("ERROR :::: set SUB_PUBLISHER_URL or SUB_DB_HOST (and optionally SUB_DB_PORT) to subscribe to another database")
				continue
			}
			if err := ensureSubscription(pool, conn); err != nil {
				fmt.Println("ERROR :::: sub failed: ", err)
			} else {
				fmt.Printf("INFO :::: subscription %q to publication %q created (or already exists)\n", SubName, PubName)
			}
		case Both:
			if err := ensurePublication(pool); err != nil {
				fmt.Println("ERROR :::: pub failed: ", err)
				continue
			}
			fmt.Printf("INFO :::: publication %q for table counter created (or already exists)\n", PubName)
			conn := publisherConnString()
			if conn == "" {
				fmt.Println("ERROR :::: set SUB_PUBLISHER_URL or SUB_DB_HOST for sub")
				continue
			}
			if err := ensureSubscription(pool, conn); err != nil {
				fmt.Println("ERROR :::: sub failed: ", err)
			} else {
				fmt.Printf("INFO :::: subscription %q to publication %q created (or already exists)\n", SubName, PubName)
			}
		case Clear:
			fmt.Print("\033[H\033[2J") // clear screen
			if err := clearCounter(pool); err != nil {
				fmt.Println("ERROR :::: clear failed: ", err)
			} else {
				fmt.Println("INFO :::: screen cleared and counter table emptied")
			}
		case Delete:
			if err := clearCounter(pool); err != nil {
				fmt.Println("ERROR :::: delete failed: ", err)
			} else {
				fmt.Println("INFO :::: counter table emptied")
			}
		case Exit:
			if writeCancel != nil {
				writeCancel()
				<-writeDone
			}
			fmt.Println("bye")
			return
		}
	}
}
