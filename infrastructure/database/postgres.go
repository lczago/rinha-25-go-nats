package database

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"time"
)

func NewPostgres() (*sql.DB, error) {
	var (
		user     = os.Getenv("DATABASE_USER")
		password = os.Getenv("DATABASE_PASSWORD")
		dbname   = os.Getenv("DATABASE_NAME")
		host     = os.Getenv("DATABASE_HOSTNAME")
		port     = os.Getenv("DATABASE_PORT")
	)

	if user == "" || password == "" || dbname == "" || host == "" || port == "" {
		return nil, errors.New("all database environment variables must be set")
	}

	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxOpenConns(10)
	db.SetConnMaxIdleTime(time.Second * 15)

	if err = db.Ping(); err != nil {
		return nil, err
	}
	log.Println("Connected to PostgreSQL!")

	return db, nil

}
