package main

import (
	"database/sql"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	"github.com/blueai2022/vtclone/api"
	"github.com/blueai2022/vtclone/config"
	db "github.com/blueai2022/vtclone/db/sqlc"
	"github.com/blueai2022/vtclone/msg"
	_ "github.com/lib/pq"
)

const (
	devEnvironment   = "development"
	analyzeFileTopic = "file-analysis"
)

func main() {
	config, err := config.Load(".")
	if err != nil {
		log.Fatal().Err(err).Msg("cannot load config")
	}

	//development: pretty print
	if strings.ToLower(config.Environment) == devEnvironment {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	conn, err := sql.Open(config.DBDriver, config.DBSource)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot connect to db")
	}
	store := db.NewStore(conn)

	msgTransport := &kafka.Transport{
		// TLS: &tls.Config{},
	}
	msgPub := msg.NewPublisher(&config, msgTransport, analyzeFileTopic)
	defer msgPub.Close()

	runGinServer(&config, store, msgPub)
}

func runGinServer(config *config.Config, store db.Store, msgPub *msg.Publisher) {
	server, err := api.NewServer(config, store, msgPub)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot create server")
	}

	err = server.Start(config.HTTPServerAddress)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot start server")
	}
}
