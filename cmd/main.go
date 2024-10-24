package main

import (
	"log"
	"net/http"

	"github.com/romacardozx/stori-card/handler"
	"github.com/romacardozx/stori-card/internal/config"
	"github.com/romacardozx/stori-card/internal/database"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	db, err := database.NewDatabase(cfg.PostgresConfig)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	http.HandleFunc("/process-email", handler.ProcessEmail(cfg, db))

	log.Printf("Server started on port %s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, nil))
}
