package handler

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/romacardozx/stori-card/internal/config"
	"github.com/romacardozx/stori-card/internal/database"
	"github.com/romacardozx/stori-card/internal/email"
	"github.com/romacardozx/stori-card/internal/file"
	"github.com/romacardozx/stori-card/internal/transaction"
)

type EmailRequest struct {
	To string `json:"To"`
}

func ProcessEmail(cfg *config.Config, db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			fmt.Fprint(w, "Method not allowed")
			return
		}

		var req EmailRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			log.Printf("Failed to decode request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Invalid request body")
			return
		}

		transactions, err := file.ReadTransactionsFromCSV(cfg.CSVFilePath)
		if err != nil {
			log.Printf("Failed to read transactions from CSV: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "Failed to read transactions from CSV")
			return
		}

		summary, err := transaction.ProcessTransactions(transactions)
		if err != nil {
			log.Printf("Failed to process transactions: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "Failed to process transactions")
			return
		}

		err = database.SaveTransactionsAndSummary(db, transactions, summary)
		if err != nil {
			log.Printf("Failed to save transactions and summary to the database: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "Failed to save transactions and summary to the database")
			return
		}

		err = email.SendSummaryEmail(cfg.SMTPConfig, summary, req.To)
		if err != nil {
			log.Printf("Failed to send email: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "Failed to send email")
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "Process completed successfully")
	}
}
