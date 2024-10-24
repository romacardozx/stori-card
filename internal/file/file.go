package file

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/romacardozx/stori-card/internal/transaction"
)

func ReadTransactionsFromCSV(filePath string) ([]transaction.Transaction, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	if _, err := reader.Read(); err != nil {
		return nil, fmt.Errorf("failed to skip CSV header: %v", err)
	}

	var transactions []transaction.Transaction

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV record: %v", err)
		}

		dateStr := record[1]
		dateStr = fmt.Sprintf("2024/%s", dateStr)
		date, err := time.Parse("2006/01/02", dateStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse date: %v", err)
		}

		amountStr := strings.TrimSpace(record[2])
		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse amount: %v", err)
		}

		transaction := transaction.Transaction{
			Date:   date,
			Amount: amount,
		}
		transactions = append(transactions, transaction)
	}

	return transactions, nil
}
