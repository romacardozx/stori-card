package database

import (
	"database/sql"
	"fmt"

	"github.com/romacardozx/stori-card/internal/config"
	"github.com/romacardozx/stori-card/internal/transaction"

	_ "github.com/lib/pq"
)

func NewDatabase(cfg config.PostgresConfig) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	err = createTables(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return db, nil
}

func createTables(db *sql.DB) error {
	// Primero, creamos las tablas si no existen
	_, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            amount DECIMAL(10, 2) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS summary (
            id SERIAL PRIMARY KEY,
            total_balance DECIMAL(10, 2) NOT NULL,
            total_transactions INTEGER NOT NULL,
            avg_debit DECIMAL(10, 2) NOT NULL,
            avg_credit DECIMAL(10, 2) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    `)
	if err != nil {
		return fmt.Errorf("failed to create tables: %v", err)
	}

	// Verificar si la columna total_transactions existe, si no, la agregamos
	var columnExists bool
	err = db.QueryRow(`
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_name = 'summary' 
            AND column_name = 'total_transactions'
        );
    `).Scan(&columnExists)
	if err != nil {
		return fmt.Errorf("failed to check column existence: %v", err)
	}

	if !columnExists {
		_, err = db.Exec(`
            ALTER TABLE summary 
            ADD COLUMN total_transactions INTEGER NOT NULL DEFAULT 0;
        `)
		if err != nil {
			return fmt.Errorf("failed to add total_transactions column: %v", err)
		}
	}

	return nil
}

func SaveTransactionsAndSummary(db *sql.DB, transactions []transaction.Transaction, summary transaction.Summary) error {
	// Iniciar transacción
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer tx.Rollback()

	// Primero limpiamos las transacciones existentes
	_, err = tx.Exec(`TRUNCATE TABLE transactions RESTART IDENTITY`)
	if err != nil {
		return fmt.Errorf("failed to clean transactions: %v", err)
	}

	// Insertar las nuevas transacciones
	for _, t := range transactions {
		_, err := tx.Exec(`
            INSERT INTO transactions (date, amount)
            VALUES ($1, $2)
        `, t.Date, t.Amount)
		if err != nil {
			return fmt.Errorf("failed to insert transaction: %v", err)
		}
	}

	// Insertar el nuevo resumen
	_, err = tx.Exec(`
        INSERT INTO summary (
            total_balance,
            total_transactions,
            avg_debit,
            avg_credit
        ) VALUES ($1, $2, $3, $4)
    `, summary.TotalBalance, summary.TotalTransactions, summary.AvgDebit, summary.AvgCredit)
	if err != nil {
		return fmt.Errorf("failed to insert summary: %v", err)
	}

	// Confirmar la transacción
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// GetLatestSummary obtiene el resumen más reciente
func GetLatestSummary(db *sql.DB) (*transaction.Summary, error) {
	var summary transaction.Summary
	err := db.QueryRow(`
        SELECT total_balance, total_transactions, avg_debit, avg_credit
        FROM summary
        ORDER BY created_at DESC
        LIMIT 1
    `).Scan(&summary.TotalBalance, &summary.TotalTransactions, &summary.AvgDebit, &summary.AvgCredit)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get latest summary: %v", err)
	}
	return &summary, nil
}
