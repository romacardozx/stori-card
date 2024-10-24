package email

import (
	"bytes"
	"embed"
	"encoding/base64"
	"fmt"
	"html/template"
	"net/smtp"
	"strings"

	"github.com/romacardozx/stori-card/internal/config"
	"github.com/romacardozx/stori-card/internal/transaction"
)

//go:embed template/template.html template/styles.css template/logo.png
var files embed.FS

func SendSummaryEmail(cfg config.SMTPConfig, summary transaction.Summary, To string) error {
	templateHTML, err := files.ReadFile("template/template.html")
	if err != nil {
		return fmt.Errorf("failed to read HTML template: %v", err)
	}

	cssContent, err := files.ReadFile("template/styles.css")
	if err != nil {
		return fmt.Errorf("failed to read CSS file: %v", err)
	}

	htmlContent := injectCSSIntoTemplate(templateHTML, cssContent)

	body, err := generateEmailBody(string(htmlContent), summary)
	if err != nil {
		return err
	}

	auth := smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host)

	logoBytes, err := files.ReadFile("template/logo.png")
	if err != nil {
		return fmt.Errorf("failed to read logo file: %v", err)
	}

	msg := buildEmailMessage(cfg, body, logoBytes)

	err = smtp.SendMail(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), auth, cfg.From, []string{To}, msg)
	if err != nil {
		return fmt.Errorf("failed to send email: %v", err)
	}

	return nil
}

func injectCSSIntoTemplate(templateHTML, cssContent []byte) []byte {
	return []byte(strings.Replace(string(templateHTML), "{{css_link}}", "<style>"+string(cssContent)+"</style>", 1))
}

func generateEmailBody(htmlContent string, summary transaction.Summary) (string, error) {
	tmpl, err := template.New("email").Parse(htmlContent)
	if err != nil {
		return "", err
	}

	var body bytes.Buffer
	err = tmpl.Execute(&body, summary)
	if err != nil {
		return "", err
	}

	return body.String(), nil
}

func buildEmailMessage(cfg config.SMTPConfig, body string, logoBytes []byte) []byte {
	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: Resumen de Transacciones\r\nContent-Type: multipart/related; boundary=boundary\r\n\r\n--%s\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n%s\r\n\r\n--%s\r\nContent-Type: image/png; name=\"logo.png\"\r\nContent-Transfer-Encoding: base64\r\nContent-Disposition: inline; filename=\"logo.png\"\r\nContent-ID: <logo>\r\n\r\n%s\r\n\r\n--%s--\r\n", cfg.From, cfg.To, "boundary", body, "boundary", base64.StdEncoding.EncodeToString(logoBytes), "boundary")
	return []byte(msg)
}
