package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"golang.org/x/net/html"
)

func isSkippableTag(n *html.Node) bool {
	if n.Type != html.ElementNode {
		return false
	}
	switch n.Data {
	case "script", "style", "noscript":
		return true
	}
	return false
}

func extractText(n *html.Node, sb *strings.Builder) {
	if isSkippableTag(n) {
		return
	}

	if n.Type == html.TextNode {
		text := strings.TrimSpace(n.Data)
		if text != "" {
			sb.WriteString(text)
			sb.WriteString(" ")
		}
	}

	for c := n.FirstChild; c != nil; c = c.NextSibling {
		extractText(c, sb)
	}
}

func htmlToText(htmlStr string) string {
	doc, err := html.Parse(strings.NewReader(htmlStr))
	if err != nil {
		return ""
	}

	var sb strings.Builder
	extractText(doc, &sb)

	result := strings.Join(strings.Fields(sb.String()), " ")
	return result
}

func main() {
	ctx := context.Background()

	client, err := mongo.Connect(ctx,
		options.Client().ApplyURI("mongodb://172.29.32.1:27017"),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := client.Database("local")
	rawCol := db.Collection("IS_lab3")
	cleanCol := db.Collection("IS_lab3_clean")

	_, _ = cleanCol.DeleteMany(ctx, bson.M{})

	cursor, err := rawCol.Find(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)

	count := 0

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		rawHTML, _ := doc["html"].(string)

		url, _ := doc["url"].(string)

		cleanText := htmlToText(rawHTML)
		if cleanText == "" {
			continue
		}

		newDoc := bson.M{
			"url":          url,
			"clean_text":   cleanText,
			"processed_at": time.Now().Unix(),
		}

		_, err := cleanCol.InsertOne(ctx, newDoc)
		if err != nil {
			fmt.Println(err)
			continue
		}

		count++
		if count%10 == 0 {
			fmt.Println("Обработано:", count)
		}
	}

	fmt.Println("Всего обработано:", count)
}
