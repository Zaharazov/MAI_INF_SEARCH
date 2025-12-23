package main

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v3"
)

type Config struct {
	DB struct {
		URI        string `yaml:"uri"`
		Database   string `yaml:"database"`
		Collection string `yaml:"collection"`
	} `yaml:"db"`

	Logic struct {
		Sitemap      string `yaml:"sitemap"`
		Delay        int    `yaml:"delay"`         
		RecrawlHours int    `yaml:"recrawl_hours"` 
	} `yaml:"logic"`
}

type Page struct {
	URL          string `bson:"url"`
	HTML         string `bson:"html"`
	Source       string `bson:"source"`
	Timestamp    int64  `bson:"timestamp"`     
	LastModified string `bson:"last_modified"` 
	ETag         string `bson:"etag"`          
}

func getLinks(target string) []string {
	resp, err := http.Get(target)
	if err != nil {
		fmt.Println("err:", err)
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var us struct {
		URLs []struct {
			Loc string `xml:"loc"`
		} `xml:"url"`
	}
	xml.Unmarshal(body, &us)

	var idx struct {
		Sitemaps []struct {
			Loc string `xml:"loc"`
		} `xml:"sitemap"`
	}
	xml.Unmarshal(body, &idx)

	var res []string
	if len(idx.Sitemaps) > 0 {
		for _, s := range idx.Sitemaps {
			fmt.Println("-> ", s.Loc)
			res = append(res, getLinks(s.Loc)...)
		}
	}

	for _, u := range us.URLs {
		res = append(res, u.Loc)
	}
	return res
}

func main() {
	if len(os.Args) < 2 {
		return
	}

	f, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Println(err)
    	os.Exit(1)
	}
	var cfg Config
	yaml.Unmarshal(f, &cfg)

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.DB.URI))
	if err != nil {
		fmt.Println(err)
    	os.Exit(1)
	}
	col := client.Database(cfg.DB.Database).Collection(cfg.DB.Collection)

	links := getLinks(cfg.Logic.Sitemap)
	fmt.Println("Всего:", len(links))

	for i, link := range links {

		u, _ := url.Parse(link)
		u.Fragment = ""
		normURL := u.String()

		fmt.Printf("[%d/%d] %s\n", i+1, len(links), normURL)

		var old Page
		err = col.FindOne(ctx, bson.M{"url": normURL}).Decode(&old)
		exists := (err == nil)

		if exists && time.Since(time.Unix(old.Timestamp, 0)).Hours() < float64(cfg.Logic.RecrawlHours) {
			fmt.Println("skip")
			continue
		}


		req, _ := http.NewRequest("GET", normURL, nil)
		req.Header.Set("User-Agent", "SimpleCrawler/1.0")
		if exists {
			if old.LastModified != "" {
				req.Header.Set("If-Modified-Since", old.LastModified)
			}
			if old.ETag != "" {
				req.Header.Set("If-None-Match", old.ETag)
			}
		}

		hc := &http.Client{Timeout: 15 * time.Second}
		resp, err := hc.Do(req)
		if err != nil {
			continue
		}

		if resp.StatusCode == 304 {
			col.UpdateOne(ctx, bson.M{"url": normURL}, bson.M{"$set": bson.M{"timestamp": time.Now().Unix()}})
			resp.Body.Close()
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		doc := Page{
			URL:          normURL,
			HTML:         string(body),
			Source:       cfg.Logic.Sitemap,
			Timestamp:    time.Now().Unix(),
			LastModified: resp.Header.Get("Last-Modified"),
			ETag:         resp.Header.Get("ETag"),
		}

		if exists {
			col.ReplaceOne(ctx, bson.M{"url": normURL}, doc)
		} else {
			col.InsertOne(ctx, doc)
		}

		fmt.Println("saved")

		time.Sleep(time.Duration(cfg.Logic.Delay) * time.Millisecond)
	}
}
