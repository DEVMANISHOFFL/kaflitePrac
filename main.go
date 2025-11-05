package main

import (
	"net/http"

	"github.com/devmanishoffl/kaflite/broker"
	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()
	b := broker.NewBroker()

	r.POST("/topics", func(c *gin.Context) {
		var body struct {
			Name string `json:"name"`
		}

		if err := c.BindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}

		b.GetOrCreateTopic(body.Name)
		c.JSON(http.StatusOK, gin.H{"status": "topic created", "topicName": body.Name})
	})

	r.POST("/publish/:topic", func(c *gin.Context) {
		topicName := c.Param("topic")
		var body struct {
			Message string `json:"message"`
		}

		if err := c.BindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}

		msg := b.Publish(topicName, body.Message)
		c.JSON(http.StatusOK, gin.H{"status": "message published", "message": msg})
	})

	r.GET("/consume/:topic", func(c *gin.Context) {
		topicName := c.Param("topic")
		m := b.Consume(topicName)
		c.JSON(http.StatusOK, gin.H{"message": m})
	})

	r.GET("/topics", func(c *gin.Context) {
		topics := b.ListTopics()
		c.JSON(http.StatusOK, gin.H{"topics": topics})
	})
	
	r.Run(":8080")
}
