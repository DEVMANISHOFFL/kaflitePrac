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

	r.POST("/publish", func(c *gin.Context) {
		var body struct {
			Topic   string `json:"topic" binding:"required"`
			Message string `json:"message" binding:"required"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		msg := b.Publish(body.Topic, body.Message)
		c.JSON(http.StatusOK, gin.H{"status": "ok", "message": msg})
	})

	r.GET("/consume/:group/:topic", func(c *gin.Context) {
		groupName := c.Param("group")
		topicName := c.Param("topic")

		group := b.GetOrCreateGroup(groupName)
		msgs, err := group.Consume(b, topicName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"group":    groupName,
			"topic":    topicName,
			"messages": msgs,
		})
	})

	r.GET("/topics", func(c *gin.Context) {
		topics := b.ListTopics()

		c.JSON(http.StatusOK, gin.H{"topics": topics})
	})

	r.Run(":8080")
}
