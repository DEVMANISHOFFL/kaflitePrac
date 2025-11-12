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
			return
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

	r.POST("/groups/:group/consumers", func(c *gin.Context) {
		groupName := c.Param("group")

		var body struct {
			Consumer string `json:"consumer"`
			Topic    string `json:"topic"`
		}

		if err := c.BindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		group := b.GetOrCreateGroup(groupName)
		topic := b.GetOrCreateTopic(body.Topic)
		group.AddConsumer(body.Consumer, topic)
		c.JSON(http.StatusOK, gin.H{
			"status": "consumer added",
			"group":  groupName,
			"topic":  body.Topic,
		})
	})

	r.GET("/consume/:group/:consumer/:topic", func(c *gin.Context) {
		groupName := c.Param("group")
		consumerName := c.Param("consumer")
		topicName := c.Param("topic")

		group := b.GetOrCreateGroup(groupName)
		msgs, err := group.ConsumeByConsumer(b, consumerName, topicName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"group":    groupName,
			"consumer": consumerName,
			"topic":    topicName,
			"count":    len(msgs),
			"messages": msgs,
		})
	})

	r.GET("/consume/:group/all/:topic", func(c *gin.Context) {
		groupName := c.Param("group")
		topicName := c.Param("topic")

		group := b.GetOrCreateGroup(groupName)
		msgs, err := group.ConsumeAll(b, topicName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"group":    groupName,
			"topic":    topicName,
			"count":    len(msgs),
			"messages": msgs,
		})
	})

	r.GET("/topics", func(c *gin.Context) {
		topics := b.ListTopics()
		c.JSON(http.StatusOK, gin.H{"topics": topics})
	})

	r.Run(":8080")
}
