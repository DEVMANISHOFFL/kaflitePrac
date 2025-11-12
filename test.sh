#!/bin/bash
set -e
echo "== Create topic =="; curl -s -X POST localhost:8080/topics -H "Content-Type: application/json" -d '{"name":"orders"}'; echo
echo "== Publish messages =="; for i in {1..3}; do curl -s -X POST localhost:8080/publish -H "Content-Type: application/json" -d "{\"topic\":\"orders\",\"message\":\"Order #100$i\"}"; echo; done
echo "== Add consumer =="; curl -s -X POST localhost:8080/groups/zomato/consumers -H "Content-Type: application/json" -d '{"consumer":"delivery-service","topic":"orders"}'; echo
echo "== Consume by consumer =="; curl -s localhost:8080/consume/zomato/delivery-service/orders; echo
echo "== Consume all =="; curl -s localhost:8080/consume/zomato/all/orders; echo
