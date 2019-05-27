(ns numbers.radio
  (:require [numbers.image :as image]
            [numbers.serdes :as serdes])
  (:import (org.apache.kafka.clients.producer ProducerRecord KafkaProducer)
           (org.apache.kafka.common.serialization Serializer StringSerializer)
           (java.util Map)))

(defn listen
  "Nearly three hours of Numbers Station broadcast from 1557125670763 to 1557135278803"
  []
  (image/obsfuscate image/source))

(defn sample
  []
  (take 20 (listen)))

(defn stations
  []
  (map #(format "%03d" %1) (range (image/height image/source))))

(defn produce
  "Send the radio burst from radio/listen to the radio-logs topic on Kafka"
  []
  (let [producer-config ^Map {"bootstrap.servers" "localhost:9092"}]
    (with-open [producer (KafkaProducer. producer-config
                                         ^Serializer (StringSerializer.)
                                         ^Serializer (serdes/json-serializer))]
      (doseq [message (listen)]
        (.send producer (ProducerRecord. "radio-logs" (:name message) message))))))