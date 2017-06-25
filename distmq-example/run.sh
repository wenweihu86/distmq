#!/usr/bin/env bash

# run producer
java -cp dependency/*:distmq-example-1.0.0-SNAPSHOT.jar com.github.wenweihu86.distmq.example.ProducerMain

# consumer command, should be run at another terminal
java -cp dependency/*:distmq-example-1.0.0-SNAPSHOT.jar com.github.wenweihu86.distmq.example.ConsumerMain
