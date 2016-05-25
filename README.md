# fluent-plugin-gcloud-pubsub
[![Build Status](https://travis-ci.org/mdoi/fluent-plugin-gcloud-pubsub.svg?branch=master)](https://travis-ci.org/mdoi/fluent-plugin-gcloud-pubsub)
[![Gem Version](https://badge.fury.io/rb/fluent-plugin-gcloud-pubsub.svg)](http://badge.fury.io/rb/fluent-plugin-gcloud-pubsub)

## Overview
[Cloud Pub/Sub](https://cloud.google.com/pubsub/) Input/Output plugin for [Fluentd](http://www.fluentd.org/) with [gcloud](https://googlecloudplatform.github.io/gcloud-ruby/) gem

- Publish BufferedOutput chunk into Cloud Pub/Sub with [batch publishing](http://googlecloudplatform.github.io/gcloud-ruby/docs/v0.2.0/Gcloud/Pubsub/Topic.html#method-i-publish)
- [Pull](http://googlecloudplatform.github.io/gcloud-ruby/docs/v0.2.0/Gcloud/Pubsub/Subscription.html#method-i-pull) messages from Cloud Pub/Sub

## Preparation
- Create a project on Google Developer Console
- Add a topic of Cloud Pub/Sub to the project
- Add a pull style subscription to the topic
- Download your credential (json) 

## Publish messages

Use `out_gcloud_pubsub`.

### Configuration
publish dummy json data like `{"message": "dummy", "value": 0}\n{"message": "dummy", "value": 1}\n ...`.

```
<source>
  type dummy
  tag example.publish
  rate 100 
  auto_increment_key value
</source>

<match example.publish>
  type gcloud_pubsub
  project <YOUR PROJECT>
  topic <YOUR TOPIC>
  key <YOUR KEY>
  flush_interval 10
  autocreate_topic false
</match>
```

- `autocreate_topic` (optional, default: `false`)
  - If set to `true`, specified topic will be created when it doesn't exist.

## Pull messages
Use `in_gcloud_pubsub`.

### Configuration
Pull json data from Cloud Pub/Sub

```
<source>
  type gcloud_pubsub
  tag example.pull
  project <YOUR PROJECT>
  topic <YOUR TOPIC>
  subscription <YOUR SUBSCRIPTION>
  key <YOUR KEY>
  max_messages 1000
  return_immediately true
  pull_interval 2
  format json
</source>

<match example.pull>
  type stdout
</match>
```

- `max_messages`
 - see maxMessages on https://cloud.google.com/pubsub/subscriber

- `return_immediately`
 - see returnImmediately on https://cloud.google.com/pubsub/subscriber
 - When `return_immediately` is true, this plugin ignore pull_interval

