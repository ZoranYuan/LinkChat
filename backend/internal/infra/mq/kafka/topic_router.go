package kafka

import (
	"errors"
	"sort"
	"strings"
)

var ErrTopicNotMapped = errors.New("事件未配置 Kafka Topic 映射")

type TopicRouter struct {
	eventToTopic     map[string]string
	topicToEvent     map[string]string
	deadLetterSuffix string
}

func NewTopicRouter(eventToTopic map[string]string, deadLetterSuffix string) (*TopicRouter, error) {
	if deadLetterSuffix == "" {
		return nil, errors.New("Kafka DLQ 后缀未配置")
	}
	router := &TopicRouter{
		eventToTopic:     make(map[string]string, len(eventToTopic)),
		topicToEvent:     make(map[string]string, len(eventToTopic)),
		deadLetterSuffix: deadLetterSuffix,
	}
	for eventName, topic := range eventToTopic {
		if eventName == "" || topic == "" {
			return nil, ErrTopicNotMapped
		}
		if _, exists := router.topicToEvent[topic]; exists {
			return nil, errors.New("Kafka Topic 映射重复")
		}
		router.eventToTopic[eventName] = topic
		router.topicToEvent[topic] = eventName
	}
	return router, nil
}

func (r *TopicRouter) TopicFor(eventName string) (string, error) {
	if r == nil {
		return "", ErrTopicNotMapped
	}

	isDLQ := strings.HasSuffix(eventName, r.deadLetterSuffix)
	baseEventName := strings.TrimSuffix(eventName, r.deadLetterSuffix)

	topic, ok := r.eventToTopic[baseEventName]
	if !ok {
		return "", ErrTopicNotMapped
	}

	if isDLQ {
		topic += r.deadLetterSuffix
	}

	return topic, nil
}

func (r *TopicRouter) EventFor(topic string) (string, error) {
	if r == nil {
		return "", ErrTopicNotMapped
	}
	eventName, ok := r.topicToEvent[topic]
	if !ok {
		return "", ErrTopicNotMapped
	}
	return eventName, nil
}

func (r *TopicRouter) Topics() []string {
	if r == nil {
		return nil
	}
	result := make([]string, 0, len(r.topicToEvent))
	for topic := range r.topicToEvent {
		result = append(result, topic)
	}
	sort.Strings(result)
	return result
}
