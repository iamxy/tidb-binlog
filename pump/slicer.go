package pump

import (
	"encoding/binary"
	"fmt"

	"github.com/Shopify/sarama"
	binlog "github.com/pingcap/tipb/go-binlog"
)

var (
	// MessageID is ID to indicate which binlog it belongs
	MessageID = []byte("messageID")
	// No is index of slices of binlog
	No = []byte("No")
	// Total is total number of binlog slices
	Total = []byte("total")
	// Checksum is checksum code of binlog payload
	// to save space, it's only in last binlog slice
	Checksum = []byte("checksum")
)

// kafkaSlicer spit payload into multiple messages
type kafkaSlicer struct {
	topic     string
	partition int32
}

func newKafkaSlicer(topic string, partition int32) *kafkaSlicer {
	return &kafkaSlicer{
		topic:     topic,
		partition: partition,
	}
}

func (s *kafkaSlicer) Generate(entity *binlog.Entity) ([]*sarama.ProducerMessage, error) {
	if !GlobalConfig.enableBinlogSlice || len(entity.Payload) < GlobalConfig.slicesSize {
		// no header, no slices
		return []*sarama.ProducerMessage{
			{
				Topic:     s.topic,
				Partition: s.partition,
				Value:     sarama.ByteEncoder(entity.Payload),
			},
		}, nil
	}

	var (
		// hard code, lenght of checksum code is 4
		total     = (len(entity.Payload) + GlobalConfig.slicesSize - 1) / GlobalConfig.slicesSize
		messages  = make([]*sarama.ProducerMessage, 0, total)
		left      = 0
		right     = 0
		totalByte = make([]byte, 4)
		messageID = []byte(fmt.Sprintf("%d-%d", entity.Pos.Suffix, entity.Pos.Offset))
	)

	binary.LittleEndian.PutUint32(totalByte, uint32(total))
	for i := 1; i < total; i++ {
		no := make([]byte, 4)
		binary.LittleEndian.PutUint32(no, uint32(i))
		right = left + GlobalConfig.slicesSize
		messages = append(messages, s.wrapProducerMessage(i, messageID, totalByte, entity.Payload[left:right], nil))
		left = right
	}

	messages = append(messages, s.wrapProducerMessage(total, messageID, totalByte, entity.Payload[left:], entity.Checksum))
	return messages, nil
}

func (s *kafkaSlicer) wrapProducerMessage(index int, messageID []byte, total []byte, payload []byte, checksum []byte) *sarama.ProducerMessage {
	no := make([]byte, 4)
	binary.LittleEndian.PutUint32(no, uint32(index))

	msg := &sarama.ProducerMessage{
		Topic:     s.topic,
		Partition: s.partition,
		Value:     sarama.ByteEncoder(payload),
		Headers: []sarama.RecordHeader{
			{
				Key:   MessageID,
				Value: messageID,
			}, {
				Key:   No,
				Value: no,
			}, {
				Key:   Total,
				Value: total,
			},
		},
	}

	if len(checksum) > 0 {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{
			Key:   Checksum,
			Value: checksum,
		})
	}

	return msg
}
