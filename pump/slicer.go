package pump

import (
	"encoding/binary"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tidb-binlog/pkg/slicer"
	binlog "github.com/pingcap/tipb/go-binlog"
)

// KafkaSlicer spit payload into multiple messages
type KafkaSlicer struct {
	topic     string
	partition int32
}

// NewKafkaSlicer returns a kafka slicer
func NewKafkaSlicer(topic string, partition int32) *KafkaSlicer {
	return &KafkaSlicer{
		topic:     topic,
		partition: partition,
	}
}

// Generate genrates binlog slices
// rules of binlog split
// unsplitted binlog doesn't have header - [disable binlog slice, length of payload is smaller than slice size limit]
// splitted binlog header
// * messageID: pos.Suffix_pos.Offset
// * total: total count of binlog slices
// * No: the number of slice in binlog slices
// * checksum: checksum code of binlog - only last slice have checksum code to save space
func (s *KafkaSlicer) Generate(entity *binlog.Entity) ([]*sarama.ProducerMessage, error) {
	if !GlobalConfig.EnableBinlogSlice || len(entity.Payload) < GlobalConfig.SlicesSize {
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
		total     = (len(entity.Payload) + GlobalConfig.SlicesSize - 1) / GlobalConfig.SlicesSize
		messages  = make([]*sarama.ProducerMessage, 0, total)
		left      = 0
		right     int
		totalByte = make([]byte, 4)
		messageID = []byte(BinlogSliceMessageID(entity.Pos))
	)

	binary.LittleEndian.PutUint32(totalByte, uint32(total))
	for i := 0; i < total-1; i++ {
		right = left + GlobalConfig.SlicesSize
		messages = append(messages, s.wrapProducerMessage(i, messageID, totalByte, entity.Payload[left:right], nil))
		left = right
	}

	messages = append(messages, s.wrapProducerMessage(total-1, messageID, totalByte, entity.Payload[left:], entity.Checksum))
	return messages, nil
}

func (s *KafkaSlicer) wrapProducerMessage(index int, messageID []byte, total []byte, payload []byte, checksum []byte) *sarama.ProducerMessage {
	no := make([]byte, 4)
	binary.LittleEndian.PutUint32(no, uint32(index))

	msg := &sarama.ProducerMessage{
		Topic:     s.topic,
		Partition: s.partition,
		Value:     sarama.ByteEncoder(payload),
		Headers: []sarama.RecordHeader{
			{
				Key:   slicer.MessageID,
				Value: messageID,
			}, {
				Key:   slicer.No,
				Value: no,
			}, {
				Key:   slicer.Total,
				Value: total,
			},
		},
	}

	if len(checksum) > 0 {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{
			Key:   slicer.Checksum,
			Value: checksum,
		})
	}

	return msg
}

// BinlogSliceMessageID return a message ID of pos
func BinlogSliceMessageID(pos binlog.Pos) string {
	return fmt.Sprintf("%d-%d", pos.Suffix, pos.Offset)
}
